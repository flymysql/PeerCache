#include "peercache/rdma_endpoint.h"

#include <cstdlib>
#include <cstring>

namespace peercache {

namespace {
constexpr int kMaxSendWr = 1024;
constexpr int kMaxRecvWr = 16;
constexpr int kMaxSge = 1;
}  // namespace

RdmaEndpoint::RdmaEndpoint(RdmaContext* ctx) : ctx_(ctx) {}

RdmaEndpoint::~RdmaEndpoint() {
  if (qp_) ibv_destroy_qp(qp_);
}

QpInfo RdmaEndpoint::create() {
  ibv_qp_init_attr attr;
  std::memset(&attr, 0, sizeof(attr));
  attr.send_cq = ctx_->cq();
  attr.recv_cq = ctx_->cq();
  attr.qp_type = IBV_QPT_RC;
  attr.cap.max_send_wr = kMaxSendWr;
  attr.cap.max_recv_wr = kMaxRecvWr;
  attr.cap.max_send_sge = kMaxSge;
  attr.cap.max_recv_sge = kMaxSge;

  qp_ = ibv_create_qp(ctx_->pd(), &attr);
  if (!qp_) throw std::runtime_error("peercache: ibv_create_qp failed");

  // INIT
  ibv_qp_attr qattr;
  std::memset(&qattr, 0, sizeof(qattr));
  qattr.qp_state = IBV_QPS_INIT;
  qattr.pkey_index = 0;
  qattr.port_num = ctx_->ib_port();
  qattr.qp_access_flags = IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE |
                          IBV_ACCESS_LOCAL_WRITE;
  int flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT |
              IBV_QP_ACCESS_FLAGS;
  if (ibv_modify_qp(qp_, &qattr, flags) != 0) {
    throw std::runtime_error("peercache: modify_qp INIT failed");
  }

  psn_ = lrand48() & 0xffffff;

  QpInfo info;
  info.qp_num = qp_->qp_num;
  info.psn = psn_;
  info.lid = ctx_->port_attr().lid;
  union ibv_gid g = ctx_->gid();
  std::memcpy(info.gid, g.raw, 16);
  return info;
}

bool RdmaEndpoint::connect(const QpInfo& remote) {
  // RTR
  ibv_qp_attr attr;
  std::memset(&attr, 0, sizeof(attr));
  attr.qp_state = IBV_QPS_RTR;
  attr.path_mtu = IBV_MTU_1024;
  attr.dest_qp_num = remote.qp_num;
  attr.rq_psn = remote.psn;
  attr.max_dest_rd_atomic = 16;
  attr.min_rnr_timer = 12;

  attr.ah_attr.is_global = 0;
  attr.ah_attr.dlid = remote.lid;
  attr.ah_attr.sl = 0;
  attr.ah_attr.src_path_bits = 0;
  attr.ah_attr.port_num = ctx_->ib_port();

  // Use a global route (GRH) when a GID index is configured (required for RoCE).
  if (ctx_->gid_index() >= 0) {
    attr.ah_attr.is_global = 1;
    std::memcpy(attr.ah_attr.grh.dgid.raw, remote.gid, 16);
    attr.ah_attr.grh.sgid_index = static_cast<uint8_t>(ctx_->gid_index());
    attr.ah_attr.grh.hop_limit = 1;
    attr.ah_attr.grh.traffic_class = 0;
  }

  int flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
              IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
  if (ibv_modify_qp(qp_, &attr, flags) != 0) {
    return false;
  }

  // RTS
  std::memset(&attr, 0, sizeof(attr));
  attr.qp_state = IBV_QPS_RTS;
  attr.timeout = 14;
  attr.retry_cnt = 7;
  attr.rnr_retry = 7;
  attr.sq_psn = psn_;
  attr.max_rd_atomic = 16;
  flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY |
          IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;
  if (ibv_modify_qp(qp_, &attr, flags) != 0) {
    return false;
  }

  connected_ = true;
  return true;
}

bool RdmaEndpoint::post_read(uint64_t local_addr, uint32_t lkey,
                             uint64_t remote_addr, uint32_t rkey,
                             uint64_t length, uint64_t wr_id) {
  ibv_sge sge;
  std::memset(&sge, 0, sizeof(sge));
  sge.addr = local_addr;
  sge.length = static_cast<uint32_t>(length);
  sge.lkey = lkey;

  ibv_send_wr wr;
  std::memset(&wr, 0, sizeof(wr));
  wr.wr_id = wr_id;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.opcode = IBV_WR_RDMA_READ;
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.wr.rdma.remote_addr = remote_addr;
  wr.wr.rdma.rkey = rkey;

  ibv_send_wr* bad = nullptr;
  return ibv_post_send(qp_, &wr, &bad) == 0;
}

int RdmaEndpoint::poll(int count) {
  int done = 0;
  ibv_wc wc;
  while (done < count) {
    int n = ibv_poll_cq(ctx_->cq(), 1, &wc);
    if (n < 0) return -1;
    if (n == 0) continue;
    if (wc.status != IBV_WC_SUCCESS) return -1;
    ++done;
  }
  return done;
}

}  // namespace peercache
