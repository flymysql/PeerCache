#include "peercache/rdma_endpoint.h"

#include <chrono>
#include <cstdio>
#include <cstring>
#include <random>
#include <stdexcept>

namespace peercache {

namespace {
constexpr int kMaxSendWr = 1024;
constexpr int kMaxRecvWr = 16;
constexpr int kMaxSge = 1;
constexpr int kCqDepth = kMaxSendWr + kMaxRecvWr;
constexpr int kPollBatch = 16;  // CQEs reaped per ibv_poll_cq call
}  // namespace

RdmaEndpoint::RdmaEndpoint(RdmaContext* ctx) : ctx_(ctx) {}

RdmaEndpoint::~RdmaEndpoint() {
  if (qp_) ibv_destroy_qp(qp_);
  if (cq_) ibv_destroy_cq(cq_);
}

QpInfo RdmaEndpoint::create() {
  cq_ = ibv_create_cq(ctx_->context(), kCqDepth, nullptr, nullptr, 0);
  if (!cq_) throw std::runtime_error("peercache: ibv_create_cq failed");

  ibv_qp_init_attr attr;
  std::memset(&attr, 0, sizeof(attr));
  attr.send_cq = cq_;
  attr.recv_cq = cq_;
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

  static thread_local std::mt19937 rng(std::random_device{}());
  psn_ = rng() & 0xffffff;

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
  // Use the port's negotiated active MTU (e.g. 4096 on RoCE) rather than a
  // hardcoded 1 KiB; large KV pages move far fewer packets at MTU 4096. Both
  // RC peers query their own port, which match on a symmetric fabric. Guard
  // against a driver reporting 0.
  attr.path_mtu = ctx_->port_attr().active_mtu ? ctx_->port_attr().active_mtu
                                               : IBV_MTU_1024;
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

bool RdmaEndpoint::drain(size_t count, std::vector<bool>& ok, double timeout_s) {
  size_t seen = 0;
  ibv_wc wc[kPollBatch];
  const auto deadline =
      std::chrono::steady_clock::now() +
      std::chrono::duration_cast<std::chrono::steady_clock::duration>(
          std::chrono::duration<double>(timeout_s));
  while (seen < count) {
    // Reap several completions per call to cut per-CQE polling overhead under
    // high concurrency (one syscall-free batch instead of one call per CQE).
    int n = ibv_poll_cq(cq_, kPollBatch, wc);
    if (n < 0) return false;
    if (n == 0) {
      // No completion yet: bail out if we have waited past the deadline so a
      // silently dropped READ cannot wedge this thread (and thus the whole
      // benchmark) forever.
      if (std::chrono::steady_clock::now() >= deadline) return false;
      continue;
    }
    for (int i = 0; i < n; ++i) {
      ++seen;
      if (wc[i].status == IBV_WC_SUCCESS) {
        if (wc[i].wr_id < ok.size()) ok[static_cast<size_t>(wc[i].wr_id)] = true;
      } else {
        // A completion arrived but the READ failed (e.g. remote access error
        // from a bad rkey/MR, or retry-exceeded from a GID/path issue). Surface
        // the status so it is diagnosable; rate-limit so a storm of failures
        // cannot flood the log.
        last_wc_status_ = wc[i].status;
        ++wc_errors_;
        if (wc_errors_ <= 8 || wc_errors_ % 256 == 0) {
          std::fprintf(stderr,
                       "peercache: RDMA READ completion failed: %s (status=%d, "
                       "wr_id=%llu); total wc errors=%llu\n",
                       ibv_wc_status_str(wc[i].status), wc[i].status,
                       static_cast<unsigned long long>(wc[i].wr_id),
                       static_cast<unsigned long long>(wc_errors_));
        }
      }
    }
  }
  return true;
}

}  // namespace peercache
