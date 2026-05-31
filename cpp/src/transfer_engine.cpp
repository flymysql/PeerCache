#include "peercache/transfer_engine.h"

#include <cstdlib>
#include <unordered_map>

namespace peercache {

namespace {
// Per-batch completion timeout. Defaults to 5s; override with
// PEERCACHE_RDMA_OP_TIMEOUT_MS so misconfigured fabrics fail fast and visibly
// instead of hanging the caller indefinitely.
double op_timeout_s() {
  static const double v = [] {
    const char* e = std::getenv("PEERCACHE_RDMA_OP_TIMEOUT_MS");
    if (e && *e) {
      char* end = nullptr;
      double ms = std::strtod(e, &end);
      if (end != e && ms > 0) return ms / 1000.0;
    }
    return 5.0;
  }();
  return v;
}
}  // namespace

TransferEngine::TransferEngine(const std::string& device_name, uint8_t ib_port,
                               int gid_index, const std::string& bind_host,
                               uint16_t bind_port, size_t max_channels_per_peer)
    : bind_host_(bind_host) {
  ctx_ = std::make_unique<RdmaContext>(device_name, ib_port, gid_index);
  conn_ = std::make_unique<ConnectionManager>(ctx_.get(), bind_host, bind_port,
                                              max_channels_per_peer);
  bind_port_ = conn_->start();
}

TransferEngine::~TransferEngine() = default;

MrHandle TransferEngine::register_mr(uint64_t addr, uint64_t length) {
  return ctx_->register_mr(addr, length);
}

void TransferEngine::deregister_mr(uint64_t addr) { ctx_->deregister_mr(addr); }

std::vector<bool> TransferEngine::batch_read(
    const std::vector<ReadRequest>& reqs) {
  std::vector<bool> ok(reqs.size(), false);

  // Group request indices by destination peer. Each peer group is served on its
  // own leased channel (QP + private CQ), so concurrent batch_read calls from
  // different threads use independent channels and never share a CQ.
  std::unordered_map<std::string, std::vector<size_t>> by_peer;
  for (size_t i = 0; i < reqs.size(); ++i) {
    by_peer[reqs[i].remote_node].push_back(i);
  }

  for (auto& kv : by_peer) {
    RdmaEndpoint* ep = conn_->lease(kv.first);
    if (!ep) continue;  // leave those requests as failed

    size_t posted = 0;
    for (size_t idx : kv.second) {
      const ReadRequest& r = reqs[idx];
      uint32_t lkey = ctx_->lkey_for(r.local_addr);
      if (lkey == 0) continue;  // destination not in a registered MR
      if (ep->post_read(r.local_addr, lkey, r.remote_addr, r.rkey, r.length,
                        static_cast<uint64_t>(idx))) {
        ++posted;
      }
    }
    // Drain this channel's own completions and mark success by wr_id. On a
    // timeout the channel may still have in-flight WRs, so discard it (rather
    // than returning it to the pool) to avoid late completions corrupting a
    // future drain; a fresh channel is established on the next lease.
    bool drained = ep->drain(posted, ok, op_timeout_s());
    if (drained) {
      conn_->release(kv.first, ep);
    } else {
      conn_->discard(kv.first, ep);
    }
  }
  return ok;
}

std::string TransferEngine::local_endpoint() const {
  return bind_host_ + ":" + std::to_string(bind_port_);
}

}  // namespace peercache
