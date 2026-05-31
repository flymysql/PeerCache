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

std::vector<bool> TransferEngine::batch_read_v(
    const std::vector<std::string>& remote_nodes,
    const std::vector<uint64_t>& local_addrs,
    const std::vector<uint64_t>& remote_addrs,
    const std::vector<uint32_t>& rkeys,
    const std::vector<uint64_t>& lengths) {
  size_t n = lengths.size();
  std::vector<bool> ok(n, false);
  if (remote_nodes.size() != n || local_addrs.size() != n ||
      remote_addrs.size() != n || rkeys.size() != n) {
    return ok;  // mismatched arrays -> all failed
  }

  // Group request indices by destination peer. Each peer group is served on its
  // own leased channel (QP + private CQ), so concurrent calls from different
  // threads use independent channels and never share a CQ.
  std::unordered_map<std::string, std::vector<size_t>> by_peer;
  for (size_t i = 0; i < n; ++i) {
    by_peer[remote_nodes[i]].push_back(i);
  }

  for (auto& kv : by_peer) {
    RdmaEndpoint* ep = conn_->lease(kv.first);
    if (!ep) continue;  // leave those requests as failed

    size_t posted = 0;
    for (size_t idx : kv.second) {
      uint32_t lkey = ctx_->lkey_for(local_addrs[idx]);
      if (lkey == 0) continue;  // destination not in a registered MR
      if (ep->post_read(local_addrs[idx], lkey, remote_addrs[idx], rkeys[idx],
                        lengths[idx], static_cast<uint64_t>(idx))) {
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

std::vector<bool> TransferEngine::batch_read(
    const std::vector<ReadRequest>& reqs) {
  // Legacy struct-based entry point: adapt to the vectorised implementation.
  size_t n = reqs.size();
  std::vector<std::string> nodes(n);
  std::vector<uint64_t> la(n), ra(n), ln(n);
  std::vector<uint32_t> rk(n);
  for (size_t i = 0; i < n; ++i) {
    nodes[i] = reqs[i].remote_node;
    la[i] = reqs[i].local_addr;
    ra[i] = reqs[i].remote_addr;
    rk[i] = reqs[i].rkey;
    ln[i] = reqs[i].length;
  }
  return batch_read_v(nodes, la, ra, rk, ln);
}

std::string TransferEngine::local_endpoint() const {
  return bind_host_ + ":" + std::to_string(bind_port_);
}

}  // namespace peercache
