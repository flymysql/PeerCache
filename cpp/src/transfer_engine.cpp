#include "peercache/transfer_engine.h"

#include <cstdlib>
#include <stdexcept>
#include <unordered_map>
#include <utility>

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

TransferEngine::TransferEngine(const std::vector<std::string>& device_names,
                               uint8_t ib_port, int gid_index,
                               const std::string& bind_host, uint16_t bind_port,
                               size_t max_channels_per_peer)
    : bind_host_(bind_host) {
  std::vector<std::string> devs = device_names;
  if (devs.empty()) devs.push_back("");  // single auto-picked device
  for (const auto& dev : devs) {
    auto ctx = std::make_unique<RdmaContext>(dev, ib_port, gid_index);
    auto conn = std::make_unique<ConnectionManager>(
        ctx.get(), bind_host, /*bind_port=*/0, max_channels_per_peer);
    uint16_t port = conn->start();
    ctxs_.push_back(std::move(ctx));
    conns_.push_back(std::move(conn));
    ports_.push_back(port);
  }
  (void)bind_port;  // each rail uses an ephemeral port; identity is rail 0
}

TransferEngine::~TransferEngine() = default;

std::vector<MrHandle> TransferEngine::register_mr(uint64_t addr,
                                                  uint64_t length) {
  std::vector<MrHandle> handles;
  handles.reserve(ctxs_.size());
  for (auto& ctx : ctxs_) {
    handles.push_back(ctx->register_mr(addr, length));
  }
  return handles;
}

void TransferEngine::deregister_mr(uint64_t addr) {
  for (auto& ctx : ctxs_) ctx->deregister_mr(addr);
}

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
    return ok;
  }
  RdmaContext* ctx = ctxs_[0].get();
  ConnectionManager* conn = conns_[0].get();

  std::unordered_map<std::string, std::vector<size_t>> by_peer;
  for (size_t i = 0; i < n; ++i) by_peer[remote_nodes[i]].push_back(i);

  for (auto& kv : by_peer) {
    RdmaEndpoint* ep = conn->lease(kv.first);
    if (!ep) continue;
    size_t posted = 0;
    for (size_t idx : kv.second) {
      uint32_t lkey = ctx->lkey_for(local_addrs[idx]);
      if (lkey == 0) continue;
      if (ep->post_read(local_addrs[idx], lkey, remote_addrs[idx], rkeys[idx],
                        lengths[idx], static_cast<uint64_t>(idx))) {
        ++posted;
      }
    }
    bool drained = ep->drain(posted, ok, op_timeout_s());
    if (drained) {
      conn->release(kv.first, ep);
    } else {
      conn->discard(kv.first, ep);
    }
  }
  return ok;
}

std::vector<bool> TransferEngine::batch_read(
    const std::vector<ReadRequest>& reqs) {
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

std::vector<bool> TransferEngine::batch_read_multi(
    const std::vector<std::string>& node_ids,
    const std::vector<uint64_t>& local_addrs,
    const std::vector<uint64_t>& remote_addrs,
    const std::vector<uint64_t>& lengths,
    const std::map<std::string, std::vector<std::string>>& rail_endpoints,
    const std::map<std::string, std::vector<uint32_t>>& rail_rkeys) {
  size_t n = lengths.size();
  std::vector<bool> ok(n, false);
  size_t N = ctxs_.size();
  if (N == 0 || node_ids.size() != n || local_addrs.size() != n ||
      remote_addrs.size() != n) {
    return ok;
  }

  // Group ops by (rail, endpoint). Endpoint uniquely identifies the owner rail,
  // so rkey is constant within a group.
  struct Group {
    size_t rail;
    std::string endpoint;
    uint32_t rkey;
    std::vector<size_t> idxs;
  };
  std::map<std::pair<size_t, std::string>, Group> groups;

  for (size_t i = 0; i < n; ++i) {
    size_t rail = i % N;
    auto ep_it = rail_endpoints.find(node_ids[i]);
    auto rk_it = rail_rkeys.find(node_ids[i]);
    if (ep_it == rail_endpoints.end() || rk_it == rail_rkeys.end()) continue;
    const auto& eps = ep_it->second;
    const auto& rks = rk_it->second;
    if (rail >= eps.size() || rail >= rks.size()) {
      // Fewer rails advertised than we have: fall back to rail 0.
      rail = 0;
      if (eps.empty() || rks.empty()) continue;
    }
    auto key = std::make_pair(rail, eps[rail]);
    auto& g = groups[key];
    if (g.idxs.empty()) {
      g.rail = rail;
      g.endpoint = eps[rail];
      g.rkey = rks[rail];
    }
    g.idxs.push_back(i);
  }

  // Phase 1: lease a channel per group and post (do not drain yet) so all rails
  // transfer concurrently.
  struct Leased {
    size_t rail;
    std::string endpoint;
    RdmaEndpoint* ep;
    size_t posted;
  };
  std::vector<Leased> leased;
  leased.reserve(groups.size());
  for (auto& kv : groups) {
    Group& g = kv.second;
    ConnectionManager* conn = conns_[g.rail].get();
    RdmaContext* ctx = ctxs_[g.rail].get();
    RdmaEndpoint* ep = conn->lease(g.endpoint);
    if (!ep) continue;
    size_t posted = 0;
    for (size_t idx : g.idxs) {
      uint32_t lkey = ctx->lkey_for(local_addrs[idx]);
      if (lkey == 0) continue;
      if (ep->post_read(local_addrs[idx], lkey, remote_addrs[idx], g.rkey,
                        lengths[idx], static_cast<uint64_t>(idx))) {
        ++posted;
      }
    }
    leased.push_back({g.rail, g.endpoint, ep, posted});
  }

  // Phase 2: drain every channel.
  double timeout = op_timeout_s();
  for (auto& L : leased) {
    ConnectionManager* conn = conns_[L.rail].get();
    bool drained = L.ep->drain(L.posted, ok, timeout);
    if (drained) {
      conn->release(L.endpoint, L.ep);
    } else {
      conn->discard(L.endpoint, L.ep);
    }
  }
  return ok;
}

std::string TransferEngine::local_endpoint() const {
  return bind_host_ + ":" + std::to_string(ports_.empty() ? 0 : ports_[0]);
}

std::vector<std::string> TransferEngine::local_endpoints() const {
  std::vector<std::string> eps;
  eps.reserve(ports_.size());
  for (uint16_t p : ports_) eps.push_back(bind_host_ + ":" + std::to_string(p));
  return eps;
}

}  // namespace peercache
