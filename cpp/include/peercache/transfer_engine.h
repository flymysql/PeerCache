#pragma once

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "peercache/connection_manager.h"
#include "peercache/rdma_context.h"
#include "peercache/types.h"

namespace peercache {

// Top-level data-plane object exposed to Python. Owns N RDMA "rails" (one per
// device), each a private RdmaContext + ConnectionManager. A single process can
// therefore drive several NICs: a buffer is registered on every rail, and
// batch_read_multi stripes one-sided READs across all rails in one GIL-released
// call, so the GIL never serialises the multi-NIC data path. With one device it
// behaves exactly like the original single-rail engine.
class TransferEngine {
 public:
  TransferEngine(const std::vector<std::string>& device_names, uint8_t ib_port,
                 int gid_index, const std::string& bind_host,
                 uint16_t bind_port, size_t max_channels_per_peer = 16);
  ~TransferEngine();

  size_t n_rails() const { return ctxs_.size(); }

  // Register a host buffer on every rail. Returns one MrHandle per rail (same
  // addr/length; lkey/rkey differ per device).
  std::vector<MrHandle> register_mr(uint64_t addr, uint64_t length);
  void deregister_mr(uint64_t addr);

  // Legacy single-rail reads (rail 0 only). Kept for back-compat.
  std::vector<bool> batch_read(const std::vector<ReadRequest>& reqs);
  std::vector<bool> batch_read_v(const std::vector<std::string>& remote_nodes,
                                 const std::vector<uint64_t>& local_addrs,
                                 const std::vector<uint64_t>& remote_addrs,
                                 const std::vector<uint32_t>& rkeys,
                                 const std::vector<uint64_t>& lengths);

  // Multi-rail striped reads. Per-op arrays (node_ids/local/remote/lengths)
  // describe N reads; rail_endpoints[node][r] / rail_rkeys[node][r] give the
  // owner's rail-r bootstrap endpoint and remote key for that node's pool MR.
  // Op i is issued on rail (i % n_rails) against the matching peer rail, so the
  // transfers overlap across all NICs. GIL is released for the whole call.
  std::vector<bool> batch_read_multi(
      const std::vector<std::string>& node_ids,
      const std::vector<uint64_t>& local_addrs,
      const std::vector<uint64_t>& remote_addrs,
      const std::vector<uint64_t>& lengths,
      const std::map<std::string, std::vector<std::string>>& rail_endpoints,
      const std::map<std::string, std::vector<uint32_t>>& rail_rkeys);

  // Rail-0 bootstrap endpoint (node identity) and the full per-rail list.
  std::string local_endpoint() const;
  std::vector<std::string> local_endpoints() const;

  // Cumulative data-plane counters for observability:
  //   read_timeouts   - drain() calls that hit the deadline (silent fabric)
  //   channel_discards- channels torn down after a timeout (not reused)
  //   rails           - number of RDMA rails (NICs) in this engine
  std::map<std::string, uint64_t> stats() const;

 private:
  std::string bind_host_;
  std::vector<std::unique_ptr<RdmaContext>> ctxs_;
  std::vector<std::unique_ptr<ConnectionManager>> conns_;
  std::vector<uint16_t> ports_;
  std::atomic<uint64_t> read_timeouts_{0};
  std::atomic<uint64_t> channel_discards_{0};
};

}  // namespace peercache
