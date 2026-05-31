#pragma once

#include <memory>
#include <string>
#include <vector>

#include "peercache/connection_manager.h"
#include "peercache/rdma_context.h"
#include "peercache/types.h"

namespace peercache {

// Top-level data-plane object exposed to Python. Owns the RDMA context and the
// connection manager. Registers local MRs and performs batched one-sided READs.
class TransferEngine {
 public:
  TransferEngine(const std::string& device_name, uint8_t ib_port, int gid_index,
                 const std::string& bind_host, uint16_t bind_port,
                 size_t max_channels_per_peer = 16);
  ~TransferEngine();

  // Register a host buffer for RDMA (as both a READ source and destination).
  MrHandle register_mr(uint64_t addr, uint64_t length);
  void deregister_mr(uint64_t addr);

  // Execute all reads (grouped per peer internally). Returns a per-request
  // success vector in the same order as `reqs`.
  std::vector<bool> batch_read(const std::vector<ReadRequest>& reqs);

  // "host:port" this engine listens on for QP bootstrap (advertised in discovery).
  std::string local_endpoint() const;

 private:
  std::unique_ptr<RdmaContext> ctx_;
  std::unique_ptr<ConnectionManager> conn_;
  std::string bind_host_;
  uint16_t bind_port_;
};

}  // namespace peercache
