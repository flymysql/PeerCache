#pragma once

#include <infiniband/verbs.h>

#include <map>
#include <mutex>
#include <string>

#include "peercache/types.h"

namespace peercache {

// Owns a single IB device context and a protection domain. Holds the registry
// of memory regions registered against this PD so we can look up the local lkey
// for any address used as a READ destination. Completion queues are owned by the
// individual endpoints (one CQ per channel) so that concurrent readers never
// share a CQ and can poll in parallel without locking.
class RdmaContext {
 public:
  // device_name may be empty -> pick the first active device.
  RdmaContext(const std::string& device_name, uint8_t ib_port, int gid_index);
  ~RdmaContext();

  RdmaContext(const RdmaContext&) = delete;
  RdmaContext& operator=(const RdmaContext&) = delete;

  // Register a host buffer; returns its lkey/rkey/addr. Thread-safe.
  MrHandle register_mr(uint64_t addr, uint64_t length);
  // Register a dmabuf-backed region (e.g. GPU memory for GPUDirect RDMA).
  // `fd` is the dmabuf file descriptor, `fd_offset` the offset within it, and
  // `addr` the device virtual address used as the IOVA. Thread-safe.
  MrHandle register_mr_dmabuf(uint64_t addr, uint64_t length, int fd,
                              uint64_t fd_offset);
  void deregister_mr(uint64_t addr);

  // Find the lkey of the registered MR that covers local_addr; 0 if none.
  uint32_t lkey_for(uint64_t local_addr) const;

  // Like lkey_for, but if no MR covers [local_addr, local_addr+length) it
  // lazily registers that range (LOCAL_WRITE -- it is a READ destination) and
  // caches the MR. This lets us read into buffers the caller never explicitly
  // registered (e.g. SGLang hands batch_get host pages outside the registered
  // KV pool). Returns 0 only if the registration itself fails. Thread-safe.
  uint32_t lkey_for_ensure(uint64_t local_addr, uint64_t length);

  // Number of MRs lazily registered by lkey_for_ensure (observability).
  uint64_t lazy_mr_count() const;

  ibv_pd* pd() const { return pd_; }
  ibv_context* context() const { return ctx_; }
  uint8_t ib_port() const { return ib_port_; }
  int gid_index() const { return gid_index_; }
  const ibv_port_attr& port_attr() const { return port_attr_; }
  union ibv_gid gid() const { return gid_; }

 private:
  ibv_context* ctx_ = nullptr;
  ibv_pd* pd_ = nullptr;
  uint8_t ib_port_ = 1;
  int gid_index_ = 0;
  ibv_port_attr port_attr_{};
  union ibv_gid gid_ {};

  mutable std::mutex mu_;
  // addr -> (length, ibv_mr*) sorted by addr for range lookup.
  std::map<uint64_t, std::pair<uint64_t, ibv_mr*>> mrs_;
  uint64_t lazy_mrs_ = 0;  // count of MRs added by lkey_for_ensure
};

}  // namespace peercache
