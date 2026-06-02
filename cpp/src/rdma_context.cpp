#include "peercache/rdma_context.h"

#include <cstring>
#include <iterator>
#include <stdexcept>
#include <string>

namespace peercache {

RdmaContext::RdmaContext(const std::string& device_name, uint8_t ib_port,
                         int gid_index)
    : ib_port_(ib_port), gid_index_(gid_index) {
  int num_devices = 0;
  ibv_device** dev_list = ibv_get_device_list(&num_devices);
  if (!dev_list || num_devices == 0) {
    throw std::runtime_error("peercache: no RDMA devices found");
  }

  ibv_device* dev = nullptr;
  if (device_name.empty()) {
    dev = dev_list[0];
  } else {
    for (int i = 0; i < num_devices; ++i) {
      if (device_name == ibv_get_device_name(dev_list[i])) {
        dev = dev_list[i];
        break;
      }
    }
  }
  if (!dev) {
    ibv_free_device_list(dev_list);
    throw std::runtime_error("peercache: RDMA device '" + device_name +
                             "' not found");
  }

  ctx_ = ibv_open_device(dev);
  ibv_free_device_list(dev_list);
  if (!ctx_) throw std::runtime_error("peercache: ibv_open_device failed");

  pd_ = ibv_alloc_pd(ctx_);
  if (!pd_) throw std::runtime_error("peercache: ibv_alloc_pd failed");

  if (ibv_query_port(ctx_, ib_port_, &port_attr_) != 0) {
    throw std::runtime_error("peercache: ibv_query_port failed");
  }
  std::memset(&gid_, 0, sizeof(gid_));
  if (gid_index_ >= 0) {
    if (ibv_query_gid(ctx_, ib_port_, gid_index_, &gid_) != 0) {
      throw std::runtime_error("peercache: ibv_query_gid failed");
    }
  }
}

RdmaContext::~RdmaContext() {
  {
    std::lock_guard<std::mutex> lk(mu_);
    for (auto& kv : mrs_) ibv_dereg_mr(kv.second.second);
    mrs_.clear();
  }
  if (pd_) ibv_dealloc_pd(pd_);
  if (ctx_) ibv_close_device(ctx_);
}

MrHandle RdmaContext::register_mr(uint64_t addr, uint64_t length) {
  int access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
               IBV_ACCESS_REMOTE_WRITE;
  ibv_mr* mr = ibv_reg_mr(pd_, reinterpret_cast<void*>(addr),
                          static_cast<size_t>(length), access);
  if (!mr) throw std::runtime_error("peercache: ibv_reg_mr failed");

  {
    std::lock_guard<std::mutex> lk(mu_);
    mrs_[addr] = {length, mr};
  }
  MrHandle h;
  h.addr = addr;
  h.length = length;
  h.lkey = mr->lkey;
  h.rkey = mr->rkey;
  return h;
}

MrHandle RdmaContext::register_mr_dmabuf(uint64_t addr, uint64_t length, int fd,
                                         uint64_t fd_offset) {
  int access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
               IBV_ACCESS_REMOTE_WRITE;
  ibv_mr* mr = ibv_reg_dmabuf_mr(pd_, fd_offset, static_cast<size_t>(length),
                                 addr, fd, access);
  if (!mr) {
    throw std::runtime_error(
        "peercache: ibv_reg_dmabuf_mr failed -- needs rdma-core>=33 and a NIC + "
        "driver with GPUDirect/dmabuf support (ConnectX + MOFED, or "
        "nvidia-peermem). Check the dmabuf fd/offset and that the device "
        "supports peer memory.");
  }
  {
    std::lock_guard<std::mutex> lk(mu_);
    mrs_[addr] = {length, mr};
  }
  MrHandle h;
  h.addr = addr;
  h.length = length;
  h.lkey = mr->lkey;
  h.rkey = mr->rkey;
  return h;
}

void RdmaContext::deregister_mr(uint64_t addr) {
  std::lock_guard<std::mutex> lk(mu_);
  auto it = mrs_.find(addr);
  if (it != mrs_.end()) {
    ibv_dereg_mr(it->second.second);
    mrs_.erase(it);
  }
}

uint32_t RdmaContext::lkey_for(uint64_t local_addr) const {
  std::lock_guard<std::mutex> lk(mu_);
  // Largest base address <= local_addr.
  auto it = mrs_.upper_bound(local_addr);
  if (it == mrs_.begin()) return 0;
  --it;
  uint64_t base = it->first;
  uint64_t len = it->second.first;
  if (local_addr >= base && local_addr < base + len) {
    return it->second.second->lkey;
  }
  return 0;
}

uint32_t RdmaContext::lkey_for_ensure(uint64_t local_addr, uint64_t length) {
  uint32_t lk = lkey_for(local_addr);
  if (lk) return lk;
  if (length == 0) return 0;
  // Register exactly the destination range. SGLang reuses a bounded set of host
  // page buffers, so the cache converges after the first touch of each page;
  // ibv_reg_mr pins the underlying pages (LOCAL_WRITE is enough for a READ
  // destination). Done outside the lock since ibv_reg_mr can be slow.
  ibv_mr* mr = ibv_reg_mr(pd_, reinterpret_cast<void*>(local_addr),
                          static_cast<size_t>(length), IBV_ACCESS_LOCAL_WRITE);
  if (!mr) return 0;
  std::lock_guard<std::mutex> lock(mu_);
  // Another thread may have covered this range while we were registering.
  auto it = mrs_.upper_bound(local_addr);
  if (it != mrs_.begin()) {
    auto prev = std::prev(it);
    if (local_addr >= prev->first &&
        local_addr + length <= prev->first + prev->second.first) {
      ibv_dereg_mr(mr);
      return prev->second.second->lkey;
    }
  }
  mrs_[local_addr] = {length, mr};
  ++lazy_mrs_;
  return mr->lkey;
}

uint64_t RdmaContext::lazy_mr_count() const {
  std::lock_guard<std::mutex> lk(mu_);
  return lazy_mrs_;
}

}  // namespace peercache
