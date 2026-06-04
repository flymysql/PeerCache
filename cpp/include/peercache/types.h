#pragma once

#include <cstdint>
#include <string>

namespace peercache {

// A registered memory region as seen by remote peers.
struct MrHandle {
  uint64_t addr = 0;    // virtual address of the region in this process
  uint64_t length = 0;  // region length in bytes
  uint32_t lkey = 0;    // local key (used as source/dest of our own posts)
  uint32_t rkey = 0;    // remote key (handed to peers so they can READ us)
};

// One one-sided RDMA READ: pull [remote_addr, remote_addr+length) from the
// peer identified by remote_node into our local buffer at local_addr.
struct ReadRequest {
  std::string remote_node;  // "host:port" QP-bootstrap endpoint of the source node
  uint64_t local_addr = 0;  // destination address in a locally-registered MR
  uint64_t remote_addr = 0; // source address in the peer's published-pool MR
  uint32_t rkey = 0;        // peer's rkey for that source MR
  uint64_t length = 0;      // bytes to transfer
};

// One one-sided RDMA WRITE: push [local_addr, local_addr+length) from our
// locally-registered MR into the peer's remote_addr using rkey.
struct WriteRequest {
  std::string remote_node;
  uint64_t local_addr = 0;   // source address in a locally-registered MR
  uint64_t remote_addr = 0; // destination in the peer's published-pool MR
  uint32_t rkey = 0;
  uint64_t length = 0;
};

// Wire-format QP identity exchanged during the TCP bootstrap handshake.
struct QpInfo {
  uint32_t qp_num = 0;
  uint32_t psn = 0;
  uint16_t lid = 0;
  uint8_t gid[16] = {0};
};

}  // namespace peercache
