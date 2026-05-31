#pragma once

#include <infiniband/verbs.h>

#include <cstdint>

#include "peercache/rdma_context.h"
#include "peercache/types.h"

namespace peercache {

// A single reliable-connected (RC) QP to one peer. The QP is created against the
// shared RdmaContext PD/CQ and brought to RTS once both sides have exchanged
// their QpInfo over the TCP bootstrap channel.
class RdmaEndpoint {
 public:
  explicit RdmaEndpoint(RdmaContext* ctx);
  ~RdmaEndpoint();

  RdmaEndpoint(const RdmaEndpoint&) = delete;
  RdmaEndpoint& operator=(const RdmaEndpoint&) = delete;

  // Create the QP and move it to INIT. Returns our QpInfo for the handshake.
  QpInfo create();

  // Given the peer's QpInfo, transition the QP INIT -> RTR -> RTS.
  bool connect(const QpInfo& remote);

  // Post a one-sided READ and wait for its completion. Returns true on success.
  // wr_id is echoed back in the completion so callers can correlate.
  bool post_read(uint64_t local_addr, uint32_t lkey, uint64_t remote_addr,
                 uint32_t rkey, uint64_t length, uint64_t wr_id);

  // Drain `count` completions from the shared CQ. Returns number drained.
  int poll(int count);

  bool connected() const { return connected_; }

 private:
  RdmaContext* ctx_;
  ibv_qp* qp_ = nullptr;
  uint32_t psn_ = 0;
  bool connected_ = false;
};

}  // namespace peercache
