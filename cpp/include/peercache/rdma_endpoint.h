#pragma once

#include <infiniband/verbs.h>

#include <cstdint>
#include <vector>

#include "peercache/rdma_context.h"
#include "peercache/types.h"

namespace peercache {

// A single reliable-connected (RC) QP to one peer, paired with its own private
// completion queue. The QP is created against the shared RdmaContext PD and
// brought to RTS once both sides have exchanged their QpInfo over the TCP
// bootstrap channel. Because every endpoint owns a dedicated CQ, several
// endpoints (channels) to the same peer can be posted to and polled fully in
// parallel from different threads without any shared-CQ contention.
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

  // Post a one-sided READ (does not wait). Returns true if the WR was accepted.
  // wr_id is echoed back in the completion so callers can correlate.
  bool post_read(uint64_t local_addr, uint32_t lkey, uint64_t remote_addr,
                 uint32_t rkey, uint64_t length, uint64_t wr_id);

  // Drain `count` completions from this endpoint's private CQ. For every
  // successful completion whose wr_id is in range, ok[wr_id] is set true.
  // Returns true if all `count` completions were collected without a poll error.
  bool drain(size_t count, std::vector<bool>& ok);

  bool connected() const { return connected_; }

 private:
  RdmaContext* ctx_;
  ibv_cq* cq_ = nullptr;
  ibv_qp* qp_ = nullptr;
  uint32_t psn_ = 0;
  bool connected_ = false;
};

}  // namespace peercache
