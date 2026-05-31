#pragma once

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "peercache/rdma_context.h"
#include "peercache/rdma_endpoint.h"

namespace peercache {

// Lazily establishes and pools a bounded set of RC QP "channels" per peer.
//
// Each channel is an RdmaEndpoint with its own private CQ, so concurrent reader
// threads can each lease an independent channel to the same peer and post/poll
// fully in parallel. Channels are reused across calls via a free list and grow
// on demand up to `max_channels_per_peer`; once the cap is reached, lease()
// blocks until a channel is released.
//
// Connection bootstrap uses a tiny TCP handshake (no rdma_cm): each side creates
// its QP, exchanges QpInfo over TCP, then transitions to RTS. A background
// listener thread accepts inbound bootstrap connections and keeps every passive
// (responder-side) QP alive so any peer can READ from us once connected.
class ConnectionManager {
 public:
  ConnectionManager(RdmaContext* ctx, const std::string& bind_host,
                    uint16_t bind_port, size_t max_channels_per_peer);
  ~ConnectionManager();

  // Start the bootstrap listener thread. Returns the bound port.
  uint16_t start();

  // Lease an RTS channel to `peer` ("host:port"), establishing a new one if the
  // pool has none free and is below the per-peer cap; blocks otherwise. Returns
  // nullptr only if a new connection could not be established. The caller MUST
  // return the channel via release() when done.
  RdmaEndpoint* lease(const std::string& peer);
  void release(const std::string& peer, RdmaEndpoint* ep);

  uint16_t port() const { return bind_port_; }

 private:
  // Bounded free-list of channels to a single peer.
  struct PeerPool {
    std::mutex mu;
    std::condition_variable cv;
    std::vector<std::unique_ptr<RdmaEndpoint>> owned;  // keeps channels alive
    std::vector<RdmaEndpoint*> free;                   // ready to lease
    size_t created = 0;
  };

  void listen_loop();
  void handle_inbound(int fd);
  PeerPool* pool_for(const std::string& peer);
  // Establish one new connected outbound channel to `peer` (TCP handshake).
  std::unique_ptr<RdmaEndpoint> connect_new(const std::string& peer);

  RdmaContext* ctx_;
  std::string bind_host_;
  uint16_t bind_port_;
  size_t max_channels_;
  int listen_fd_ = -1;
  std::thread listener_;
  std::atomic<bool> running_{false};

  std::mutex pools_mu_;
  std::unordered_map<std::string, std::unique_ptr<PeerPool>> pools_;

  // Passive responder-side QPs created from inbound handshakes; kept alive so
  // remote peers can keep issuing READs against them.
  std::mutex inbound_mu_;
  std::vector<std::unique_ptr<RdmaEndpoint>> inbound_;
};

}  // namespace peercache
