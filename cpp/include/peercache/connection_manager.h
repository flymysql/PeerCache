#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "peercache/rdma_context.h"
#include "peercache/rdma_endpoint.h"

namespace peercache {

// Lazily establishes and pools one RC QP per peer.
//
// Connection bootstrap uses a tiny TCP handshake (no rdma_cm): each side creates
// its QP, exchanges QpInfo over TCP, then transitions to RTS. This keeps device
// selection (we pick device_name in RdmaContext) fully decoupled from connection
// setup. A background listener thread accepts inbound bootstrap connections so
// any peer can READ from us once they connect.
class ConnectionManager {
 public:
  ConnectionManager(RdmaContext* ctx, const std::string& bind_host,
                    uint16_t bind_port);
  ~ConnectionManager();

  // Start the bootstrap listener thread. Returns the bound port.
  uint16_t start();

  // Return an RTS endpoint to `peer` ("host:port"), connecting if necessary.
  RdmaEndpoint* get_or_connect(const std::string& peer);

  uint16_t port() const { return bind_port_; }

 private:
  void listen_loop();
  void handle_inbound(int fd);

  RdmaContext* ctx_;
  std::string bind_host_;
  uint16_t bind_port_;
  int listen_fd_ = -1;
  std::thread listener_;
  std::atomic<bool> running_{false};

  std::mutex mu_;
  std::map<std::string, std::unique_ptr<RdmaEndpoint>> endpoints_;
};

}  // namespace peercache
