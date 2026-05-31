#include "peercache/connection_manager.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <vector>

namespace peercache {

namespace {

bool read_full(int fd, void* buf, size_t n) {
  auto* p = static_cast<uint8_t*>(buf);
  size_t got = 0;
  while (got < n) {
    ssize_t r = ::recv(fd, p + got, n - got, 0);
    if (r <= 0) return false;
    got += static_cast<size_t>(r);
  }
  return true;
}

bool write_full(int fd, const void* buf, size_t n) {
  const auto* p = static_cast<const uint8_t*>(buf);
  size_t put = 0;
  while (put < n) {
    ssize_t r = ::send(fd, p + put, n - put, 0);
    if (r <= 0) return false;
    put += static_cast<size_t>(r);
  }
  return true;
}

// Fixed 26-byte wire layout for QpInfo (network byte order for scalars).
void serialize(const QpInfo& q, uint8_t out[26]) {
  uint32_t qp = htonl(q.qp_num);
  uint32_t psn = htonl(q.psn);
  uint16_t lid = htons(q.lid);
  std::memcpy(out + 0, &qp, 4);
  std::memcpy(out + 4, &psn, 4);
  std::memcpy(out + 8, &lid, 2);
  std::memcpy(out + 10, q.gid, 16);
}

void deserialize(const uint8_t in[26], QpInfo* q) {
  uint32_t qp, psn;
  uint16_t lid;
  std::memcpy(&qp, in + 0, 4);
  std::memcpy(&psn, in + 4, 4);
  std::memcpy(&lid, in + 8, 2);
  q->qp_num = ntohl(qp);
  q->psn = ntohl(psn);
  q->lid = ntohs(lid);
  std::memcpy(q->gid, in + 10, 16);
}

bool send_str(int fd, const std::string& s) {
  uint32_t len = htonl(static_cast<uint32_t>(s.size()));
  return write_full(fd, &len, 4) && write_full(fd, s.data(), s.size());
}

bool recv_str(int fd, std::string* s) {
  uint32_t len = 0;
  if (!read_full(fd, &len, 4)) return false;
  len = ntohl(len);
  s->resize(len);
  return len == 0 ? true : read_full(fd, &(*s)[0], len);
}

}  // namespace

ConnectionManager::ConnectionManager(RdmaContext* ctx,
                                     const std::string& bind_host,
                                     uint16_t bind_port)
    : ctx_(ctx), bind_host_(bind_host), bind_port_(bind_port) {}

ConnectionManager::~ConnectionManager() {
  running_ = false;
  if (listen_fd_ >= 0) ::shutdown(listen_fd_, SHUT_RDWR);
  if (listener_.joinable()) listener_.join();
  if (listen_fd_ >= 0) ::close(listen_fd_);
}

uint16_t ConnectionManager::start() {
  listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
  if (listen_fd_ < 0) throw std::runtime_error("peercache: socket() failed");
  int one = 1;
  ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

  sockaddr_in addr;
  std::memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = INADDR_ANY;
  addr.sin_port = htons(bind_port_);
  if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    throw std::runtime_error("peercache: bind() failed");
  }
  if (::listen(listen_fd_, 64) != 0) {
    throw std::runtime_error("peercache: listen() failed");
  }
  socklen_t slen = sizeof(addr);
  ::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &slen);
  bind_port_ = ntohs(addr.sin_port);

  running_ = true;
  listener_ = std::thread([this] { listen_loop(); });
  return bind_port_;
}

void ConnectionManager::listen_loop() {
  while (running_) {
    int fd = ::accept(listen_fd_, nullptr, nullptr);
    if (fd < 0) {
      if (!running_) break;
      continue;
    }
    handle_inbound(fd);
    ::close(fd);
  }
}

void ConnectionManager::handle_inbound(int fd) {
  // Client sends: its advertised endpoint string, then its QpInfo.
  std::string peer_key;
  uint8_t buf[26];
  if (!recv_str(fd, &peer_key)) return;
  if (!read_full(fd, buf, sizeof(buf))) return;
  QpInfo remote;
  deserialize(buf, &remote);

  auto ep = std::make_unique<RdmaEndpoint>(ctx_);
  QpInfo local = ep->create();

  uint8_t out[26];
  serialize(local, out);
  if (!write_full(fd, out, sizeof(out))) return;
  if (!ep->connect(remote)) return;

  std::lock_guard<std::mutex> lk(mu_);
  endpoints_[peer_key] = std::move(ep);
}

RdmaEndpoint* ConnectionManager::get_or_connect(const std::string& peer) {
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = endpoints_.find(peer);
    if (it != endpoints_.end() && it->second->connected()) {
      return it->second.get();
    }
  }

  auto colon = peer.rfind(':');
  if (colon == std::string::npos) return nullptr;
  std::string host = peer.substr(0, colon);
  std::string port = peer.substr(colon + 1);

  addrinfo hints;
  std::memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  addrinfo* res = nullptr;
  if (::getaddrinfo(host.c_str(), port.c_str(), &hints, &res) != 0) {
    return nullptr;
  }
  int fd = ::socket(res->ai_family, res->ai_socktype, res->ai_protocol);
  if (fd < 0) {
    ::freeaddrinfo(res);
    return nullptr;
  }
  if (::connect(fd, res->ai_addr, res->ai_addrlen) != 0) {
    ::close(fd);
    ::freeaddrinfo(res);
    return nullptr;
  }
  ::freeaddrinfo(res);

  auto ep = std::make_unique<RdmaEndpoint>(ctx_);
  QpInfo local = ep->create();

  std::string my_key = bind_host_ + ":" + std::to_string(bind_port_);
  uint8_t out[26];
  serialize(local, out);
  bool ok = send_str(fd, my_key) && write_full(fd, out, sizeof(out));

  uint8_t in[26];
  if (ok) ok = read_full(fd, in, sizeof(in));
  ::close(fd);
  if (!ok) return nullptr;

  QpInfo remote;
  deserialize(in, &remote);
  if (!ep->connect(remote)) return nullptr;

  std::lock_guard<std::mutex> lk(mu_);
  RdmaEndpoint* raw = ep.get();
  endpoints_[peer] = std::move(ep);
  return raw;
}

}  // namespace peercache
