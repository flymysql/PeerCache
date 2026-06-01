"""Data-plane transport abstraction.

Two implementations share one interface:

- ``RdmaTransport``: wraps the C++ ``_peercache.TransferEngine`` (raw libibverbs
  one-sided RDMA READ, zero copy).
- ``TcpTransport``: a pure-Python fallback that mirrors the same API over TCP so
  the discovery + directory + pool design can be validated end-to-end on hosts
  without RDMA hardware. It still performs a genuine remote read into the
  destination address (via ``ctypes.memmove``), just over a socket.

Both expose:
    register_mr(addr, length) -> Mr(addr, rkey, lkey)
    batch_read(list[ReadOp]) -> list[bool]
    local_endpoint() -> "host:port"
"""

from __future__ import annotations

import ctypes
import socket
import struct
import threading
from dataclasses import dataclass
from typing import List, Optional, Tuple


@dataclass
class Mr:
    addr: int
    rkey: int           # rail-0 remote key (back-compat)
    lkey: int           # rail-0 local key (back-compat)
    rkeys: List[int] = None  # per-rail remote keys (len == n_rails)
    lkeys: List[int] = None  # per-rail local keys

    def __post_init__(self):
        if self.rkeys is None:
            self.rkeys = [self.rkey]
        if self.lkeys is None:
            self.lkeys = [self.lkey]


@dataclass
class ReadOp:
    remote_endpoint: str  # data node "host:port"
    local_addr: int
    remote_addr: int
    rkey: int
    length: int


class Transport:
    def register_mr(self, addr: int, length: int) -> Mr:  # pragma: no cover
        raise NotImplementedError

    def deregister_mr(self, addr: int) -> None:  # pragma: no cover
        raise NotImplementedError

    def batch_read(self, ops: List[ReadOp]) -> List[bool]:  # pragma: no cover
        raise NotImplementedError

    def batch_read_v(
        self,
        remote_nodes: List[str],
        local_addrs: List[int],
        remote_addrs: List[int],
        rkeys: List[int],
        lengths: List[int],
    ) -> List[bool]:
        """Vectorised read: parallel arrays instead of ReadOp objects.

        Default builds ReadOp objects and delegates to ``batch_read``; the RDMA
        transport overrides this to call the C++ engine directly (no per-op
        Python/pybind object on the GIL-held hot path)."""
        ops = [
            ReadOp(remote_nodes[i], local_addrs[i], remote_addrs[i],
                   rkeys[i], lengths[i])
            for i in range(len(lengths))
        ]
        return self.batch_read(ops)

    def n_rails(self) -> int:
        return 1

    def stats(self) -> dict:
        """Cumulative data-plane counters (read_timeouts, channel_discards, rails)."""
        return {"read_timeouts": 0, "channel_discards": 0, "rails": self.n_rails()}

    def local_endpoints(self) -> List[str]:
        return [self.local_endpoint()]

    def batch_read_multi(
        self,
        node_ids: List[str],
        local_addrs: List[int],
        remote_addrs: List[int],
        lengths: List[int],
        rail_endpoints: dict,
        rail_rkeys: dict,
    ) -> List[bool]:
        """Multi-rail striped read. Op i uses rail (i % n_rails) of its owner.

        Default (single-rail) builds ReadOps against rail 0 and delegates."""
        N = max(1, self.n_rails())
        nodes: List[str] = []
        for i, node in enumerate(node_ids):
            eps = rail_endpoints.get(node) or []
            rail = i % N if (i % N) < len(eps) else 0
            nodes.append(eps[rail] if eps else "")
        # rkeys per op selected on the same rail.
        rks: List[int] = []
        for i, node in enumerate(node_ids):
            ks = rail_rkeys.get(node) or [0]
            rail = i % N if (i % N) < len(ks) else 0
            rks.append(ks[rail] if ks else 0)
        return self.batch_read_v(nodes, local_addrs, remote_addrs, rks, lengths)

    def local_endpoint(self) -> str:  # pragma: no cover
        raise NotImplementedError

    def close(self) -> None:
        pass


class RdmaTransport(Transport):
    def __init__(self, config):
        from peercache import _peercache  # built C++ extension (peercache._peercache)

        if not getattr(_peercache, "HAS_RDMA", False):
            raise RuntimeError("peercache: _peercache built without RDMA support")
        devices = config.device_rails() if hasattr(config, "device_rails") else [config.device_name]
        self._engine = _peercache.TransferEngine(
            device_names=devices,
            ib_port=config.ib_port,
            gid_index=config.gid_index,
            bind_host=config.local_hostname,
            bind_port=config.rdma_port,
            max_channels_per_peer=getattr(config, "max_channels_per_peer", 16),
        )
        self._ReadRequest = _peercache.ReadRequest

    def n_rails(self) -> int:
        return int(self._engine.n_rails())

    def register_mr(self, addr: int, length: int) -> Mr:
        handles = self._engine.register_mr(addr, length)  # one per rail
        rkeys = [h.rkey for h in handles]
        lkeys = [h.lkey for h in handles]
        return Mr(addr=addr, rkey=rkeys[0], lkey=lkeys[0], rkeys=rkeys, lkeys=lkeys)

    def deregister_mr(self, addr: int) -> None:
        self._engine.deregister_mr(addr)

    def batch_read(self, ops: List[ReadOp]) -> List[bool]:
        reqs = [
            self._ReadRequest(
                remote_node=op.remote_endpoint,
                local_addr=op.local_addr,
                remote_addr=op.remote_addr,
                rkey=op.rkey,
                length=op.length,
            )
            for op in ops
        ]
        return list(self._engine.batch_read(reqs))

    def batch_read_v(self, remote_nodes, local_addrs, remote_addrs, rkeys, lengths):
        # Hot path: hand the raw arrays straight to C++ (GIL released there),
        # avoiding one Python/pybind object per op.
        return list(self._engine.batch_read_v(
            remote_nodes, local_addrs, remote_addrs, rkeys, lengths))

    def batch_read_multi(self, node_ids, local_addrs, remote_addrs, lengths,
                         rail_endpoints, rail_rkeys):
        # Stripe across all rails inside one GIL-released C++ call.
        return list(self._engine.batch_read_multi(
            node_ids, local_addrs, remote_addrs, lengths,
            rail_endpoints, rail_rkeys))

    def stats(self) -> dict:
        return dict(self._engine.stats())

    def local_endpoint(self) -> str:
        return self._engine.local_endpoint()

    def local_endpoints(self) -> List[str]:
        return list(self._engine.local_endpoints())


# --------------------------------------------------------------------------- #
# TCP fallback
# --------------------------------------------------------------------------- #

def _recv_exact(sock: socket.socket, n: int) -> Optional[bytes]:
    chunks = []
    got = 0
    while got < n:
        b = sock.recv(n - got)
        if not b:
            return None
        chunks.append(b)
        got += len(b)
    return b"".join(chunks)


class TcpTransport(Transport):
    """Serves reads from this process's registered memory over a raw socket.

    Request frame:  16 bytes = (remote_addr u64, length u64), big-endian.
    Response frame: `length` raw bytes copied from local memory at remote_addr.
    """

    def __init__(self, config):
        self._regions: List[Tuple[int, int]] = []  # (addr, length) for validation
        self._lock = threading.Lock()
        # Per-endpoint pool of idle sockets so that multiple reader threads can
        # each lease their own socket and issue reads in parallel (a socket only
        # ever has one in-flight request, keeping the framing trivial).
        self._idle: dict[str, List[socket.socket]] = {}
        self._pool_lock = threading.Lock()
        self._max_idle = max(1, getattr(config, "max_channels_per_peer", 16))

        # Multi-rail: one listener per "rail" (mirrors the RDMA per-device rails
        # so the multi-rail read path is exercisable over TCP). All rails serve
        # the same process memory, so any rail can satisfy any read.
        n_rails = len(config.device_rails()) if hasattr(config, "device_rails") else 1
        n_rails = max(1, n_rails)
        self._socks: List[socket.socket] = []
        self._endpoints: List[str] = []
        for _ in range(n_rails):
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((config.rdma_bind_host, 0))
            s.listen(128)
            _, port = s.getsockname()
            self._socks.append(s)
            self._endpoints.append(f"{config.local_hostname}:{port}")
        self._sock = self._socks[0]
        self._endpoint = self._endpoints[0]
        self._running = True
        for s in self._socks:
            threading.Thread(target=self._serve_loop, args=(s,), daemon=True).start()

    def register_mr(self, addr: int, length: int) -> Mr:
        with self._lock:
            self._regions.append((addr, length))
        return Mr(addr=addr, rkey=0, lkey=0)

    def deregister_mr(self, addr: int) -> None:
        with self._lock:
            self._regions = [(a, l) for (a, l) in self._regions if a != addr]

    def _covered(self, addr: int, length: int) -> bool:
        with self._lock:
            for base, ln in self._regions:
                if addr >= base and addr + length <= base + ln:
                    return True
        return False

    def _serve_loop(self, sock: socket.socket) -> None:
        while self._running:
            try:
                conn, _ = sock.accept()
            except OSError:
                break
            threading.Thread(target=self._serve_conn, args=(conn,), daemon=True).start()

    def _serve_conn(self, conn: socket.socket) -> None:
        with conn:
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            while self._running:
                hdr = _recv_exact(conn, 16)
                if hdr is None:
                    return
                remote_addr, length = struct.unpack(">QQ", hdr)
                if not self._covered(remote_addr, length):
                    return  # close on invalid request
                data = ctypes.string_at(remote_addr, length)
                try:
                    conn.sendall(data)
                except OSError:
                    return

    def _acquire(self, endpoint: str) -> socket.socket:
        with self._pool_lock:
            idle = self._idle.get(endpoint)
            if idle:
                return idle.pop()
        host, port = endpoint.rsplit(":", 1)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, int(port)))
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        return sock

    def _release(self, endpoint: str, sock: socket.socket) -> None:
        with self._pool_lock:
            idle = self._idle.setdefault(endpoint, [])
            if len(idle) < self._max_idle:
                idle.append(sock)
                return
        try:
            sock.close()
        except Exception:
            pass

    def batch_read(self, ops: List[ReadOp]) -> List[bool]:
        results: List[bool] = []
        for op in ops:
            ok = False
            sock = None
            try:
                # Lease a dedicated socket; a single in-flight request per socket
                # keeps framing trivial while allowing cross-thread parallelism.
                sock = self._acquire(op.remote_endpoint)
                sock.sendall(struct.pack(">QQ", op.remote_addr, op.length))
                data = _recv_exact(sock, op.length)
                if data is not None and len(data) == op.length:
                    buf = (ctypes.c_char * op.length).from_buffer_copy(data)
                    ctypes.memmove(op.local_addr, buf, op.length)
                    ok = True
                    self._release(op.remote_endpoint, sock)
                    sock = None
            except Exception:
                ok = False
            if sock is not None:  # error path: do not reuse a broken socket
                try:
                    sock.close()
                except Exception:
                    pass
            results.append(ok)
        return results

    def local_endpoint(self) -> str:
        return self._endpoint

    def local_endpoints(self) -> List[str]:
        return list(self._endpoints)

    def n_rails(self) -> int:
        return len(self._endpoints)

    def close(self) -> None:
        self._running = False
        for s in getattr(self, "_socks", [self._sock]):
            try:
                s.close()
            except Exception:
                pass
        with self._pool_lock:
            for socks in self._idle.values():
                for s in socks:
                    try:
                        s.close()
                    except Exception:
                        pass
            self._idle.clear()


def create_transport(config) -> Transport:
    """Pick a transport based on config.protocol and availability."""
    if config.protocol == "tcp":
        return TcpTransport(config)
    try:
        return RdmaTransport(config)
    except Exception as e:  # RDMA unavailable -> graceful fallback
        import logging

        logging.getLogger(__name__).warning(
            "peercache: RDMA transport unavailable (%s); using TCP fallback", e
        )
        return TcpTransport(config)
