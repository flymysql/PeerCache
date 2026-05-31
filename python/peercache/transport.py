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
    rkey: int
    lkey: int


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

    def local_endpoint(self) -> str:  # pragma: no cover
        raise NotImplementedError

    def close(self) -> None:
        pass


class RdmaTransport(Transport):
    def __init__(self, config):
        import _peercache  # built C++ extension

        if not getattr(_peercache, "HAS_RDMA", False):
            raise RuntimeError("peercache: _peercache built without RDMA support")
        self._engine = _peercache.TransferEngine(
            device_name=config.device_name,
            ib_port=config.ib_port,
            gid_index=config.gid_index,
            bind_host=config.local_hostname,
            bind_port=config.rdma_port,
        )
        self._ReadRequest = _peercache.ReadRequest

    def register_mr(self, addr: int, length: int) -> Mr:
        h = self._engine.register_mr(addr, length)
        return Mr(addr=h.addr, rkey=h.rkey, lkey=h.lkey)

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

    def local_endpoint(self) -> str:
        return self._engine.local_endpoint()


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
        self._clients: dict[str, socket.socket] = {}
        self._client_lock = threading.Lock()

        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((config.rdma_bind_host, config.rdma_port))
        self._sock.listen(128)
        _, port = self._sock.getsockname()
        self._endpoint = f"{config.local_hostname}:{port}"
        self._running = True
        threading.Thread(target=self._serve_loop, daemon=True).start()

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

    def _serve_loop(self) -> None:
        while self._running:
            try:
                conn, _ = self._sock.accept()
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

    def _drop_client(self, endpoint: str) -> None:
        with self._client_lock:
            s = self._clients.pop(endpoint, None)
        if s is not None:
            try:
                s.close()
            except Exception:
                pass

    def batch_read(self, ops: List[ReadOp]) -> List[bool]:
        results: List[bool] = []
        for op in ops:
            ok = False
            try:
                # A single in-flight request per socket keeps framing trivial.
                with self._client_lock:
                    sock = self._clients.get(op.remote_endpoint)
                    if sock is None:
                        host, port = op.remote_endpoint.rsplit(":", 1)
                        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sock.connect((host, int(port)))
                        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                        self._clients[op.remote_endpoint] = sock
                    sock.sendall(struct.pack(">QQ", op.remote_addr, op.length))
                    data = _recv_exact(sock, op.length)
                if data is not None and len(data) == op.length:
                    buf = (ctypes.c_char * op.length).from_buffer_copy(data)
                    ctypes.memmove(op.local_addr, buf, op.length)
                    ok = True
                else:
                    self._drop_client(op.remote_endpoint)
            except Exception:
                self._drop_client(op.remote_endpoint)
                ok = False
            results.append(ok)
        return results

    def local_endpoint(self) -> str:
        return self._endpoint

    def close(self) -> None:
        self._running = False
        try:
            self._sock.close()
        except Exception:
            pass


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
