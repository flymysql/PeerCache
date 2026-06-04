"""A tiny length-prefixed TCP RPC used by the control plane.

Frame format: 4-byte big-endian length, then a serialized ``[method, args]``
request or ``[ok, result_or_error]`` response. msgpack is used when available,
otherwise JSON (control-plane payloads are small dicts of scalars).
"""

from __future__ import annotations

import socket
import struct
import threading
from typing import Any, Callable, Dict, Optional

try:  # optional faster/compact codec
    import msgpack

    def _dumps(obj: Any) -> bytes:
        return msgpack.packb(obj, use_bin_type=True)

    def _loads(buf: bytes) -> Any:
        return msgpack.unpackb(buf, raw=False)

except Exception:  # pragma: no cover - fallback path
    import base64
    import json

    class _BytesEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, (bytes, bytearray, memoryview)):
                raw = bytes(obj)
                return {"__b64__": base64.b64encode(raw).decode("ascii")}
            return super().default(obj)

    def _json_hook(obj):
        if isinstance(obj, dict) and set(obj.keys()) == {"__b64__"}:
            return base64.b64decode(obj["__b64__"])
        return obj

    def _dumps(obj: Any) -> bytes:
        return json.dumps(obj, cls=_BytesEncoder).encode("utf-8")

    def _loads(buf: bytes) -> Any:
        return json.loads(buf.decode("utf-8"), object_hook=_json_hook)


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


def _send_frame(sock: socket.socket, payload: bytes) -> None:
    sock.sendall(struct.pack(">I", len(payload)) + payload)


def _recv_frame(sock: socket.socket) -> Optional[bytes]:
    header = _recv_exact(sock, 4)
    if header is None:
        return None
    (length,) = struct.unpack(">I", header)
    return _recv_exact(sock, length)


Handler = Callable[[dict], Any]


class RpcServer:
    """Threaded RPC server. Register handlers with ``register(method, fn)``."""

    def __init__(self, bind_host: str = "0.0.0.0", bind_port: int = 0):
        self._handlers: Dict[str, Handler] = {}
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((bind_host, bind_port))
        self._sock.listen(128)
        self.host, self.port = self._sock.getsockname()
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def register(self, method: str, fn: Handler) -> None:
        self._handlers[method] = fn

    def start(self) -> int:
        self._running = True
        self._thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._thread.start()
        return self.port

    def stop(self) -> None:
        self._running = False
        try:
            self._sock.close()
        except Exception:
            pass

    def _accept_loop(self) -> None:
        while self._running:
            try:
                conn, _ = self._sock.accept()
            except OSError:
                break
            threading.Thread(
                target=self._serve_conn, args=(conn,), daemon=True
            ).start()

    def _serve_conn(self, conn: socket.socket) -> None:
        with conn:
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            while self._running:
                frame = _recv_frame(conn)
                if frame is None:
                    return
                method, args = _loads(frame)
                handler = self._handlers.get(method)
                if handler is None:
                    resp = [False, f"unknown method: {method}"]
                else:
                    try:
                        resp = [True, handler(args or {})]
                    except Exception as e:  # surface errors to the caller
                        resp = [False, repr(e)]
                _send_frame(conn, _dumps(resp))


class RpcClient:
    """Persistent client to a single ``host:port`` with auto-reconnect."""

    def __init__(self, endpoint: str, timeout: float = 5.0):
        host, port = endpoint.rsplit(":", 1)
        self._addr = (host, int(port))
        self._timeout = timeout
        self._sock: Optional[socket.socket] = None
        self._lock = threading.Lock()

    def _connect(self) -> socket.socket:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(self._timeout)
        s.connect(self._addr)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        return s

    def call(self, method: str, args: Optional[dict] = None) -> Any:
        payload = _dumps([method, args or {}])
        with self._lock:
            for attempt in range(2):  # one reconnect retry
                try:
                    if self._sock is None:
                        self._sock = self._connect()
                    _send_frame(self._sock, payload)
                    frame = _recv_frame(self._sock)
                    if frame is None:
                        raise ConnectionError("connection closed by peer")
                    ok, result = _loads(frame)
                    if not ok:
                        raise RuntimeError(f"rpc error from {self._addr}: {result}")
                    return result
                except (OSError, ConnectionError) as e:
                    self._reset()
                    if attempt == 1:
                        raise
            raise RuntimeError("unreachable")

    def _reset(self) -> None:
        if self._sock is not None:
            try:
                self._sock.close()
            except Exception:
                pass
            self._sock = None

    def close(self) -> None:
        with self._lock:
            self._reset()


class RpcClientPool:
    """Per-endpoint pool of reusable RpcClients.

    ``call()`` leases an idle client (or opens a new connection), so multiple
    threads can issue control-plane RPCs to the same endpoint concurrently
    instead of serializing on a single shared connection. A client is returned
    to the idle list only after a successful call; on error it is closed so a
    broken connection is never reused.
    """

    def __init__(self, timeout: float = 5.0, max_idle_per_endpoint: int = 8):
        self._timeout = timeout
        self._max_idle = max(1, max_idle_per_endpoint)
        self._idle: Dict[str, list] = {}
        self._lock = threading.Lock()

    def _acquire(self, endpoint: str) -> RpcClient:
        with self._lock:
            idle = self._idle.get(endpoint)
            if idle:
                return idle.pop()
        return RpcClient(endpoint, self._timeout)

    def _release(self, endpoint: str, client: RpcClient) -> None:
        with self._lock:
            idle = self._idle.setdefault(endpoint, [])
            if len(idle) < self._max_idle:
                idle.append(client)
                return
        client.close()

    def call(self, endpoint: str, method: str, args: Optional[dict] = None) -> Any:
        client = self._acquire(endpoint)
        try:
            result = client.call(method, args)
        except Exception:
            client.close()
            raise
        self._release(endpoint, client)
        return result

    def close(self) -> None:
        with self._lock:
            for clients in self._idle.values():
                for c in clients:
                    c.close()
            self._idle.clear()
