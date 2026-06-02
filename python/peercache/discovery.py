"""Service discovery: a single meta node tracks live membership.

The meta node does *only* discovery -- no metadata, no data. Nodes register their
endpoints, heartbeat, and pull the live membership list. Each node then builds its
own consistent-hash ring locally from that list.

Registration polls the meta forever (with periodic logs) instead of failing on a
timeout, so a node started before the meta simply waits for it to come up rather
than crashing the host process.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Callable, Dict, List, Optional, Tuple

from peercache.rpc import RpcClient, RpcServer
from peercache.types import NodeInfo

logger = logging.getLogger(__name__)


class DiscoveryServer:
    """Runs on the meta node. Stateless except for the membership table."""

    def __init__(self, bind_host: str = "0.0.0.0", bind_port: int = 31998,
                 member_ttl: float = 6.0):
        self._ttl = member_ttl
        self._members: Dict[str, Tuple[dict, float]] = {}
        self._lock = threading.Lock()
        self._rpc = RpcServer(bind_host, bind_port)
        self._rpc.register("register", self._on_register)
        self._rpc.register("heartbeat", self._on_heartbeat)
        self._rpc.register("members", self._on_members)
        self._rpc.register("deregister", self._on_deregister)
        self.host = self._rpc.host
        self.port = self._rpc.port

    def start(self) -> int:
        self.port = self._rpc.start()
        logger.info("discovery(meta): membership service listening on %s:%d (member_ttl=%.1fs)",
                    self.host, self.port, self._ttl)
        return self.port

    def stop(self) -> None:
        self._rpc.stop()

    def _ids(self) -> List[str]:
        return sorted(self._members.keys())

    def _prune(self) -> None:
        now = time.time()
        dead = [nid for nid, (_, ts) in self._members.items() if now - ts > self._ttl]
        for nid in dead:
            self._members.pop(nid, None)
        if dead:
            logger.info("discovery(meta): pruned %d dead node(s) (no heartbeat > %.1fs): %s; "
                        "members now (%d): %s", len(dead), self._ttl, sorted(dead),
                        len(self._members), self._ids())

    def _on_register(self, args: dict) -> dict:
        info = args["node"]
        nid = info["node_id"]
        with self._lock:
            is_new = nid not in self._members
            self._members[nid] = (info, time.time())
            self._prune()
            ids = self._ids()
        logger.info("discovery(meta): node %s %s from control=%s rdma=%s; members now (%d): %s",
                    nid, "REGISTERED" if is_new else "re-registered",
                    f"{info.get('control_host')}:{info.get('control_port')}",
                    f"{info.get('rdma_host')}:{info.get('rdma_port')}", len(ids), ids)
        return {"members": [m for m, _ in self._members.values()]}

    def _on_heartbeat(self, args: dict) -> dict:
        node_id = args["node_id"]
        with self._lock:
            entry = self._members.get(node_id)
            if entry is not None:
                self._members[node_id] = (entry[0], time.time())
                known = True
            else:
                known = False
            self._prune()
        if not known:
            logger.info("discovery(meta): heartbeat from UNKNOWN node %s -> asking it to "
                        "re-register", node_id)
        return {"known": known}

    def _on_members(self, args: dict) -> dict:
        with self._lock:
            self._prune()
            return {"members": [m for m, _ in self._members.values()]}

    def _on_deregister(self, args: dict) -> dict:
        nid = args["node_id"]
        with self._lock:
            existed = self._members.pop(nid, None) is not None
            ids = self._ids()
        if existed:
            logger.info("discovery(meta): node %s DEREGISTERED; members now (%d): %s",
                        nid, len(ids), ids)
        return {"ok": True}


class DiscoveryClient:
    """Runs on every node. Registers self, heartbeats, refreshes membership."""

    def __init__(
        self,
        discovery_addr: str,
        self_info: NodeInfo,
        on_members: Optional[Callable[[List[NodeInfo]], None]] = None,
        heartbeat_interval: float = 2.0,
        register_retry_interval: float = 2.0,
    ):
        self._addr = discovery_addr
        self._client = RpcClient(discovery_addr)
        self._self = self_info
        self._on_members = on_members
        self._interval = heartbeat_interval
        self._retry = max(0.5, register_retry_interval)
        self._members: Dict[str, NodeInfo] = {}
        self._lock = threading.Lock()
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._beats = 0

    def members(self) -> Dict[str, NodeInfo]:
        with self._lock:
            return dict(self._members)

    def _member_ids(self) -> List[str]:
        with self._lock:
            return sorted(self._members.keys())

    def endpoint_of(self, node_id: str) -> Optional[str]:
        with self._lock:
            info = self._members.get(node_id)
            return info.rdma_endpoint() if info else None

    def control_of(self, node_id: str) -> Optional[str]:
        with self._lock:
            info = self._members.get(node_id)
            return info.control_endpoint() if info else None

    def _apply_members(self, raw_members: List[dict]) -> None:
        members = {m["node_id"]: NodeInfo.from_dict(m) for m in raw_members}
        with self._lock:
            old = set(self._members.keys())
            self._members = members
        new = set(members.keys())
        joined, left = new - old, old - new
        if joined or left:
            parts = []
            if joined:
                parts.append(f"joined={sorted(joined)}")
            if left:
                parts.append(f"left={sorted(left)}")
            logger.info("discovery: membership changed -> %d node(s) %s; meta=%s; members=%s",
                        len(new), " ".join(parts), self._addr, sorted(new))
        if self._on_members is not None:
            self._on_members(list(members.values()))

    def register(self) -> None:
        resp = self._client.call("register", {"node": self._self.to_dict()})
        self._apply_members(resp.get("members", []))

    def _register_blocking(self) -> None:
        """Poll the meta until registration succeeds (or stop() is called).

        Never raises on timeout -- a node started before the meta just waits,
        logging periodically, instead of crashing the host process."""
        attempt = 0
        t0 = time.time()
        while self._running:
            attempt += 1
            try:
                self.register()
                logger.info(
                    "discovery: node=%s registered with meta %s after %d attempt(s) "
                    "(%.1fs); current members (%d): %s",
                    self._self.node_id, self._addr, attempt, time.time() - t0,
                    len(self._members), self._member_ids(),
                )
                return
            except Exception as e:
                logger.warning(
                    "discovery: node=%s waiting for meta %s (attempt %d, %.0fs elapsed): "
                    "%s; retrying in %.1fs ...",
                    self._self.node_id, self._addr, attempt, time.time() - t0, e, self._retry,
                )
                time.sleep(self._retry)

    def start(self) -> None:
        logger.info("discovery: node=%s starting; meta=%s; control=%s rdma=%s",
                    self._self.node_id, self._addr,
                    self._self.control_endpoint(), self._self.rdma_endpoint())
        self._running = True
        self._register_blocking()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        try:
            self._client.call("deregister", {"node_id": self._self.node_id})
            logger.info("discovery: node=%s deregistered from meta %s", self._self.node_id, self._addr)
        except Exception:
            pass
        self._client.close()

    def _loop(self) -> None:
        while self._running:
            time.sleep(self._interval)
            try:
                resp = self._client.call("heartbeat", {"node_id": self._self.node_id})
                known = bool(resp.get("known", False))
                self._beats += 1
                logger.info("discovery: heartbeat #%d node=%s -> meta %s (known=%s, members=%d)",
                            self._beats, self._self.node_id, self._addr, known,
                            len(self._members))
                if not known:
                    # Meta restarted or evicted us; re-register.
                    logger.info("discovery: node=%s not known by meta %s -> re-registering",
                                self._self.node_id, self._addr)
                    self.register()
                    continue
                resp = self._client.call("members")
                self._apply_members(resp.get("members", []))
            except Exception as e:
                # Meta transiently unreachable; keep last-known membership.
                logger.warning("discovery: node=%s heartbeat to meta %s failed: %s "
                               "(keeping last-known %d member(s))",
                               self._self.node_id, self._addr, e, len(self._members))
                continue
