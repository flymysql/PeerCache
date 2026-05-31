"""Service discovery: a single meta node tracks live membership.

The meta node does *only* discovery -- no metadata, no data. Nodes register their
endpoints, heartbeat, and pull the live membership list. Each node then builds its
own consistent-hash ring locally from that list.
"""

from __future__ import annotations

import threading
import time
from typing import Callable, Dict, List, Optional, Tuple

from peercache.rpc import RpcClient, RpcServer
from peercache.types import NodeInfo


class DiscoveryServer:
    """Runs on the meta node. Stateless except for the membership table."""

    def __init__(self, bind_host: str = "0.0.0.0", bind_port: int = 9100,
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
        return self.port

    def stop(self) -> None:
        self._rpc.stop()

    def _prune(self) -> None:
        now = time.time()
        dead = [nid for nid, (_, ts) in self._members.items() if now - ts > self._ttl]
        for nid in dead:
            self._members.pop(nid, None)

    def _on_register(self, args: dict) -> dict:
        info = args["node"]
        with self._lock:
            self._members[info["node_id"]] = (info, time.time())
            self._prune()
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
            return {"known": known}

    def _on_members(self, args: dict) -> dict:
        with self._lock:
            self._prune()
            return {"members": [m for m, _ in self._members.values()]}

    def _on_deregister(self, args: dict) -> dict:
        with self._lock:
            self._members.pop(args["node_id"], None)
            return {"ok": True}


class DiscoveryClient:
    """Runs on every node. Registers self, heartbeats, refreshes membership."""

    def __init__(
        self,
        discovery_addr: str,
        self_info: NodeInfo,
        on_members: Optional[Callable[[List[NodeInfo]], None]] = None,
        heartbeat_interval: float = 2.0,
    ):
        self._client = RpcClient(discovery_addr)
        self._self = self_info
        self._on_members = on_members
        self._interval = heartbeat_interval
        self._members: Dict[str, NodeInfo] = {}
        self._lock = threading.Lock()
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def members(self) -> Dict[str, NodeInfo]:
        with self._lock:
            return dict(self._members)

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
            self._members = members
        if self._on_members is not None:
            self._on_members(list(members.values()))

    def register(self) -> None:
        resp = self._client.call("register", {"node": self._self.to_dict()})
        self._apply_members(resp.get("members", []))

    def start(self) -> None:
        self.register()
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        try:
            self._client.call("deregister", {"node_id": self._self.node_id})
        except Exception:
            pass
        self._client.close()

    def _loop(self) -> None:
        while self._running:
            time.sleep(self._interval)
            try:
                resp = self._client.call("heartbeat", {"node_id": self._self.node_id})
                if not resp.get("known", False):
                    # Meta restarted or evicted us; re-register.
                    self.register()
                    continue
                resp = self._client.call("members")
                self._apply_members(resp.get("members", []))
            except Exception:
                # Meta transiently unreachable; keep last-known membership.
                continue
