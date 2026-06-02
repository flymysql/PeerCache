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

from peercache.rpc import RpcClientPool, RpcServer
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
    """Runs on every node. Registers self, heartbeats, refreshes membership.

    Multi-master: discovery is replicated across the ``max_masters`` lowest
    (by hostname) live hosts -- every host runs a ``DiscoveryServer`` on the
    cluster-wide meta port, and the active masters are derived from the live
    membership, so when a master dies the next host is promoted automatically
    and a cluster with fewer than ``max_masters`` hosts has all of them as
    masters. The client registers/heartbeats to all current masters (plus the
    configured seed addresses, which stay reachable for bootstrapping new
    nodes) and merges the membership it gets back. The registry is soft state,
    so a freshly promoted/restarted master repopulates within one heartbeat.
    """

    def __init__(
        self,
        discovery_addr: str,
        self_info: NodeInfo,
        on_members: Optional[Callable[[List[NodeInfo]], None]] = None,
        heartbeat_interval: float = 2.0,
        register_retry_interval: float = 2.0,
        meta_port: Optional[int] = None,
        max_masters: int = 3,
    ):
        # discovery_addr may be a single "host:port" or a comma-separated list
        # of bootstrap seeds. The meta port is cluster-wide (every host listens
        # on it); default it to the first seed's port.
        self._seeds = [s.strip() for s in str(discovery_addr).split(",") if s.strip()]
        self._addr = ",".join(self._seeds)
        if meta_port is None:
            meta_port = int(self._seeds[0].rsplit(":", 1)[1])
        self._meta_port = int(meta_port)
        self._max_masters = max(1, int(max_masters))
        self._pool = RpcClientPool()
        self._self = self_info
        self._on_members = on_members
        self._interval = heartbeat_interval
        self._retry = max(0.5, register_retry_interval)
        self._members: Dict[str, NodeInfo] = {}
        self._lock = threading.Lock()
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._beats = 0
        # Heartbeats fire every _interval (drives liveness/TTL), but logging one
        # line per beat is noisy. Emit an INFO summary at most every
        # _log_interval seconds (or whenever membership/known state changes);
        # the in-between beats log at DEBUG.
        self._log_interval = 10.0
        self._last_log = 0.0
        self._last_log_members = -1

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

    def master_hosts(self) -> List[str]:
        """The hosts currently acting as discovery masters (diagnostics)."""
        with self._lock:
            members = list(self._members.values())
        return self._masters_from(members)

    def _masters_from(self, members: List[NodeInfo]) -> List[str]:
        """The min(max_masters, #hosts) lowest hostnames among live members."""
        hosts = sorted({m.control_host for m in members})
        return hosts[: self._max_masters]

    def _targets(self) -> List[str]:
        """Endpoints to register/heartbeat to: the derived masters plus the
        configured seeds (kept reachable so new nodes can always bootstrap)."""
        with self._lock:
            members = list(self._members.values())
        master_eps = [f"{h}:{self._meta_port}" for h in self._masters_from(members)]
        # Seeds first so a cold start (no members yet) still has a target;
        # dedup preserves order.
        return list(dict.fromkeys(self._seeds + master_eps))

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
            logger.info("discovery: membership changed -> %d node(s) %s; masters=%s; members=%s",
                        len(new), " ".join(parts),
                        self._masters_from(list(members.values())), sorted(new))
        if self._on_members is not None:
            self._on_members(list(members.values()))

    def _register_one(self, endpoint: str, merged: Dict[str, dict]) -> bool:
        try:
            resp = self._pool.call(endpoint, "register", {"node": self._self.to_dict()})
            for m in resp.get("members", []):
                merged[m["node_id"]] = m
            return True
        except Exception:
            return False

    def register(self) -> None:
        """Register with every current target; merge the membership. Raises only
        if *no* target was reachable (so _register_blocking keeps polling)."""
        merged: Dict[str, dict] = {}
        reached = sum(self._register_one(ep, merged) for ep in self._targets())
        if reached == 0:
            raise ConnectionError(f"no discovery master reachable among {self._targets()}")
        self._apply_members(list(merged.values()))

    def _register_blocking(self) -> None:
        """Poll the masters until registration succeeds (or stop() is called).

        Never raises on timeout -- a node started before any master just waits,
        logging periodically, instead of crashing the host process."""
        attempt = 0
        t0 = time.time()
        while self._running:
            attempt += 1
            try:
                self.register()
                logger.info(
                    "discovery: node=%s registered with masters %s after %d attempt(s) "
                    "(%.1fs); current members (%d): %s",
                    self._self.node_id, self.master_hosts(), attempt, time.time() - t0,
                    len(self._members), self._member_ids(),
                )
                return
            except Exception as e:
                logger.warning(
                    "discovery: node=%s waiting for a discovery master (seeds=%s) "
                    "(attempt %d, %.0fs elapsed): %s; retrying in %.1fs ...",
                    self._self.node_id, self._seeds, attempt, time.time() - t0, e, self._retry,
                )
                time.sleep(self._retry)

    def start(self) -> None:
        logger.info("discovery: node=%s starting; seeds=%s meta_port=%d max_masters=%d; "
                    "control=%s rdma=%s",
                    self._self.node_id, self._seeds, self._meta_port, self._max_masters,
                    self._self.control_endpoint(), self._self.rdma_endpoint())
        self._running = True
        self._register_blocking()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        for ep in self._targets():
            try:
                self._pool.call(ep, "deregister", {"node_id": self._self.node_id})
            except Exception:
                pass
        logger.info("discovery: node=%s deregistered from masters %s",
                    self._self.node_id, self.master_hosts())
        self._pool.close()

    def _loop(self) -> None:
        while self._running:
            time.sleep(self._interval)
            merged: Dict[str, dict] = {}
            reached = 0
            for ep in self._targets():
                try:
                    hb = self._pool.call(ep, "heartbeat", {"node_id": self._self.node_id})
                    reached += 1
                    if hb.get("known", False):
                        mem = self._pool.call(ep, "members")
                        for m in mem.get("members", []):
                            merged[m["node_id"]] = m
                    else:
                        # This master restarted or was freshly promoted and does
                        # not know us yet -> (re-)register to it.
                        self._register_one(ep, merged)
                except Exception:
                    continue
            self._beats += 1
            now = time.time()
            if reached == 0:
                # All masters transiently unreachable; keep last-known membership.
                logger.warning("discovery: node=%s reached no discovery master "
                               "(targets=%s); keeping last-known %d member(s)",
                               self._self.node_id, self._targets(), len(self._members))
                continue
            if merged:
                self._apply_members(list(merged.values()))
            members = len(self._members)
            if (now - self._last_log >= self._log_interval
                    or members != self._last_log_members):
                logger.info(
                    "discovery: heartbeat #%d node=%s -> %d/%d master(s) reachable "
                    "(members=%d, masters=%s)",
                    self._beats, self._self.node_id, reached, len(self._targets()),
                    members, self.master_hosts())
                self._last_log = now
                self._last_log_members = members
