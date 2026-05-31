"""The distributed directory (DHT).

Each node hosts one *shard* of the directory: a local key -> DataLocation map.
The consistent-hash ring decides which node owns a key's directory entry. There is
no central metadata store -- the directory is the union of all shards.

- write: producer PUTs ``key -> {node, addr, rkey, len}`` to ``hash(key)``'s owner.
- read:  reader GETs the location from ``hash(key)``'s owner, then RDMA-READs the
  data directly from the producing node.
- eviction: when a node evicts a page from its published pool it DELETEs the
  corresponding directory entry so readers stop seeing a stale address.
"""

from __future__ import annotations

import threading
from typing import Callable, Dict, List, Optional

from peercache.hashring import ConsistentHashRing
from peercache.rpc import RpcClientPool, RpcServer
from peercache.types import DataLocation


class DirectoryServer:
    """Local directory shard. Handlers are attached to a shared RpcServer."""

    def __init__(self):
        self._store: Dict[str, dict] = {}
        self._lock = threading.Lock()

    def attach(self, rpc: RpcServer) -> None:
        rpc.register("dir_put", self._on_put)
        rpc.register("dir_get", self._on_get)
        rpc.register("dir_exists", self._on_exists)
        rpc.register("dir_delete", self._on_delete)

    def _on_put(self, args: dict) -> dict:
        entries: Dict[str, dict] = args["entries"]
        with self._lock:
            self._store.update(entries)
        return {"ok": True}

    def _on_get(self, args: dict) -> dict:
        keys: List[str] = args["keys"]
        with self._lock:
            return {"locations": [self._store.get(k) for k in keys]}

    def _on_exists(self, args: dict) -> dict:
        keys: List[str] = args["keys"]
        with self._lock:
            return {"exists": [k in self._store for k in keys]}

    def _on_delete(self, args: dict) -> dict:
        keys: List[str] = args["keys"]
        with self._lock:
            for k in keys:
                self._store.pop(k, None)
        return {"ok": True}

    # Local-shard size (diagnostics).
    def __len__(self) -> int:
        with self._lock:
            return len(self._store)


class DirectoryClient:
    """Routes directory ops to owning shards via the consistent-hash ring."""

    def __init__(
        self,
        ring: ConsistentHashRing,
        resolve_control: Callable[[str], Optional[str]],
        replicas: int = 1,
        pool: Optional[RpcClientPool] = None,
    ):
        self._ring = ring
        self._resolve = resolve_control  # node_id -> "host:port" control endpoint
        self._replicas = max(1, replicas)
        self._pool = pool or RpcClientPool()

    def _owners(self, key: str) -> List[str]:
        if self._replicas == 1:
            node = self._ring.get_node(key)
            return [node] if node else []
        return self._ring.get_nodes(key, self._replicas)

    def _group_by_owner(self, keys: List[str]) -> Dict[str, List[str]]:
        """Group keys by their *primary* owner (used for read path)."""
        groups: Dict[str, List[str]] = {}
        for k in keys:
            owners = self._owners(k)
            if not owners:
                continue
            groups.setdefault(owners[0], []).append(k)
        return groups

    def put(self, entries: Dict[str, DataLocation]) -> None:
        """Publish locations. Writes to all replicas of each key."""
        # node_id -> {key: location_dict}
        per_node: Dict[str, Dict[str, dict]] = {}
        for key, loc in entries.items():
            for owner in self._owners(key):
                per_node.setdefault(owner, {})[key] = loc.to_dict()
        for node_id, node_entries in per_node.items():
            endpoint = self._resolve(node_id)
            if endpoint is None:
                continue
            try:
                self._pool.call(endpoint, "dir_put", {"entries": node_entries})
            except Exception:
                continue  # best-effort publish; missing entry == cache miss later

    def get(self, keys: List[str]) -> List[Optional[DataLocation]]:
        """Look up locations, preserving input order. None == not found."""
        result: Dict[str, Optional[DataLocation]] = {k: None for k in keys}
        for owner, group in self._group_by_owner(keys).items():
            locs = self._get_from_owner(keys, group, owner)
            for k, loc in zip(group, locs):
                result[k] = loc
        return [result[k] for k in keys]

    def _get_from_owner(
        self, all_keys: List[str], group: List[str], primary: str
    ) -> List[Optional[DataLocation]]:
        # Try primary then (for replicated dirs) the remaining replica owners.
        candidates = [primary]
        if self._replicas > 1:
            for k in group[:1]:
                candidates = self._owners(k)
        for node_id in candidates:
            endpoint = self._resolve(node_id)
            if endpoint is None:
                continue
            try:
                resp = self._pool.call(endpoint, "dir_get", {"keys": group})
                raw = resp.get("locations", [])
                return [DataLocation.from_dict(r) if r else None for r in raw]
            except Exception:
                continue
        return [None] * len(group)

    def exists(self, keys: List[str]) -> List[bool]:
        result: Dict[str, bool] = {k: False for k in keys}
        for owner, group in self._group_by_owner(keys).items():
            endpoint = self._resolve(owner)
            if endpoint is None:
                continue
            try:
                resp = self._pool.call(endpoint, "dir_exists", {"keys": group})
                for k, ex in zip(group, resp.get("exists", [])):
                    result[k] = bool(ex)
            except Exception:
                continue
        return [result[k] for k in keys]

    def delete(self, keys: List[str]) -> None:
        per_node: Dict[str, List[str]] = {}
        for k in keys:
            for owner in self._owners(k):
                per_node.setdefault(owner, []).append(k)
        for node_id, group in per_node.items():
            endpoint = self._resolve(node_id)
            if endpoint is None:
                continue
            try:
                self._pool.call(endpoint, "dir_delete", {"keys": group})
            except Exception:
                continue
