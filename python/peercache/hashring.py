"""Consistent-hash ring with virtual nodes.

Used to map a cache key to the node that owns its *directory* entry, and to pick
the replica set when ``directory_replicas > 1``.
"""

from __future__ import annotations

import bisect
import hashlib
from typing import Dict, Iterable, List, Optional


def _hash(data: str) -> int:
    return int.from_bytes(hashlib.blake2b(data.encode("utf-8"), digest_size=8).digest(), "big")


class ConsistentHashRing:
    def __init__(self, vnodes: int = 160):
        self._vnodes = vnodes
        self._ring: Dict[int, str] = {}
        self._sorted_keys: List[int] = []
        self._nodes: set[str] = set()

    @property
    def nodes(self) -> List[str]:
        return sorted(self._nodes)

    def __contains__(self, node_id: str) -> bool:
        return node_id in self._nodes

    def __len__(self) -> int:
        return len(self._nodes)

    def add_node(self, node_id: str) -> None:
        if node_id in self._nodes:
            return
        self._nodes.add(node_id)
        for i in range(self._vnodes):
            h = _hash(f"{node_id}#{i}")
            self._ring[h] = node_id
        self._rebuild()

    def remove_node(self, node_id: str) -> None:
        if node_id not in self._nodes:
            return
        self._nodes.discard(node_id)
        for i in range(self._vnodes):
            self._ring.pop(_hash(f"{node_id}#{i}"), None)
        self._rebuild()

    def set_nodes(self, node_ids: Iterable[str]) -> None:
        """Replace the membership in one shot (used after a discovery refresh)."""
        target = set(node_ids)
        if target == self._nodes:
            return
        self._ring.clear()
        self._nodes = target
        for node_id in target:
            for i in range(self._vnodes):
                self._ring[_hash(f"{node_id}#{i}")] = node_id
        self._rebuild()

    def _rebuild(self) -> None:
        self._sorted_keys = sorted(self._ring.keys())

    def get_node(self, key: str) -> Optional[str]:
        if not self._sorted_keys:
            return None
        h = _hash(key)
        idx = bisect.bisect(self._sorted_keys, h)
        if idx == len(self._sorted_keys):
            idx = 0
        return self._ring[self._sorted_keys[idx]]

    def get_nodes(self, key: str, n: int) -> List[str]:
        """Return up to ``n`` distinct nodes clockwise from ``hash(key)``."""
        if not self._sorted_keys or n <= 0:
            return []
        n = min(n, len(self._nodes))
        h = _hash(key)
        idx = bisect.bisect(self._sorted_keys, h)
        result: List[str] = []
        count = len(self._sorted_keys)
        i = idx
        while len(result) < n:
            node = self._ring[self._sorted_keys[i % count]]
            if node not in result:
                result.append(node)
            i += 1
            if i - idx > count:  # safety: walked the whole ring
                break
        return result
