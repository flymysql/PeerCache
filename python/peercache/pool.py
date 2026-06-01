"""The backend-owned published pool.

This is the source side of every remote READ. ``set()`` copies a KV page from
SGLang's host buffer into this pool (a node-local ``memmove`` -- no network, no
master) and records its offset; the pool is registered as one RDMA MR so peers can
READ from ``base_addr + offset`` using ``rkey``.

When the pool fills up, the least-recently-used page is evicted and its key is
reported via ``on_evict`` so the owner can delete the directory entry, keeping the
published address valid until it is evicted.

Note: pages within one pool share a fixed byte size (KV page size), so an
exact-size free list fully avoids fragmentation.
"""

from __future__ import annotations

import ctypes
import threading
from collections import OrderedDict
from typing import Callable, Dict, List, Optional, Tuple


class PublishedPool:
    def __init__(
        self,
        base_addr: int,
        capacity: int,
        rkey: int,
        on_evict: Optional[Callable[[List[str]], None]] = None,
        rkeys: Optional[List[int]] = None,
    ):
        self._base = base_addr
        self._capacity = capacity
        self._rkey = rkey
        # Per-rail remote keys for the same MR (multi-NIC). Defaults to [rkey].
        self._rkeys = list(rkeys) if rkeys else [rkey]
        self._on_evict = on_evict

        self._next_offset = 0
        self._free: Dict[int, List[int]] = {}  # length -> [free offsets]
        self._entries: "OrderedDict[str, Tuple[int, int]]" = OrderedDict()
        self._used = 0
        self._lock = threading.Lock()

    @property
    def rkey(self) -> int:
        return self._rkey

    @property
    def rkeys(self) -> List[int]:
        return self._rkeys

    @property
    def base_addr(self) -> int:
        return self._base

    @property
    def capacity(self) -> int:
        return self._capacity

    @property
    def bytes_used(self) -> int:
        with self._lock:
            return self._used

    def __contains__(self, key: str) -> bool:
        with self._lock:
            return key in self._entries

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)

    def address_of(self, key: str) -> Optional[Tuple[int, int]]:
        """Return (remote_addr, length) for a published key, or None."""
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return None
            offset, length = entry
            self._entries.move_to_end(key)
            return self._base + offset, length

    def _allocate(self, length: int, evicted: List[str]) -> Optional[int]:
        """Find an offset for `length` bytes, evicting LRU pages if needed.

        Caller holds the lock. Evicted keys are appended to `evicted`.
        """
        if length > self._capacity:
            return None
        free_list = self._free.get(length)
        if free_list:
            return free_list.pop()
        if self._next_offset + length <= self._capacity:
            off = self._next_offset
            self._next_offset += length
            return off
        # Evict LRU pages until a matching-size slot frees up.
        while self._entries:
            old_key, (old_off, old_len) = self._entries.popitem(last=False)
            self._used -= old_len
            evicted.append(old_key)
            self._free.setdefault(old_len, []).append(old_off)
            if old_len == length:
                return self._free[length].pop()
        return None

    def publish(self, key: str, src_ptr: int, length: int) -> Optional[int]:
        """Copy `length` bytes from `src_ptr` into the pool; return remote_addr.

        Returns None if the page cannot fit. Triggers `on_evict` (outside the
        lock) for any pages displaced to make room.
        """
        evicted: List[str] = []
        remote_addr: Optional[int] = None
        with self._lock:
            existing = self._entries.get(key)
            if existing is not None:
                self._entries.move_to_end(key)
                return self._base + existing[0]
            off = self._allocate(length, evicted)
            if off is None:
                remote_addr = None
            else:
                ctypes.memmove(self._base + off, src_ptr, length)
                self._entries[key] = (off, length)
                self._entries.move_to_end(key)
                self._used += length
                remote_addr = self._base + off
        if evicted and self._on_evict is not None:
            self._on_evict(evicted)
        return remote_addr

    def snapshot(self) -> List[Tuple[str, int, int]]:
        """Return [(key, remote_addr, length)] for all currently-resident pages.

        Used to re-publish directory entries after a ring-membership change so
        the entries re-shard onto the current owners."""
        with self._lock:
            return [(k, self._base + off, ln) for k, (off, ln) in self._entries.items()]

    def remove(self, key: str) -> None:
        with self._lock:
            entry = self._entries.pop(key, None)
            if entry is not None:
                off, length = entry
                self._used -= length
                self._free.setdefault(length, []).append(off)
