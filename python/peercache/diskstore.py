"""The disk persistence tier (L4) for the published pool.

When a page is published it is also written to disk asynchronously (write-through).
When the in-memory pool evicts a page it stays on disk, so the owner can promote it
back into the pool on a later read (local or remote). The disk tier is itself
LRU-bounded by ``max_bytes``; evicting from disk deletes the directory entry via
the ``on_evict`` callback (the page is then truly gone -> a cache miss).

On-disk layout: one file per key under ``data_dir``, named ``<sha1(key)>.bin`` with
a small header ``[4-byte big-endian keylen][key utf-8][raw page bytes]`` so the
index can be rebuilt by scanning if the sidecar ``index.json`` is missing.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import struct
import threading
from collections import OrderedDict
from queue import Queue
from typing import Callable, List, Optional, Tuple

logger = logging.getLogger(__name__)

_INDEX_FILE = "index.json"
_STOP = object()


def _safe_name(key: str) -> str:
    return hashlib.sha1(key.encode("utf-8")).hexdigest() + ".bin"


class DiskStore:
    def __init__(
        self,
        data_dir: str,
        max_bytes: int,
        on_evict: Optional[Callable[[List[str]], None]] = None,
        node_id: str = "",
    ):
        # Isolate each node under its own subdir so a shared filesystem is safe.
        self.dir = os.path.join(data_dir, node_id) if node_id else data_dir
        os.makedirs(self.dir, exist_ok=True)
        self.max_bytes = max_bytes
        self._on_evict = on_evict

        self._index: "OrderedDict[str, Tuple[str, int]]" = OrderedDict()  # key -> (name, length)
        self._used = 0
        self._lock = threading.Lock()
        self._inflight = set()
        self._writes_since_flush = 0

        self._q: "Queue" = Queue()
        self._worker = threading.Thread(target=self._run, daemon=True, name="peercache-disk")
        self._running = True
        self._load_index()
        self._worker.start()

    # -- index persistence -------------------------------------------------- #
    def _index_path(self) -> str:
        return os.path.join(self.dir, _INDEX_FILE)

    def _load_index(self) -> None:
        path = self._index_path()
        if os.path.exists(path):
            try:
                with open(path, "r") as f:
                    data = json.load(f)
                for key, (name, length) in data.items():
                    fp = os.path.join(self.dir, name)
                    if os.path.exists(fp):
                        self._index[key] = (name, int(length))
                        self._used += int(length)
                return
            except Exception as e:
                logger.warning("peercache disk: index load failed (%s); rescanning", e)
                self._index.clear()
                self._used = 0
        self._rebuild_from_scan()

    def _rebuild_from_scan(self) -> None:
        for name in sorted(os.listdir(self.dir)):
            if not name.endswith(".bin"):
                continue
            fp = os.path.join(self.dir, name)
            try:
                with open(fp, "rb") as f:
                    hdr = f.read(4)
                    if len(hdr) < 4:
                        continue
                    (klen,) = struct.unpack(">I", hdr)
                    key = f.read(klen).decode("utf-8")
                length = os.path.getsize(fp) - 4 - klen
                if length >= 0:
                    self._index[key] = (name, length)
                    self._used += length
            except Exception:
                continue

    def _flush_index(self) -> None:
        tmp = self._index_path() + ".tmp"
        try:
            with open(tmp, "w") as f:
                json.dump({k: [n, l] for k, (n, l) in self._index.items()}, f)
            os.replace(tmp, self._index_path())
        except Exception as e:
            logger.debug("peercache disk: index flush failed: %s", e)

    # -- async writer ------------------------------------------------------- #
    def put(self, key: str, data: bytes) -> None:
        """Enqueue an async write-through of `data` for `key` (idempotent)."""
        if not self._running or len(data) > self.max_bytes:
            return
        with self._lock:
            if key in self._index or key in self._inflight:
                return
            self._inflight.add(key)
        self._q.put((key, data))

    def _run(self) -> None:
        while True:
            item = self._q.get()
            if item is _STOP:
                break
            key, data = item
            try:
                self._write(key, data)
            except Exception as e:
                logger.debug("peercache disk: write failed for %r: %s", key, e)
            finally:
                with self._lock:
                    self._inflight.discard(key)

    def _write(self, key: str, data: bytes) -> None:
        name = _safe_name(key)
        fp = os.path.join(self.dir, name)
        tmp = fp + ".tmp"
        kb = key.encode("utf-8")
        with open(tmp, "wb") as f:
            f.write(struct.pack(">I", len(kb)))
            f.write(kb)
            f.write(data)
        os.replace(tmp, fp)
        evicted: List[str] = []
        with self._lock:
            if key not in self._index:
                self._used += len(data)
            self._index[key] = (name, len(data))
            self._index.move_to_end(key)
            self._writes_since_flush += 1
            # Enforce capacity (LRU from the front).
            while self._used > self.max_bytes and len(self._index) > 1:
                old_key, (old_name, old_len) = self._index.popitem(last=False)
                self._used -= old_len
                evicted.append(old_key)
                try:
                    os.remove(os.path.join(self.dir, old_name))
                except OSError:
                    pass
            flush = self._writes_since_flush >= 256
            if flush:
                self._writes_since_flush = 0
        if flush:
            self._flush_index()
        if evicted and self._on_evict is not None:
            self._on_evict(evicted)

    # -- reads -------------------------------------------------------------- #
    def get(self, key: str) -> Optional[bytes]:
        with self._lock:
            entry = self._index.get(key)
            if entry is None:
                return None
            name, length = entry
            self._index.move_to_end(key)
        fp = os.path.join(self.dir, name)
        try:
            with open(fp, "rb") as f:
                hdr = f.read(4)
                (klen,) = struct.unpack(">I", hdr)
                f.seek(4 + klen)
                return f.read(length)
        except Exception:
            return None

    def exists(self, key: str) -> bool:
        with self._lock:
            return key in self._index

    def remove(self, key: str) -> None:
        with self._lock:
            entry = self._index.pop(key, None)
            if entry is None:
                return
            name, length = entry
            self._used -= length
        try:
            os.remove(os.path.join(self.dir, name))
        except OSError:
            pass

    def stats(self) -> Tuple[int, int]:
        with self._lock:
            return self._used, len(self._index)

    def close(self) -> None:
        self._running = False
        self._q.put(_STOP)
        try:
            self._worker.join(timeout=5.0)
        except Exception:
            pass
        self._flush_index()
