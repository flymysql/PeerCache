"""Dedicated KV cache server for centralized (non-P2P) mode.

In ``mode=centralized``, storage nodes host the published pool, directory
shards, and optional disk tier. SGLang inference nodes (``PeerCacheStore`` with
``role=inference``) write pages via the ``data_ingest`` RPC and read them back
with one-sided RDMA READs — the same data path as P2P mode, but KV bytes live
on storage servers instead of the producing inference node.

Launch standalone (no SGLang required)::

    peercache-storage-server \\
        --discovery-addr META:31998 \\
        --global-segment-size 64gb \\
        --disk-path /data/peercache/
"""

from __future__ import annotations

import ctypes
import logging
import signal
import threading
import time
from typing import Dict, List, Optional

from peercache.config import PeerCacheConfig
from peercache.diskstore import DiskStore
from peercache.metrics import Metrics, MetricsServer
from peercache.pool import PublishedPool
from peercache.server import NodeRuntime
from peercache.store import _alloc_host_buffer
from peercache.types import DataLocation

logger = logging.getLogger(__name__)


class StorageServer:
    """Centralized-mode storage node: pool + directory shard + disk tier."""

    def __init__(self, config: PeerCacheConfig):
        if not config.is_centralized():
            raise ValueError(
                "peercache: StorageServer requires mode='centralized' "
                f"(got {config.mode!r})"
            )
        if config.role == "auto":
            config.role = "storage"
        elif config.effective_role() != "storage":
            raise ValueError(
                "peercache: StorageServer requires role='storage' or 'auto' "
                f"(got {config.role!r})"
            )

        self.config = config
        self.runtime = NodeRuntime(config)
        self._pool: Optional[PublishedPool] = None
        self._pool_keepalive = None
        self._rail_endpoints: List[str] = []
        self._key_len: Dict[str, int] = {}
        self._key_len_lock = threading.Lock()

        self._metrics = Metrics(node_id=config.node_id)
        self._metrics_server: Optional[MetricsServer] = None
        if config.metrics_enabled:
            self._metrics_server = MetricsServer(
                self._metrics,
                config.metrics_bind_host,
                config.metrics_port,
                dashboard=config.metrics_dashboard,
            )
            self._metrics_server.start()
        self._register_gauges()

        self._disk: Optional[DiskStore] = None
        if config.disk_enabled:
            try:
                self._disk = DiskStore(
                    config.disk_path,
                    config.disk_size,
                    on_evict=self._on_disk_evict,
                    node_id=config.node_id,
                )
                logger.info(
                    "StorageServer disk tier at %s (cap=%d bytes)",
                    self._disk.dir, config.disk_size,
                )
            except OSError as e:
                logger.warning(
                    "peercache: disk tier disabled, cannot use %s (%s)",
                    config.disk_path, e,
                )

        self._ensure_pool()
        self.runtime.control_rpc.register("data_ingest", self._on_data_ingest)
        self.runtime.control_rpc.register("data_promote", self._on_data_promote)
        self.runtime.add_member_listener(self._on_membership_change)

    def _ensure_pool(self) -> None:
        if self._pool is not None:
            return
        capacity = max(1, int(self.config.global_segment_size))
        self._pool_keepalive, base_addr = _alloc_host_buffer(capacity)
        pool_mr = self.runtime.transport.register_mr(base_addr, capacity)
        self._pool = PublishedPool(
            base_addr=base_addr,
            capacity=capacity,
            rkey=pool_mr.rkey,
            on_evict=self._on_pool_evict,
            rkeys=pool_mr.rkeys,
        )
        self._rail_endpoints = list(self.runtime.transport.local_endpoints())
        logger.info(
            "StorageServer pool ready: %d bytes across %d rail(s) node=%s rdma=%s",
            capacity, len(self._rail_endpoints),
            self.config.node_id, self.runtime.local_rdma_endpoint,
        )

    def _resident_location(self, remote_addr: int, length: int) -> DataLocation:
        return DataLocation(
            node_id=self.config.node_id,
            rdma_endpoint=self.runtime.local_rdma_endpoint,
            remote_addr=remote_addr,
            rkey=self._pool.rkey,
            length=length,
            resident=True,
            rail_endpoints=list(self._rail_endpoints),
            rail_rkeys=list(self._pool.rkeys),
        )

    def _on_pool_evict(self, evicted_keys: List[str]) -> None:
        self._metrics.inc("evictions", len(evicted_keys))
        try:
            if self._disk is None:
                self.runtime.directory.delete(evicted_keys)
                return
            endpoint = self.runtime.local_rdma_endpoint
            entries = {}
            with self._key_len_lock:
                lengths = {k: self._key_len.get(k) for k in evicted_keys}
            for k in evicted_keys:
                length = lengths.get(k)
                if length is None:
                    continue
                entries[k] = DataLocation(
                    node_id=self.config.node_id,
                    rdma_endpoint=endpoint,
                    remote_addr=0,
                    rkey=0,
                    length=length,
                    resident=False,
                )
            if entries:
                self.runtime.directory.put(entries)
        except Exception as e:
            logger.debug("peercache: directory update on evict failed: %s", e)

    def _on_disk_evict(self, evicted_keys: List[str]) -> None:
        self._metrics.inc("disk_evictions", len(evicted_keys))
        with self._key_len_lock:
            for k in evicted_keys:
                self._key_len.pop(k, None)
        try:
            self.runtime.directory.delete(evicted_keys)
        except Exception as e:
            logger.debug("peercache: directory delete on disk evict failed: %s", e)

    def _ensure_resident(self, keys: List[str]) -> List[Optional[DataLocation]]:
        out: List[Optional[DataLocation]] = []
        promoted: Dict[str, DataLocation] = {}
        for k in keys:
            al = self._pool.address_of(k)
            if al is not None:
                addr, length = al
                out.append(self._resident_location(addr, length))
                continue
            data = self._disk.get(k) if self._disk is not None else None
            if data is None:
                out.append(None)
                continue
            buf = (ctypes.c_byte * len(data)).from_buffer_copy(data)
            addr = self._pool.publish(k, ctypes.addressof(buf), len(data))
            if addr is None:
                out.append(None)
                continue
            loc = self._resident_location(addr, len(data))
            promoted[k] = loc
            with self._key_len_lock:
                self._key_len[k] = len(data)
            self._metrics.inc("promotes")
            out.append(loc)
        if promoted:
            try:
                self.runtime.directory.put(promoted)
            except Exception:
                pass
        return out

    def _on_data_ingest(self, args: dict) -> dict:
        """RPC: inference node pushes KV page bytes into this storage pool."""
        pages = args.get("pages") or []
        results: List[bool] = []
        entries: Dict[str, DataLocation] = {}
        published_bytes = 0
        t0 = time.perf_counter()

        keys = [p.get("key") for p in pages if p.get("key")]
        existing = (
            self.runtime.directory.exists(keys)
            if keys else []
        )
        exist_map = dict(zip(keys, existing))

        for p in pages:
            key = p.get("key")
            data = p.get("data")
            if not key or data is None:
                results.append(False)
                continue
            if exist_map.get(key):
                results.append(True)
                continue
            if not isinstance(data, (bytes, bytearray, memoryview)):
                if isinstance(data, list):
                    data = bytes(data)
                else:
                    results.append(False)
                    continue
            data = bytes(data)
            buf = (ctypes.c_byte * len(data)).from_buffer_copy(data)
            remote_addr = self._pool.publish(key, ctypes.addressof(buf), len(data))
            if remote_addr is None:
                results.append(False)
                continue
            if self._disk is not None:
                try:
                    self._disk.put(key, data)
                    self._metrics.inc("disk_writes")
                    self._metrics.inc("disk_bytes_written", len(data))
                except Exception as e:
                    logger.debug("peercache: disk write-through failed: %s", e)
            with self._key_len_lock:
                self._key_len[key] = len(data)
            entries[key] = self._resident_location(remote_addr, len(data))
            results.append(True)
            published_bytes += len(data)

        for key in list(entries.keys()):
            al = self._pool.address_of(key)
            if al is None:
                with self._key_len_lock:
                    length = self._key_len.get(key, entries[key].length)
                entries[key] = DataLocation(
                    node_id=self.config.node_id,
                    rdma_endpoint=self.runtime.local_rdma_endpoint,
                    remote_addr=0,
                    rkey=0,
                    length=length,
                    resident=False,
                )
            else:
                entries[key].remote_addr = al[0]

        if entries:
            self.runtime.directory.put(entries)
        self._metrics.record_write(published_bytes, time.perf_counter() - t0)
        return {"ok": results}

    def _on_data_promote(self, args: dict) -> dict:
        keys: List[str] = args.get("keys", [])
        locs = self._ensure_resident(keys)
        misses = [k for k, loc in zip(keys, locs) if loc is None]
        if misses:
            try:
                self.runtime.directory.delete(misses)
            except Exception:
                pass
        return {"locations": [loc.to_dict() if loc is not None else None for loc in locs]}

    def _on_membership_change(self, members) -> None:
        try:
            self._republish_directory()
        except Exception:
            pass

    def _republish_directory(self) -> None:
        if self._pool is None:
            return
        endpoint = self.runtime.local_rdma_endpoint
        entries: Dict[str, DataLocation] = {}
        for key, addr, length in self._pool.snapshot():
            entries[key] = self._resident_location(addr, length)
        if self._disk is not None:
            with self._key_len_lock:
                disk_only = {k: ln for k, ln in self._key_len.items() if k not in entries}
            for key, length in disk_only.items():
                entries[key] = DataLocation(
                    node_id=self.config.node_id, rdma_endpoint=endpoint,
                    remote_addr=0, rkey=0, length=length, resident=False,
                )
        if not entries:
            return
        keys = list(entries.keys())
        for lo in range(0, len(keys), 512):
            chunk = {k: entries[k] for k in keys[lo:lo + 512]}
            try:
                self.runtime.directory.put(chunk)
            except Exception:
                pass
        self._metrics.inc("directory_republishes")
        logger.info(
            "peercache: storage re-published %d directory entries after membership change",
            len(entries),
        )

    def _register_gauges(self) -> None:
        m = self._metrics
        m.set_gauge_provider("pool_bytes_used", lambda: self._pool.bytes_used if self._pool else 0)
        m.set_gauge_provider("pool_capacity_bytes", lambda: self._pool.capacity if self._pool else 0)
        m.set_gauge_provider("pool_keys", lambda: len(self._pool) if self._pool else 0)
        m.set_gauge_provider("disk_bytes_used", lambda: self._disk.stats()[0] if self._disk else 0)
        m.set_gauge_provider("disk_capacity_bytes", lambda: self.config.disk_size if self._disk else 0)
        m.set_gauge_provider("disk_keys", lambda: self._disk.stats()[1] if self._disk else 0)
        m.set_gauge_provider("members", lambda: len(self.runtime.discovery.members()))
        m.set_gauge_provider("storage_nodes", lambda: len(self.runtime.storage_nodes()))

    def start(self) -> None:
        self.runtime.start()

    def stop(self) -> None:
        if getattr(self, "_stopped", False):
            return
        self._stopped = True
        if self._metrics_server is not None:
            self._metrics_server.stop()
        if self._disk is not None:
            self._disk.close()
        self.runtime.stop()

    def run_forever(self) -> None:
        """Block until SIGINT/SIGTERM."""
        self.start()
        stop = {"flag": False}

        def _handle(signum, frame):
            stop["flag"] = True

        signal.signal(signal.SIGINT, _handle)
        signal.signal(signal.SIGTERM, _handle)
        logger.info(
            "StorageServer running: node=%s discovery=%s control=%s:%d",
            self.config.node_id,
            self.config.discovery_addr,
            self.config.local_hostname,
            self.runtime.info.control_port,
        )
        try:
            while not stop["flag"]:
                time.sleep(0.5)
        finally:
            self.stop()
            logger.info("StorageServer stopped")
