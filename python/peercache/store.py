"""PeerCacheStore: the SGLang HiCache L3 storage backend.

Registered with SGLang via the ``dynamic`` backend mechanism (no SGLang patch):

    --hicache-storage-backend dynamic
    --hicache-storage-backend-extra-config
        '{"backend_name":"peercache","module_path":"peercache.store",
          "class_name":"PeerCacheStore","discovery_addr":"META:31998", ...}'

Write path  : ``set`` copies the page into the node-local published pool
              (no network) and PUTs ``key -> {node,addr,rkey,len}`` to the
              directory shard chosen by consistent hashing.
Read path   : ``get`` looks up the directory, then issues a one-sided RDMA READ
              straight into SGLang's registered host buffer (zero copy). If the
              data lives on this node, it is a local memcpy instead.
"""

from __future__ import annotations

import ctypes
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional

from peercache.config import PeerCacheConfig
from peercache.diskstore import DiskStore
from peercache.metrics import Metrics, MetricsServer
from peercache.pool import PublishedPool
from peercache.rpc import RpcClientPool
from peercache.server import NodeRuntime
from peercache.types import DataLocation

logger = logging.getLogger(__name__)

# SGLang is optional at import time so the package can be tested standalone.
try:
    from sglang.srt.mem_cache.hicache_storage import (  # type: ignore
        HiCacheStorage,
        HiCacheStorageConfig,
        HiCacheStorageExtraInfo,
        PoolName,
    )

    _HAS_SGLANG = True
except Exception:  # pragma: no cover - standalone / test path
    _HAS_SGLANG = False

    class HiCacheStorage:  # minimal stand-in
        def register_mem_pool_host(self, mem_pool_host):
            self.mem_pool_host = mem_pool_host

        def register_mem_host_pool_v2(self, host_pool, host_pool_name):
            if not hasattr(self, "registered_pools"):
                self.registered_pools = {}
            self.registered_pools[host_pool_name] = host_pool

    HiCacheStorageConfig = Any  # type: ignore
    HiCacheStorageExtraInfo = Any  # type: ignore

    class PoolName(str):  # type: ignore
        KV = "kv"


_pinned_warned = False


def _warn_unpinned(reason: str) -> None:
    global _pinned_warned
    if not _pinned_warned:
        _pinned_warned = True
        logger.warning(
            "peercache: host buffers are NOT page-locked (%s); RDMA throughput "
            "will be reduced. Install torch so pinned memory can be used.",
            reason,
        )


def _alloc_host_buffer(size: int):
    """Allocate a pinned/host buffer and return (keepalive_obj, base_addr)."""
    try:
        import torch

        # Pinned memory is required for real RDMA registration; falls back to
        # pageable if pinning fails (still fine for the TCP transport).
        try:
            t = torch.empty(size, dtype=torch.uint8, pin_memory=True)
        except Exception as e:
            _warn_unpinned(f"torch pin_memory failed: {e}")
            t = torch.empty(size, dtype=torch.uint8)
        return t, t.data_ptr()
    except Exception:
        _warn_unpinned("torch not available")
        buf = (ctypes.c_byte * size)()
        return buf, ctypes.addressof(buf)


class PeerCacheStore(HiCacheStorage):
    def __init__(self, storage_config: "HiCacheStorageConfig" = None, extra: Optional[dict] = None):
        extra_config = {}
        if storage_config is not None and getattr(storage_config, "extra_config", None):
            extra_config.update(storage_config.extra_config)
        if extra:
            extra_config.update(extra)

        self.config = PeerCacheConfig.from_extra_config(extra_config)
        self.storage_config = storage_config

        # Identity / key-suffix parameters (mirror Mooncake's layout).
        self.tp_rank = getattr(storage_config, "tp_rank", 0) or 0
        self.tp_size = getattr(storage_config, "tp_size", 1) or 1
        self.pp_rank = getattr(storage_config, "pp_rank", 0) or 0
        self.pp_size = getattr(storage_config, "pp_size", 1) or 1
        self.is_mla = bool(getattr(storage_config, "is_mla_model", False))
        enable_pp = self.pp_size > 1
        if enable_pp:
            self.mha_suffix = f"{self.tp_rank}_{self.pp_rank}"
            self.mla_suffix = f"{self.pp_rank}"
        else:
            self.mha_suffix = f"{self.tp_rank}"
            self.mla_suffix = ""

        self.runtime = NodeRuntime(self.config)
        self.runtime.start()
        logger.info(
            "PeerCacheStore up: node=%s rdma=%s control=%s:%d discovery=%s",
            self.config.node_id,
            self.runtime.local_rdma_endpoint,
            self.config.local_hostname,
            self.runtime.info.control_port,
            self.config.discovery_addr,
        )

        self.mem_pool_host = None
        self.registered_pools = {}
        self._pool: Optional[PublishedPool] = None
        self._pool_keepalive = None
        self._recv_mr = None
        # Per-rail (multi-NIC) bootstrap endpoints this node advertises for its
        # published pool; reads stripe across them. Defaults to the single rail.
        self._rail_endpoints: List[str] = []
        # _key_len is touched by concurrent batch_set / batch_get / eviction
        # callbacks, so all access is guarded to stay safe under threaded SGLang.
        self._key_len: Dict[str, int] = {}
        self._key_len_lock = threading.Lock()

        # Optional read-location cache (see directory_read_cache_ttl): maps a
        # component key -> (DataLocation, expiry_monotonic). Skips the directory
        # RPC for hot, static working sets.
        self._dir_cache_ttl = float(getattr(self.config, "directory_read_cache_ttl", 0.0) or 0.0)
        self._dir_cache: Dict[str, tuple] = {}
        self._dir_cache_lock = threading.Lock()

        # Metrics + monitoring (optional, default on).
        self._metrics = Metrics(node_id=self.config.node_id)
        self._metrics_server: Optional[MetricsServer] = None
        if self.config.metrics_enabled:
            self._metrics_server = MetricsServer(
                self._metrics,
                self.config.metrics_bind_host,
                self.config.metrics_port,
                dashboard=self.config.metrics_dashboard,
            )
            self._metrics_server.start()
        self._register_gauges()

        # Disk persistence tier (optional, default on; degrades gracefully if the
        # configured directory cannot be created).
        self._disk: Optional[DiskStore] = None
        if self.config.disk_enabled:
            try:
                self._disk = DiskStore(
                    self.config.disk_path,
                    self.config.disk_size,
                    on_evict=self._on_disk_evict,
                    node_id=self.config.node_id,
                )
                logger.info(
                    "PeerCache disk tier at %s (cap=%d bytes)",
                    self._disk.dir, self.config.disk_size,
                )
            except OSError as e:
                logger.warning(
                    "peercache: disk tier disabled, cannot use %s (%s)",
                    self.config.disk_path, e,
                )
                self._disk = None

        # RPC pool for cross-node promote calls (data-plane control).
        self._data_rpc = RpcClientPool()
        self._prefetch = ThreadPoolExecutor(
            max_workers=2, thread_name_prefix="peercache-prefetch"
        )
        self.runtime.control_rpc.register("data_promote", self._on_data_promote)

        # Re-shard the directory when ring membership changes: the consistent-hash
        # owner of a key can move when a node joins/leaves, and entries are not
        # migrated automatically, so each producer re-publishes the locations of
        # the pages it owns onto the (new) owners. Runs off the discovery thread.
        self.runtime.add_member_listener(self._on_membership_change)

    # ------------------------------------------------------------------ #
    # Registration
    # ------------------------------------------------------------------ #
    def _register_buffer(self, addr: int, length: int, buf=None):
        """Register a buffer for RDMA, using the dmabuf path for GPU memory when
        the buffer exposes a dmabuf fd, else a plain MR (host, or GPU memory when
        nvidia-peermem is loaded)."""
        transport = self.runtime.transport
        fd = None
        offset = 0
        if buf is not None:
            getfd = getattr(buf, "dmabuf_fd", None)
            if callable(getfd):
                try:
                    fd = int(getfd())
                    off = getattr(buf, "dmabuf_offset", None)
                    offset = int(off()) if callable(off) else 0
                except Exception:
                    fd = None
        try:
            if fd is not None and fd >= 0 and hasattr(transport, "register_mr_dmabuf"):
                return transport.register_mr_dmabuf(addr, length, fd, offset)
            return transport.register_mr(addr, length)
        except Exception as e:
            raise RuntimeError(
                f"peercache: failed to register a {length}-byte buffer for RDMA "
                f"({e}). For GPU buffers (GPUDirect) ensure the NIC/driver support "
                f"peer memory (nvidia-peermem loaded, or a dmabuf-capable stack)."
            ) from e

    def register_mem_pool_host(self, mem_pool_host):
        self.mem_pool_host = mem_pool_host

        # 1) Receive MR: SGLang's KV buffer is the destination of READs. It may
        #    live in GPU memory (GPUDirect RDMA): if the buffer exposes a dmabuf
        #    fd we register via ibv_reg_dmabuf_mr; otherwise a plain ibv_reg_mr
        #    of the (device) virtual address, which works when nvidia-peermem is
        #    loaded. A registration failure here usually means GPUDirect isn't
        #    available on the host.
        kv = mem_pool_host.kv_buffer
        kv_ptr = kv.data_ptr()
        kv_bytes = kv.numel() * kv.element_size()
        self._recv_mr = self._register_buffer(kv_ptr, kv_bytes, kv)

        # 2) Published-pool MR: backend-owned source of remote READs (per-TP slice).
        capacity = max(1, self.config.global_segment_size // self.tp_size)
        self._pool_keepalive, base_addr = _alloc_host_buffer(capacity)
        pool_mr = self.runtime.transport.register_mr(base_addr, capacity)
        self._pool = PublishedPool(
            base_addr=base_addr,
            capacity=capacity,
            rkey=pool_mr.rkey,
            on_evict=self._on_pool_evict,
            rkeys=pool_mr.rkeys,
        )
        # Endpoints peers use to READ this pool, one per rail (NIC).
        self._rail_endpoints = list(self.runtime.transport.local_endpoints())
        logger.info(
            "PeerCacheStore registered MRs: recv=%d bytes, pool=%d bytes",
            kv_bytes,
            capacity,
        )

    def register_mem_host_pool_v2(self, host_pool, host_pool_name):
        self.registered_pools[host_pool_name] = host_pool
        # Extra (hybrid) pools' buffers must also be RDMA-registered so peers can
        # READ them; they share the same published-pool publish path on write.
        for buf in getattr(host_pool, "get_hybrid_pool_buffer", lambda: [])():
            self.runtime.transport.register_mr(
                buf.data_ptr(), buf.numel() * buf.element_size()
            )

    def _resident_location(self, remote_addr: int, length: int) -> DataLocation:
        """Build a resident DataLocation carrying all rail endpoints/rkeys so a
        multi-rail reader can stripe the READ across every NIC."""
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
        # A page left the in-memory pool. With a disk tier the page is still on
        # disk (write-through), so keep the directory entry but mark it
        # non-resident; readers will trigger a promote. Without disk, drop it.
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
        except Exception as e:  # best-effort
            logger.debug("peercache: directory update on evict failed: %s", e)

    def _on_disk_evict(self, evicted_keys: List[str]) -> None:
        # A page left the disk tier too -> it is truly gone; remove its directory
        # entry so readers see a clean miss.
        self._metrics.inc("disk_evictions", len(evicted_keys))
        with self._key_len_lock:
            for k in evicted_keys:
                self._key_len.pop(k, None)
        try:
            self.runtime.directory.delete(evicted_keys)
        except Exception as e:
            logger.debug("peercache: directory delete on disk evict failed: %s", e)

    def _register_gauges(self) -> None:
        m = self._metrics
        m.set_gauge_provider("pool_bytes_used", lambda: self._pool.bytes_used if self._pool else 0)
        m.set_gauge_provider("pool_capacity_bytes", lambda: self._pool.capacity if self._pool else 0)
        m.set_gauge_provider("pool_keys", lambda: len(self._pool) if self._pool else 0)
        m.set_gauge_provider("disk_bytes_used", lambda: self._disk.stats()[0] if self._disk else 0)
        m.set_gauge_provider("disk_capacity_bytes", lambda: self.config.disk_size if self._disk else 0)
        m.set_gauge_provider("disk_keys", lambda: self._disk.stats()[1] if self._disk else 0)
        m.set_gauge_provider("members", lambda: len(self.runtime.discovery.members()))
        # Data-plane (transport) gauges: rails (NICs) and cumulative timeouts /
        # channel discards surfaced from the C++ engine (0 on the TCP fallback).
        def _tstat(key):
            try:
                return self.runtime.transport.stats().get(key, 0)
            except Exception:
                return 0
        m.set_gauge_provider("rdma_rails", lambda: _tstat("rails"))
        m.set_gauge_provider("rdma_read_timeouts", lambda: _tstat("read_timeouts"))
        m.set_gauge_provider("rdma_channel_discards", lambda: _tstat("channel_discards"))

    # ------------------------------------------------------------------ #
    # Disk promote: load a key from disk back into the pool (makes it readable)
    # ------------------------------------------------------------------ #
    def _ensure_resident(self, keys: List[str]) -> List[Optional[DataLocation]]:
        """For each key, return a resident DataLocation (in this node's pool) or
        None. Promotes from disk into the pool when necessary."""
        out: List[Optional[DataLocation]] = []
        promoted: Dict[str, DataLocation] = {}
        for k in keys:
            if self._pool is None:
                out.append(None)
                continue
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

    def _on_membership_change(self, members) -> None:
        """Dispatch a directory re-shard off the discovery/heartbeat thread."""
        if self._pool is None:
            return
        try:
            self._prefetch.submit(self._republish_directory)
        except Exception:
            pass

    def _republish_directory(self) -> None:
        """Re-PUT this node's directory entries so they land on the current
        owners after a membership change (resident pages from the pool, plus
        disk-only pages as non-resident). Best-effort and idempotent."""
        if self._pool is None:
            return
        endpoint = self.runtime.local_rdma_endpoint
        entries: Dict[str, DataLocation] = {}
        for key, addr, length in self._pool.snapshot():
            entries[key] = self._resident_location(addr, length)
        if self._disk is not None:
            with self._key_len_lock:
                disk_only = {k: l for k, l in self._key_len.items() if k not in entries}
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
        logger.info("peercache: re-published %d directory entries after a "
                    "membership change", len(entries))

    def _on_data_promote(self, args: dict) -> dict:
        """RPC handler: a remote reader asks us to promote disk-resident keys."""
        keys: List[str] = args.get("keys", [])
        locs = self._ensure_resident(keys)
        misses = [k for k, l in zip(keys, locs) if l is None]
        if misses:
            try:
                self.runtime.directory.delete(misses)
            except Exception:
                pass
        return {"locations": [l.to_dict() if l is not None else None for l in locs]}

    # ------------------------------------------------------------------ #
    # Key suffixing (mirrors Mooncake MHA k/v split and MLA single-key)
    # ------------------------------------------------------------------ #
    def _component_keys(self, keys: List[str]):
        """Return (component_keys, multiplier) aligned with get_page_buffer_meta."""
        out: List[str] = []
        if self.is_mla:
            for k in keys:
                out.append(f"{k}_{self.mla_suffix}_k")
            return out, 1
        for k in keys:
            out.append(f"{k}_{self.mha_suffix}_k")
            out.append(f"{k}_{self.mha_suffix}_v")
        return out, 2

    @staticmethod
    def _page_results(comp_results: List[bool], multiplier: int) -> List[bool]:
        return [
            all(comp_results[i : i + multiplier])
            for i in range(0, len(comp_results), multiplier)
        ]

    # ------------------------------------------------------------------ #
    # v1 zero-copy paths (primary)
    # ------------------------------------------------------------------ #
    def batch_set_v1(self, keys, host_indices, extra_info=None) -> List[bool]:
        comp_keys, mult = self._component_keys(keys)
        ptrs, sizes = self.mem_pool_host.get_page_buffer_meta(host_indices)
        assert len(comp_keys) == len(ptrs) == len(sizes)
        comp_results = self._publish(comp_keys, ptrs, sizes)
        return self._page_results(comp_results, mult)

    def batch_get_v1(self, keys, host_indices, extra_info=None) -> List[bool]:
        comp_keys, mult = self._component_keys(keys)
        ptrs, sizes = self.mem_pool_host.get_page_buffer_meta(host_indices)
        assert len(comp_keys) == len(ptrs) == len(sizes)
        comp_results = self._fetch(comp_keys, ptrs, sizes)
        return self._page_results(comp_results, mult)

    def batch_exists(self, keys, extra_info=None) -> int:
        comp_keys, mult = self._component_keys(keys)
        # The directory retains entries for disk-resident pages (marked
        # non-resident), so a hit here already covers "in memory OR on disk".
        ex = self.runtime.directory.exists(comp_keys)
        n = len(comp_keys) // mult
        for i, present in enumerate(ex):
            if not present:
                n = i // mult
                break
        if n and self._disk is not None:
            # Warm the hit prefix back into the pool for the imminent get.
            self._prefetch_async(comp_keys[: n * mult])
        return n

    # ------------------------------------------------------------------ #
    # Core publish / fetch over component objects
    # ------------------------------------------------------------------ #
    def _publish(self, comp_keys: List[str], ptrs: List[int], sizes: List[int]) -> List[bool]:
        t0 = time.perf_counter()
        # Skip components already present in the directory (idempotent set).
        existing = self.runtime.directory.exists(comp_keys)
        entries = {}
        results = [False] * len(comp_keys)
        endpoint = self.runtime.local_rdma_endpoint
        published_bytes = 0
        for i, key in enumerate(comp_keys):
            if existing[i]:
                results[i] = True
                continue
            remote_addr = self._pool.publish(key, ptrs[i], sizes[i])
            if remote_addr is None:
                continue  # pool could not fit this page
            # Write-through to the disk tier (async) so the page survives pool
            # eviction and can be promoted back / read remotely later.
            if self._disk is not None:
                try:
                    self._disk.put(key, ctypes.string_at(ptrs[i], sizes[i]))
                    self._metrics.inc("disk_writes")
                    self._metrics.inc("disk_bytes_written", sizes[i])
                except Exception as e:
                    logger.debug("peercache: disk write-through failed: %s", e)
            with self._key_len_lock:
                self._key_len[key] = sizes[i]
            entries[key] = self._resident_location(remote_addr, sizes[i])
            results[i] = True
            published_bytes += sizes[i]
        # Reconcile: a page published earlier in THIS batch may have been evicted
        # by a later page (pool full). Such a key is now on disk only, so publish
        # it as non-resident rather than re-asserting a stale resident address.
        for key in list(entries.keys()):
            al = self._pool.address_of(key)
            if al is None:
                with self._key_len_lock:
                    length = self._key_len.get(key, entries[key].length)
                entries[key] = DataLocation(
                    node_id=self.config.node_id,
                    rdma_endpoint=endpoint,
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
        return results

    def _dir_get(self, comp_keys: List[str]) -> List[Optional[DataLocation]]:
        """directory.get with an optional short-TTL resident-location cache."""
        if self._dir_cache_ttl <= 0:
            return self.runtime.directory.get(comp_keys)
        now = time.monotonic()
        out: List[Optional[DataLocation]] = [None] * len(comp_keys)
        miss_keys: List[str] = []
        miss_idx: List[int] = []
        with self._dir_cache_lock:
            for i, k in enumerate(comp_keys):
                ent = self._dir_cache.get(k)
                if ent is not None and ent[1] > now:
                    out[i] = ent[0]
                else:
                    miss_keys.append(k)
                    miss_idx.append(i)
        if miss_keys:
            fresh = self.runtime.directory.get(miss_keys)
            exp = now + self._dir_cache_ttl
            with self._dir_cache_lock:
                for j, loc in enumerate(fresh):
                    out[miss_idx[j]] = loc
                    # Only cache resident locations; non-resident entries still
                    # need the promote path resolved on every access.
                    if loc is not None and loc.resident:
                        self._dir_cache[miss_keys[j]] = (loc, exp)
        return out

    def _dir_cache_invalidate(self, keys: List[str]) -> None:
        if self._dir_cache_ttl <= 0 or not keys:
            return
        with self._dir_cache_lock:
            for k in keys:
                self._dir_cache.pop(k, None)

    def _fetch(self, comp_keys: List[str], ptrs: List[int], sizes: List[int]) -> List[bool]:
        t0 = time.perf_counter()
        locations = self._dir_get(comp_keys)
        results = [False] * len(comp_keys)
        sources: List[Optional[str]] = [None] * len(comp_keys)

        # 1) Resolve non-resident entries (evicted to disk) back into a pool MR.
        #    Remote keys are promoted by their owner via RPC; self-owned keys are
        #    promoted locally (loads disk -> pool == "prefetch back into LRU").
        promoted = self._resolve_non_resident(comp_keys, locations)

        # Build parallel arrays for the remote reads (no per-op Python object on
        # the GIL-held hot path); local hits are served by memmove inline. The
        # per-node rail maps let the transport stripe each batch across all of
        # the owner's NICs (rails) inside one GIL-released call.
        r_nodes: List[str] = []
        r_local: List[int] = []
        r_remote: List[int] = []
        r_len: List[int] = []
        op_index: List[int] = []
        rail_eps: Dict[str, List[str]] = {}
        rail_rks: Dict[str, List[int]] = {}
        for i, loc in enumerate(locations):
            if loc is None or not loc.resident:
                continue
            if loc.length != sizes[i]:
                continue  # size mismatch -> treat as miss
            if loc.node_id == self.config.node_id:
                ctypes.memmove(ptrs[i], loc.remote_addr, loc.length)
                results[i] = True
                sources[i] = "disk" if i in promoted else "local"
                continue
            nk = loc.rdma_endpoint  # rail-0 endpoint identifies the owner
            if nk not in rail_eps:
                rail_eps[nk] = loc.endpoints()
                rail_rks[nk] = loc.rkeys()
            r_nodes.append(nk)
            r_local.append(ptrs[i])
            r_remote.append(loc.remote_addr)
            r_len.append(loc.length)
            op_index.append(i)
        if op_index:
            oks = self.runtime.transport.batch_read_multi(
                r_nodes, r_local, r_remote, r_len, rail_eps, rail_rks)
            failed = 0
            for j, ok in enumerate(oks):
                idx = op_index[j]
                results[idx] = bool(ok)
                if ok:
                    sources[idx] = "disk" if idx in promoted else "remote"
                else:
                    failed += 1
            # A resident location was found but the RDMA READ failed (timeout /
            # fabric error) -- distinct from a directory miss.
            if failed:
                self._metrics.inc("read_failures", failed)

        latency = time.perf_counter() - t0
        for i in range(len(comp_keys)):
            self._metrics.record_read(results[i], sizes[i] if results[i] else 0,
                                      latency, sources[i])
        # Drop any failed keys from the read cache so a stale/evicted location
        # self-heals on the next access (re-resolved via the directory).
        if self._dir_cache_ttl > 0:
            self._dir_cache_invalidate([k for i, k in enumerate(comp_keys) if not results[i]])
        return results

    def _resolve_non_resident(self, comp_keys, locations) -> set:
        """In-place: promote any non-resident entries so they become readable.

        Returns the set of indices that were promoted (served from disk)."""
        promoted: set = set()
        remote_by_owner: Dict[str, List[int]] = {}
        local_idx: List[int] = []
        for i, loc in enumerate(locations):
            if loc is None or loc.resident:
                continue
            if loc.node_id == self.config.node_id:
                local_idx.append(i)
            else:
                remote_by_owner.setdefault(loc.node_id, []).append(i)

        for i in local_idx:
            new = self._ensure_resident([comp_keys[i]])[0]
            locations[i] = new
            if new is not None:
                promoted.add(i)

        for owner, idxs in remote_by_owner.items():
            endpoint = self.runtime.discovery.control_of(owner)
            if endpoint is None:
                for i in idxs:
                    locations[i] = None
                continue
            keys = [comp_keys[i] for i in idxs]
            try:
                resp = self._data_rpc.call(endpoint, "data_promote", {"keys": keys})
                newlocs = resp.get("locations", [])
                for i, nl in zip(idxs, newlocs):
                    locations[i] = DataLocation.from_dict(nl) if nl else None
                    if locations[i] is not None:
                        promoted.add(i)
            except Exception:
                for i in idxs:
                    locations[i] = None
        return promoted

    def _prefetch_async(self, comp_keys: List[str]) -> None:
        """Best-effort background promote so a subsequent get is warm (used by
        exists when the directory reports a hit that may be disk-resident)."""
        def _run():
            try:
                locs = self.runtime.directory.get(comp_keys)
                self._resolve_non_resident(comp_keys, locs)
            except Exception:
                pass
        self._prefetch.submit(_run)

    # ------------------------------------------------------------------ #
    # v2 paths (hybrid models: KV + sidecar pools such as Mamba/SWA/indexer)
    # ------------------------------------------------------------------ #
    def _v2_host_pool(self, name):
        if str(name) in (str(PoolName.KV), "kv"):
            return self.mem_pool_host
        return self.registered_pools.get(name)

    def _v2_component_keys(self, transfer):
        keys = transfer.keys or []
        if str(transfer.name) in (str(PoolName.KV), "kv"):
            return self._component_keys(keys)
        # Extra pools: one storage object per page, tagged by pool + tp suffix.
        suffix = f"_{self.mha_suffix}_{transfer.name}"
        return [f"{k}{suffix}" for k in keys], 1

    def batch_set_v2(self, transfers, extra_info=None) -> dict:
        results: dict = {}
        for t in transfers:
            host_pool = self._v2_host_pool(t.name)
            comp_keys, mult = self._v2_component_keys(t)
            ptrs, sizes = host_pool.get_page_buffer_meta(t.host_indices)
            comp = self._publish(comp_keys, ptrs, sizes)
            results[t.name] = self._page_results(comp, mult)
        return results

    def batch_get_v2(self, transfers, extra_info=None) -> dict:
        results: dict = {}
        for t in transfers:
            host_pool = self._v2_host_pool(t.name)
            comp_keys, mult = self._v2_component_keys(t)
            ptrs, sizes = host_pool.get_page_buffer_meta(t.host_indices)
            comp = self._fetch(comp_keys, ptrs, sizes)
            results[t.name] = self._page_results(comp, mult)
        return results

    def batch_exists_v2(self, keys, pool_transfers=None, extra_info=None):
        from sglang.srt.mem_cache.hicache_storage import (  # lazy import
            PoolHitPolicy,
            PoolTransferResult,
        )

        kv_pages = self.batch_exists(keys, extra_info)
        hit_count = {PoolName.KV: kv_pages} if kv_pages else {}
        final_pages = kv_pages

        for transfer in pool_transfers or []:
            if final_pages == 0:
                break
            comp_keys, mult = self._v2_component_keys(transfer)
            ex = self.runtime.directory.exists(comp_keys)
            page_exists = [
                all(ex[i * mult : (i + 1) * mult]) for i in range(kv_pages)
            ]
            boundary = 0
            if transfer.hit_policy == PoolHitPolicy.ALL_PAGES:
                boundary = next(
                    (i for i in range(kv_pages) if not page_exists[i]), kv_pages
                )
            else:  # trailing pages
                trailing = max(1, len(transfer.keys) if transfer.keys else 1)
                for prefix_len in range(kv_pages, 0, -1):
                    if all(
                        page_exists[i]
                        for i in range(max(0, prefix_len - trailing), prefix_len)
                    ):
                        boundary = prefix_len
                        break
            if boundary:
                hit_count[transfer.name] = boundary
            final_pages = min(final_pages, boundary)

        return PoolTransferResult(final_pages, hit_count)

    # ------------------------------------------------------------------ #
    # Abstract single-key / batch (zero-copy ptr+size) API
    # ------------------------------------------------------------------ #
    def set(self, key, value=None, target_location=None, target_sizes=None) -> bool:
        assert target_location is not None and target_sizes is not None
        return self._publish([key], [target_location], [target_sizes])[0]

    def batch_set(self, keys, values=None, target_locations=None, target_sizes=None) -> bool:
        assert target_locations is not None and target_sizes is not None
        return all(self._publish(list(keys), list(target_locations), list(target_sizes)))

    def get(self, key, target_location=None, target_sizes=None):
        assert target_location is not None and target_sizes is not None
        ok = self._fetch([key], [target_location], [target_sizes])[0]
        return target_location if ok else None

    def batch_get(self, keys, target_locations=None, target_sizes=None) -> int:
        assert target_locations is not None and target_sizes is not None
        oks = self._fetch(list(keys), list(target_locations), list(target_sizes))
        for i, ok in enumerate(oks):
            if not ok:
                return i
        return len(keys)

    def exists(self, key) -> bool:
        return self.runtime.directory.exists([key])[0]

    def clear(self) -> None:
        with self._key_len_lock:
            keys = set(self._key_len.keys())
        if self._pool is not None:
            keys.update(self._pool._entries.keys())  # snapshot
        keys = list(keys)
        if keys:
            self.runtime.directory.delete(keys)
        for k in keys:
            if self._pool is not None:
                self._pool.remove(k)
            if self._disk is not None:
                self._disk.remove(k)
        with self._key_len_lock:
            self._key_len.clear()

    def close(self) -> None:
        # Idempotent: safe to call from both an explicit shutdown and atexit.
        if getattr(self, "_closed", False):
            return
        self._closed = True
        try:
            self._prefetch.shutdown(wait=False)
        except Exception:
            pass
        if self._metrics_server is not None:
            self._metrics_server.stop()
        if self._disk is not None:
            self._disk.close()
        self._data_rpc.close()
        self.runtime.stop()
