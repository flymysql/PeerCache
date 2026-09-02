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
from peercache.slotmap import (
    SlotGeometry,
    SlotRegion,
    slot_matches,
    slot_present,
    pick_way_from_bucket,
    encode_header,
    key_hash128,
    HEADER_SIZE,
)
from peercache.types import DataLocation

logger = logging.getLogger(__name__)

# SGLang is optional at import time so the package can be tested standalone.
# Import the *base class* on its own and as robustly as possible: SGLang's
# dynamic backend loader rejects a store that doesn't `issubclass(HiCacheStorage)`,
# so PeerCacheStore must inherit the REAL class whenever SGLang is importable.
# The other names (HiCacheStorageConfig / HiCacheStorageExtraInfo / PoolName) are
# optional and vary across SGLang versions, so they are imported separately with
# graceful fallbacks -- a missing optional name must NOT drop us to the stand-in
# base class (which previously broke the subclass check on newer SGLang).
try:
    from sglang.srt.mem_cache.hicache_storage import HiCacheStorage  # type: ignore

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


try:
    from sglang.srt.mem_cache.hicache_storage import HiCacheStorageConfig  # type: ignore
except Exception:
    HiCacheStorageConfig = Any  # type: ignore

try:
    from sglang.srt.mem_cache.hicache_storage import HiCacheStorageExtraInfo  # type: ignore
except Exception:
    HiCacheStorageExtraInfo = Any  # type: ignore

try:
    from sglang.srt.mem_cache.hicache_storage import PoolName  # type: ignore
except Exception:
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
        # `extra` is normally a dict of extra config. Some sglang builtin
        # backend paths call `backend_class(storage_config, mem_pool_host)`
        # positionally (like mooncake); tolerate a non-dict there so the
        # builtin registration works unchanged. The mem pool host is picked up
        # later via register_mem_pool_host() anyway.
        if extra is not None:
            if isinstance(extra, dict):
                extra_config.update(extra)
            else:
                logger.debug(
                    "peercache: ignoring non-dict extra arg (%s); "
                    "host pool arrives via register_mem_pool_host()",
                    type(extra).__name__,
                )

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
        # --- slotmap (mode=slotmap) state -----------------------------------
        # Owner-side slot region (this node's fixed slot MR) + its geometry, and
        # a cache of peer layouts (node_id -> {base_addr, rkeys, rail_endpoints})
        # so a reader can compute a peer's slot address with no RPC per key.
        self._slot_region: Optional[SlotRegion] = None
        self._slot_geom: Optional[SlotGeometry] = None
        self._slot_keepalive = None
        self._slot_mr = None
        self._peer_layouts: Dict[str, dict] = {}
        self._peer_layouts_lock = threading.Lock()
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

        # One-shot exists->get handoff cache. SGLang's HiCache prefetch resolves
        # a prefix in two steps: batch_exists() to find the hit length, then
        # batch_get() to pull those pages -- two directory RPCs for the *same*
        # keys. batch_exists() primes the resident hit locations here; the
        # imminent batch_get() consumes (pops) them, skipping the second RPC.
        # Entries are popped on use and short-TTL, so a consumed location is only
        # ever as stale as the exists->get gap (sub-millisecond in practice) --
        # unlike directory_read_cache_ttl this is always on and never serves a
        # location older than one prefetch handoff.
        self._primed: Dict[str, tuple] = {}
        self._primed_lock = threading.Lock()
        self._primed_ttl = 1.0  # un-consumed primes expire (seconds)
        self._primed_cap = 1 << 16  # sweep expired entries past this size

        # Storage keyspace. The zero-copy v1/v2 paths split each page into
        # suffixed K/V component keys; the generic value/pointer batch_set/
        # batch_get store one blob per *raw* key. batch_exists()/exists() must
        # probe whichever namespace the producer actually wrote, otherwise the
        # lookup always misses (exists_pages_found stays 0 while data is being
        # written). The generic set/get path flips this to raw; a read-only node
        # self-heals by probing the other namespace once on a full miss.
        self._raw_keys = False
        self._keyspace_detected = False

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
        # slotmap: peers fetch this node's slot-region layout (base+rkeys+geom)
        # once so they can compute slot addresses locally, directory-free.
        self.runtime.control_rpc.register("slot_layout", self._on_slot_layout)

        # Re-shard the directory when ring membership changes (P2P producers and
        # centralized storage nodes only; inference clients have no local pool).
        if not self.config.is_inference_client_only():
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

    def _register_recv(self, host_pool) -> None:
        """Register a host pool's KV buffer as a receive MR (READ destination).

        The buffer may be host or GPU memory (GPUDirect); a registration failure
        here usually means GPUDirect isn't available on the host.

        A pool with no backing ``kv_buffer`` is a *logical anchor* (DeepSeek-V4
        hybrid: its physical tensors are registered pool-by-pool via
        ``register_mem_host_pool_v2``). Recv-MR registration is skipped there —
        read destinations are lazily registered by the C++ data plane per
        batch_get, so this is not a functional gap.
        """
        kv = getattr(host_pool, "kv_buffer", None)
        if kv is None:
            logger.debug(
                "peercache: host pool has no backing kv_buffer (logical anchor); "
                "recv MR skipped — read destinations are lazily registered"
            )
            return
        kv_bytes = kv.numel() * kv.element_size()
        self._recv_mr = self._register_buffer(kv.data_ptr(), kv_bytes, kv)
        logger.info("PeerCacheStore registered recv MR: %d bytes", kv_bytes)

    def _ensure_published_pool(self) -> None:
        """Create the backend-owned published pool (source of remote READs) once.

        Shared by the v1 (register_mem_pool_host) and v2 (register_mem_host_pool_v2)
        registration paths so PeerCache can publish regardless of which one SGLang
        calls. Idempotent.

        In centralized mode inference nodes are clients only — KV bytes live on
        storage servers, so no local published pool is created."""
        if self.config.is_inference_client_only():
            return
        if self.config.is_slotmap():
            self._ensure_slot_region()
            return
        if self._pool is not None:
            return
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
            "PeerCacheStore published pool ready: %d bytes across %d rail(s)",
            capacity, len(self._rail_endpoints),
        )

    # ------------------------------------------------------------------ #
    # slotmap: deterministic slot region + directory-free read/write
    # ------------------------------------------------------------------ #
    def _ensure_slot_region(self) -> None:
        """Create this node's fixed slot MR (source of remote READs) once.

        Layout: num_buckets x ways x slot_stride, one size class. Registered as
        a single RDMA MR; peers compute a key's address purely by hashing, so no
        directory / lookup RPC is ever needed on the read path."""
        if self._slot_region is not None:
            return
        max_payload = int(self.config.slot_max_page_bytes)
        ways = max(1, int(self.config.slot_ways))
        stride = SlotGeometry(max_payload, 1, ways).slot_stride
        num_buckets = int(self.config.slot_num_buckets)
        if num_buckets <= 0:
            cap = max(1, self.config.global_segment_size // self.tp_size)
            num_buckets = max(1, cap // (stride * ways))
        geom = SlotGeometry(max_payload, num_buckets, ways)
        self._slot_keepalive, base_addr = _alloc_host_buffer(geom.capacity)
        self._slot_mr = self.runtime.transport.register_mr(base_addr, geom.capacity)
        self._slot_geom = geom
        self._slot_region = SlotRegion(base_addr, geom)
        self._rail_endpoints = list(self.runtime.transport.local_endpoints())
        logger.info(
            "PeerCacheStore slot region ready: %d buckets x %d ways x %d B "
            "(=%d bytes) across %d rail(s)",
            geom.num_buckets, geom.ways, geom.slot_stride, geom.capacity,
            len(self._rail_endpoints),
        )

    def _on_slot_layout(self, args: dict) -> dict:
        """RPC: advertise this node's slot region so peers can address it."""
        if self._slot_region is None or self._slot_geom is None:
            return {"ok": False}
        g = self._slot_geom
        return {
            "ok": True,
            "base_addr": self._slot_region.base_addr,
            "rkeys": list(self._slot_mr.rkeys),
            "rail_endpoints": list(self._rail_endpoints),
            "max_payload": g.max_payload,
            "num_buckets": g.num_buckets,
            "ways": g.ways,
            "slot_stride": g.slot_stride,
        }

    def _peer_layout(self, node_id: str) -> Optional[dict]:
        """Return a peer's cached slot layout, fetching it once via RPC.

        The layout (base address, rkeys, geometry) is fixed for a node's
        lifetime, so it is cached; a reader then computes any key's slot address
        with zero per-key metadata traffic."""
        if node_id == self.config.node_id and self._slot_geom is not None:
            with self._peer_layouts_lock:
                lay = self._peer_layouts.get(node_id)
            if lay is None:
                lay = self._on_slot_layout({})
                lay["geom"] = self._slot_geom
                lay["region"] = self._slot_region
                with self._peer_layouts_lock:
                    self._peer_layouts[node_id] = lay
            return lay
        with self._peer_layouts_lock:
            lay = self._peer_layouts.get(node_id)
        if lay is not None:
            return lay
        endpoint = self.runtime.discovery.control_of(node_id)
        if endpoint is None:
            return None
        try:
            resp = self._data_rpc.call(endpoint, "slot_layout", {})
        except Exception as e:
            logger.debug("peercache: slot_layout RPC to %s failed: %s", node_id, e)
            return None
        if not resp.get("ok"):
            return None
        resp["geom"] = SlotGeometry(
            resp["max_payload"], resp["num_buckets"], resp["ways"]
        )
        with self._peer_layouts_lock:
            self._peer_layouts[node_id] = resp
        return resp

    def _invalidate_peer_layouts(self) -> None:
        """Drop cached peer layouts after a membership change (nodes may have
        left; their base/rkey become invalid)."""
        with self._peer_layouts_lock:
            self._peer_layouts.clear()

    def _publish_slotmap(
        self, comp_keys: List[str], ptrs: List[int], sizes: List[int]
    ) -> List[bool]:
        """Directory-free write: each key -> owner node -> N-way bucket slot.

        Own-keys are written locally (memmove + seqlock). Remote keys use a
        read-modify-write: one batched RDMA READ of the target buckets' bytes
        picks a way per key (same policy as the local writer: overwrite same key
        -> empty way -> oldest seq), then one batched RDMA WRITE lands the slot.
        No directory PUT, no reservation RPC."""
        t0 = time.perf_counter()
        results = [False] * len(comp_keys)
        published_bytes = 0

        # 1) Local (own-key) writes + collect remote keys grouped by owner.
        remote: List[tuple] = []  # (idx, key, owner, length, geom, lay)
        for i, key in enumerate(comp_keys):
            owner = self.runtime.data_owner_all(key)
            if owner is None:
                continue
            length = int(sizes[i])
            if owner == self.config.node_id:
                if self._slot_region is not None:
                    off = self._slot_region.write_local(key, ptrs[i], length)
                    if off is not None:
                        results[i] = True
                        published_bytes += length
                continue
            lay = self._peer_layout(owner)
            if lay is None or length > lay["geom"].max_payload:
                continue
            remote.append((i, key, owner, length, lay["geom"], lay))

        # 2) Remote READ of each key's bucket to choose a way (RMW).
        published_bytes += self._publish_slotmap_remote(remote, ptrs, results)

        self._metrics.record_write(published_bytes, time.perf_counter() - t0)
        return results

    def _publish_slotmap_remote(self, remote, ptrs, results) -> int:
        """Read-modify-write the remote slots. Returns bytes published."""
        if not remote:
            return 0
        # --- Phase A: batched READ of each target bucket. ---
        rd_nodes: List[str] = []
        rd_local: List[int] = []
        rd_remote: List[int] = []
        rd_len: List[int] = []
        scratches: List[Any] = []
        rail_eps: Dict[str, List[str]] = {}
        rail_rks: Dict[str, List[int]] = {}
        for (_i, key, _owner, _length, geom, lay) in remote:
            bucket = geom.bucket_index(key)
            scratch = (ctypes.c_char * geom.bucket_stride)()
            scratches.append(scratch)
            nk = lay["rail_endpoints"][0]
            if nk not in rail_eps:
                rail_eps[nk] = list(lay["rail_endpoints"])
                rail_rks[nk] = [int(x) for x in lay["rkeys"]]
            rd_nodes.append(nk)
            rd_local.append(ctypes.addressof(scratch))
            rd_remote.append(lay["base_addr"] + bucket * geom.bucket_stride)
            rd_len.append(geom.bucket_stride)
        try:
            rd_ok = self.runtime.transport.batch_read_multi(
                rd_nodes, rd_local, rd_remote, rd_len, rail_eps, rail_rks
            )
        except Exception as e:
            logger.debug("peercache: slotmap RMW read failed: %s", e)
            rd_ok = [False] * len(rd_nodes)

        # --- Phase B: pick a way from each bucket, build & batch WRITE. ---
        w_nodes: List[str] = []
        w_local: List[int] = []
        w_remote: List[int] = []
        w_len: List[int] = []
        w_idx: List[int] = []
        keepalive: list = []
        w_rail_eps: Dict[str, List[str]] = {}
        w_rail_rks: Dict[str, List[int]] = {}
        # Ways already claimed by earlier keys of THIS batch, per (owner,bucket).
        # Without this, several same-bucket keys in one batch all read the same
        # pre-write snapshot, pick the same empty way, and overwrite each other.
        claimed: Dict[tuple, dict] = {}
        for j, (i, key, owner, length, geom, lay) in enumerate(remote):
            bucket = geom.bucket_index(key)
            snap = bytearray(scratches[j]) if rd_ok[j] else bytearray(geom.bucket_stride)
            # Fold in this batch's earlier picks for the same bucket so the way
            # chooser sees them as occupied.
            ck = (owner, bucket)
            for w, hdr in claimed.get(ck, {}).items():
                off = w * geom.slot_stride
                snap[off:off + HEADER_SIZE] = hdr
            way, new_seq = pick_way_from_bucket(bytes(snap), geom, key)
            slot_off = geom.slot_offset(bucket, way)
            hdr = encode_header(key, length, new_seq)
            claimed.setdefault(ck, {})[way] = hdr
            buf = self._pack_slot(key, ptrs[i], length, new_seq, keepalive)
            nk = lay["rail_endpoints"][0]
            if nk not in w_rail_eps:
                w_rail_eps[nk] = list(lay["rail_endpoints"])
                w_rail_rks[nk] = [int(x) for x in lay["rkeys"]]
            w_nodes.append(nk)
            w_local.append(ctypes.addressof(buf))
            w_remote.append(lay["base_addr"] + slot_off)
            w_len.append(HEADER_SIZE + length)
            w_idx.append(i)
        published_bytes = 0
        if w_nodes:
            try:
                oks = self.runtime.transport.batch_write_multi(
                    w_nodes, w_local, w_remote, w_len, w_rail_eps, w_rail_rks
                )
            except Exception as e:
                logger.debug("peercache: slotmap batch_write failed: %s", e)
                oks = [False] * len(w_nodes)
            for j, ok in enumerate(oks):
                if ok:
                    results[w_idx[j]] = True
                    published_bytes += w_len[j] - HEADER_SIZE
        return published_bytes

    def _pack_slot(self, key: str, src_ptr: int, length: int, seq: int, keepalive: list):
        """Build a [header|payload] buffer for one remote RDMA WRITE.

        ``seq`` is an even (stable) sequence number strictly fresher than what
        the slot held; a single WRITE lands the whole slot, and the reader's
        key_hash/len/seq gate rejects anything that is not this exact page."""
        total = HEADER_SIZE + length
        buf = (ctypes.c_char * total)()
        hdr = encode_header(key, length, seq)
        ctypes.memmove(ctypes.addressof(buf), hdr, HEADER_SIZE)
        ctypes.memmove(ctypes.addressof(buf) + HEADER_SIZE, src_ptr, length)
        keepalive.append(buf)
        return buf

    def _fetch_slotmap(
        self, comp_keys: List[str], ptrs: List[int], sizes: List[int]
    ) -> List[bool]:
        """Directory-free read: compute each key's owner+slot, READ its whole
        bucket in one shot, validate the header locally, memmove on a hit.

        A read is a single one-sided RDMA READ per key (the bucket) with NO
        metadata lookup. A slot that fails validation (wrong key / torn / empty)
        is a clean miss -- never a dirty hit."""
        t0 = time.perf_counter()
        results = [False] * len(comp_keys)
        # Stage a scratch buffer per remote read (whole bucket), then validate.
        r_nodes: List[str] = []
        r_local: List[int] = []
        r_remote: List[int] = []
        r_len: List[int] = []
        r_meta: List[tuple] = []  # (idx, bucket_buf, geom, dst_ptr, exp_len, key)
        rail_eps: Dict[str, List[str]] = {}
        rail_rks: Dict[str, List[int]] = {}
        keepalive: list = []
        local_hits = 0
        for i, key in enumerate(comp_keys):
            owner = self.runtime.data_owner_all(key)
            if owner is None:
                continue
            length = int(sizes[i])
            if owner == self.config.node_id:
                if self._slot_region is not None and self._slot_region.read_local(
                    key, ptrs[i], length
                ):
                    results[i] = True
                    local_hits += 1
                continue
            lay = self._peer_layout(owner)
            if lay is None or length > lay["geom"].max_payload:
                continue
            geom = lay["geom"]
            bucket = geom.bucket_index(key)
            bucket_off = bucket * geom.bucket_stride
            bucket_bytes = geom.bucket_stride  # ways x slot_stride (one bucket)
            scratch = (ctypes.c_char * bucket_bytes)()
            keepalive.append(scratch)
            nk = lay["rail_endpoints"][0]
            if nk not in rail_eps:
                rail_eps[nk] = list(lay["rail_endpoints"])
                rail_rks[nk] = [int(x) for x in lay["rkeys"]]
            r_nodes.append(nk)
            r_local.append(ctypes.addressof(scratch))
            r_remote.append(lay["base_addr"] + bucket_off)
            r_len.append(bucket_bytes)
            r_meta.append((i, scratch, geom, ptrs[i], length, key))
        if r_nodes:
            try:
                oks = self.runtime.transport.batch_read_multi(
                    r_nodes, r_local, r_remote, r_len, rail_eps, rail_rks
                )
            except Exception as e:
                logger.debug("peercache: slotmap batch_read failed: %s", e)
                oks = [False] * len(r_nodes)
            for j, ok in enumerate(oks):
                idx, scratch, geom, dst_ptr, exp_len, key = r_meta[j]
                if not ok:
                    continue
                # Scan the ways in this bucket for a validated match.
                for way in range(geom.ways):
                    off = way * geom.slot_stride
                    hdr = bytes(scratch[off:off + HEADER_SIZE])
                    if slot_matches(hdr, key, exp_len):
                        ctypes.memmove(
                            dst_ptr,
                            ctypes.addressof(scratch) + off + HEADER_SIZE,
                            exp_len,
                        )
                        results[idx] = True
                        break
        latency = time.perf_counter() - t0
        for i in range(len(comp_keys)):
            self._metrics.record_read(
                results[i], sizes[i] if results[i] else 0, latency,
                "local" if results[i] and i < len(comp_keys) else "remote",
            )
        return results


    def _exists_slotmap(self, comp_keys: List[str]) -> List[bool]:
        """Directory-free existence probe: for each key compute owner+bucket,
        READ the whole bucket in one shot, and check the header for a valid
        stable page for this key -- no payload copy, no metadata lookup.

        Length-agnostic (batch_exists carries no sizes): a header whose key hash
        matches with an even seq counts as present. A collision/eviction/torn
        slot is a clean miss, never a dirty hit."""
        present = [False] * len(comp_keys)
        r_local: List[int] = []
        r_nodes: List[str] = []
        r_remote: List[int] = []
        r_len: List[int] = []
        r_meta: List[tuple] = []  # (idx, scratch, geom, key)
        rail_eps: Dict[str, List[str]] = {}
        rail_rks: Dict[str, List[int]] = {}
        keepalive: list = []
        for i, key in enumerate(comp_keys):
            owner = self.runtime.data_owner_all(key)
            if owner is None:
                continue
            if owner == self.config.node_id:
                if self._slot_region is not None:
                    if self._slot_region.exists_local(key) is not None:
                        present[i] = True
                continue
            lay = self._peer_layout(owner)
            if lay is None:
                continue
            geom = lay["geom"]
            bucket_off = geom.bucket_index(key) * geom.bucket_stride
            bucket_bytes = geom.bucket_stride
            scratch = (ctypes.c_char * bucket_bytes)()
            keepalive.append(scratch)
            nk = lay["rail_endpoints"][0]
            if nk not in rail_eps:
                rail_eps[nk] = list(lay["rail_endpoints"])
                rail_rks[nk] = [int(x) for x in lay["rkeys"]]
            r_nodes.append(nk)
            r_local.append(ctypes.addressof(scratch))
            r_remote.append(lay["base_addr"] + bucket_off)
            r_len.append(bucket_bytes)
            r_meta.append((i, scratch, geom, key))
        if r_nodes:
            try:
                oks = self.runtime.transport.batch_read_multi(
                    r_nodes, r_local, r_remote, r_len, rail_eps, rail_rks
                )
            except Exception as e:
                logger.debug("peercache: slotmap exists read failed: %s", e)
                oks = [False] * len(r_nodes)
            for j, ok in enumerate(oks):
                if not ok:
                    continue
                idx, scratch, geom, key = r_meta[j]
                for way in range(geom.ways):
                    off = way * geom.slot_stride
                    hdr = bytes(scratch[off:off + HEADER_SIZE])
                    if slot_present(hdr, key) is not None:
                        present[idx] = True
                        break
        return present

    def register_mem_pool_host(self, mem_pool_host):
        """SGLang v1 registration: the KV host pool.

        For hybrid models (DeepSeek-V4 etc.) sglang may pass the anchor pool of
        a HostPoolGroup, which is a *logical* anchor with no backing
        ``kv_buffer`` (its physical tensors are registered pool-by-pool through
        ``register_mem_host_pool_v2`` — mirror mooncake). We therefore tolerate
        an anchor without a backing buffer: recv-MR registration is skipped
        (read destinations are lazily registered by the C++ data plane per
        batch_get), and the published pool is still created so side-pool
        publishes have somewhere to go.

        If the pool is itself a HostPoolGroup facade (defensive; not how
        current sglang calls), unwrap it and register every side pool by name.
        """
        if getattr(mem_pool_host, "anchor_entry", None) is not None:
            # HostPoolGroup facade (defensive): register every pool by name and
            # use the anchor's host pool as mem_pool_host.
            group = mem_pool_host
            self._host_pool_group = group
            self._registered_pool_names = []
            for entry in group.entries:
                name = str(entry.name)
                pool = entry.host_pool
                if getattr(pool, "kv_buffer", None) is not None:
                    self.registered_pools[name] = pool
                    self._registered_pool_names.append(name)
            anchor_pool = group.anchor_entry.host_pool
            logger.info(
                "peercache: unwrapped HostPoolGroup -> anchor %s (registered %d backing pools: %s)",
                getattr(group.anchor_entry, "name", "?"),
                len(self._registered_pool_names),
                self._registered_pool_names,
            )
            mem_pool_host = anchor_pool
        self.mem_pool_host = mem_pool_host
        self._register_recv(mem_pool_host)
        self._ensure_published_pool()

    def register_mem_host_pool_v2(self, host_pool, host_pool_name):
        """SGLang v2 registration: called once per pool (KV + hybrid sidecars).

        Must do everything v1 does for the KV pool -- set mem_pool_host, register
        the recv MR, and create the published pool -- otherwise PeerCache has no
        pool to publish into and silently does nothing (pool_capacity_bytes=0)."""
        self.registered_pools[str(host_pool_name)] = host_pool
        if str(host_pool_name) in (str(PoolName.KV), "kv"):
            self.mem_pool_host = host_pool
            self._register_recv(host_pool)
        # Extra (hybrid) pools' buffers must also be RDMA-registered so peers can
        # READ them; they share the same published-pool publish path on write.
        for buf in getattr(host_pool, "get_hybrid_pool_buffer", lambda: [])():
            self._register_buffer(buf.data_ptr(), buf.numel() * buf.element_size(), buf)
        self._ensure_published_pool()

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
        m.set_gauge_provider(
            "storage_nodes", lambda: len(self.runtime.storage_nodes())
        )
        # Data-plane (transport) gauges:
        def _tstat(key):
            try:
                return self.runtime.transport.stats().get(key, 0)
            except Exception:
                return 0
        m.set_gauge_provider("rdma_rails", lambda: _tstat("rails"))
        m.set_gauge_provider("rdma_read_timeouts", lambda: _tstat("read_timeouts"))
        m.set_gauge_provider("rdma_channel_discards", lambda: _tstat("channel_discards"))
        # READs that *completed with an error status* (distinct from a timeout):
        # e.g. status 10 = remote access error (bad rkey / MR / out-of-bounds),
        # 13 = retry-exceeded (GID/path/MTU). last_wc_status is the raw
        # ibv_wc_status of the most recent such failure (0 = none).
        m.set_gauge_provider("rdma_read_wc_errors", lambda: _tstat("read_wc_errors"))
        m.set_gauge_provider("rdma_last_wc_status", lambda: _tstat("last_wc_status"))
        # READs that never reached the wire: local_reg_misses = the local READ
        # destination was outside any registered MR (e.g. SGLang handed batch_get
        # a buffer outside the registered host KV pool); post_failures =
        # ibv_post_send rejected the WR; lease_failures = no channel to the peer.
        m.set_gauge_provider("rdma_local_reg_misses", lambda: _tstat("local_reg_misses"))
        m.set_gauge_provider("rdma_post_failures", lambda: _tstat("post_failures"))
        m.set_gauge_provider("rdma_lease_failures", lambda: _tstat("lease_failures"))
        # MRs lazily registered for unregistered local READ destinations (the
        # generic batch_get path); converges once SGLang's host pages are seen.
        m.set_gauge_provider("rdma_lazy_local_mrs", lambda: _tstat("lazy_local_mrs"))
        m.set_gauge_provider("rdma_write_wc_errors", lambda: _tstat("write_wc_errors"))
        m.set_gauge_provider("rdma_write_timeouts", lambda: _tstat("write_timeouts"))

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
        if self.config.is_slotmap():
            # No directory to re-shard; just drop cached peer layouts (a node may
            # have left / a new one joined -> ring ownership moved). Keys whose
            # owner changed become clean misses until re-written -- harmless for
            # a recomputable cache.
            self._invalidate_peer_layouts()
            return
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
    def _tag_keys(self, keys: List[str]) -> List[str]:
        """Apply the optional keyspace prefix (tenant/model isolation).

        Mirrors Mooncake's `_tag_keys`: when `config.prefix` is set, every
        logical key is namespaced as `<prefix>_<key>` before any suffixing, so
        two sglang deployments sharing one PeerCache cluster never collide.
        All nodes of a deployment must configure the same prefix.
        """
        if not self.config.prefix:
            return list(keys)
        p = self.config.prefix
        return [f"{p}_{k}" for k in keys]

    def _component_keys(self, keys: List[str]):
        """Return (component_keys, multiplier) aligned with get_page_buffer_meta."""
        keys = self._tag_keys(keys)
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

    def _lookup_keys(self, keys):
        """(component_keys, multiplier) for the active storage keyspace: raw
        keys for the generic value/pointer path, suffixed for zero-copy v1/v2."""
        if self._raw_keys:
            return list(keys), 1
        return self._component_keys(keys)

    @staticmethod
    def _hit_prefix(locs: List, multiplier: int) -> int:
        """Number of leading *pages* present (stops at the first missing one)."""
        n = len(locs) // multiplier
        for i, loc in enumerate(locs):
            if loc is None:
                return i // multiplier
        return n

    # ------------------------------------------------------------------ #
    # v1 zero-copy paths (primary)
    # ------------------------------------------------------------------ #
    def _v1_buffer_meta(self, keys, host_indices):
        """Resolve host page buffers for a v1 op.

        A logical anchor (DeepSeek-V4 hybrid) has no backing buffers — the
        physical tensors live in v2-registered side pools, so the v1 path is
        not usable for it. Return None instead of crashing the backup/prefetch
        thread with a bare assert.
        """
        pool = self.mem_pool_host
        if pool is None or getattr(pool, "kv_buffer", None) is None:
            logger.debug(
                "peercache: v1 op on pool without backing kv_buffer "
                "(logical anchor); v1 path not usable here"
            )
            return None
        try:
            return pool.get_page_buffer_meta(host_indices)
        except Exception as e:  # noqa: BLE001
            logger.warning("peercache: get_page_buffer_meta failed: %s", e)
            return None

    def batch_set_v1(self, keys, host_indices, extra_info=None) -> List[bool]:
        meta = self._v1_buffer_meta(keys, host_indices)
        if meta is None:
            return [False] * len(keys)
        comp_keys, mult = self._component_keys(keys)
        ptrs, sizes = meta
        if not (len(comp_keys) == len(ptrs) == len(sizes)):
            logger.warning(
                "peercache: v1 set meta mismatch: %d comp keys vs %d buffers",
                len(comp_keys), len(ptrs),
            )
            return [False] * len(keys)
        comp_results = self._publish(comp_keys, ptrs, sizes)
        return self._page_results(comp_results, mult)

    def batch_get_v1(self, keys, host_indices, extra_info=None) -> List[bool]:
        meta = self._v1_buffer_meta(keys, host_indices)
        if meta is None:
            return [False] * len(keys)
        comp_keys, mult = self._component_keys(keys)
        ptrs, sizes = meta
        if not (len(comp_keys) == len(ptrs) == len(sizes)):
            logger.warning(
                "peercache: v1 get meta mismatch: %d comp keys vs %d buffers",
                len(comp_keys), len(ptrs),
            )
            return [False] * len(keys)
        comp_results = self._fetch(comp_keys, ptrs, sizes)
        return self._page_results(comp_results, mult)

    def batch_exists(self, keys, extra_info=None) -> int:
        # Fetch full locations (not just booleans): a non-None entry means
        # present (resident in a pool OR spilled to disk -- same semantics as
        # directory.exists), and resident hits are primed so the imminent
        # batch_get reuses them instead of issuing a second directory RPC.
        comp_keys, mult = self._lookup_keys(keys)
        if self.config.is_slotmap():
            # Directory-free existence probe: one bucket READ per key, header
            # validated locally. SGLang wants the contiguous hit prefix length
            # (in original-key units), so count until the first miss.
            present = self._exists_slotmap(comp_keys)
            n = 0
            for k in range(len(keys)):
                block = present[k * mult:(k + 1) * mult]
                if block and all(block):
                    n += 1
                else:
                    break
            self._metrics.inc("exists_requests")
            if n:
                self._metrics.inc("exists_pages_found", n)
                self._keyspace_detected = True
            return n
        locs = self._dir_get(comp_keys)
        n = self._hit_prefix(locs, mult)
        if n:
            self._keyspace_detected = True
        elif not self._keyspace_detected:
            # A read-only node has not yet observed which keyspace the producer
            # writes (raw vs suffixed). On a full miss, probe the other namespace
            # once; if it hits, lock onto it so future lookups match the writer.
            alt_raw = not self._raw_keys
            alt_keys, alt_mult = (
                (list(keys), 1) if alt_raw else self._component_keys(keys)
            )
            alt_locs = self._dir_get(alt_keys)
            alt_n = self._hit_prefix(alt_locs, alt_mult)
            if alt_n:
                self._raw_keys = alt_raw
                self._keyspace_detected = True
                comp_keys, mult, locs, n = alt_keys, alt_mult, alt_locs, alt_n
        # Surface that SGLang is probing L3 and how many pages we report present.
        # exists_requests>0 with read_requests==0 means SGLang found pages but
        # did not fetch them (local hit / prefetch disabled); exists_requests==0
        # means the prefetch never reached the storage backend at all.
        self._metrics.inc("exists_requests")
        if n:
            self._metrics.inc("exists_pages_found", n)
        hit = n * mult
        if hit:
            self._prime(comp_keys[:hit], locs[:hit])
        if n and self._disk is not None:
            # Warm the hit prefix back into the pool for the imminent get.
            self._prefetch_async(comp_keys[:hit])
        return n

    # ------------------------------------------------------------------ #
    # Core publish / fetch over component objects
    # ------------------------------------------------------------------ #
    def _publish(self, comp_keys: List[str], ptrs: List[int], sizes: List[int]) -> List[bool]:
        mode = self.config.mode
        if mode == "slotmap":
            return self._publish_slotmap(comp_keys, ptrs, sizes)
        if mode == "p2p":
            return self._publish_p2p(comp_keys, ptrs, sizes)
        if mode == "centralized":
            return self._publish_storage(comp_keys, ptrs, sizes)
        # hybrid: write_policy selects local / storage / both
        policy = self.config.write_policy
        has_storage = bool(self.runtime.storage_nodes())
        if policy == "local" or not has_storage:
            if policy in ("storage", "both") and not has_storage:
                logger.warning(
                    "peercache: hybrid write_policy=%s but no storage nodes registered; "
                    "falling back to local publish", policy,
                )
            return self._publish_p2p(comp_keys, ptrs, sizes)
        if policy == "storage":
            return self._publish_storage(comp_keys, ptrs, sizes)
        # both: storage tier for cross-node sharing + local copy (no extra dir PUT)
        storage_res = self._publish_storage(comp_keys, ptrs, sizes)
        local_res = self._publish_p2p(
            comp_keys, ptrs, sizes, update_directory=False
        )
        return [bool(s or l) for s, l in zip(storage_res, local_res)]

    def _publish_storage(
        self, comp_keys: List[str], ptrs: List[int], sizes: List[int]
    ) -> List[bool]:
        """Push pages to a storage node via RDMA WRITE (zero-copy) or RPC fallback."""
        t0 = time.perf_counter()
        if not self.runtime.storage_nodes():
            logger.warning("peercache: storage writes requested but no storage nodes registered")
            return [False] * len(comp_keys)

        existing = self.runtime.directory.exists(comp_keys)
        results = [False] * len(comp_keys)
        key_to_idx = {k: i for i, k in enumerate(comp_keys)}

        # Group pending pages by target storage owner.
        pending: Dict[str, List[dict]] = {}
        for i, key in enumerate(comp_keys):
            if existing[i]:
                results[i] = True
                continue
            owner = self.runtime.data_owner(key)
            if owner is None:
                continue
            pending.setdefault(owner, []).append(
                {"key": key, "length": sizes[i], "local_ptr": ptrs[i], "idx": i}
            )

        published_bytes = 0
        transport = self.runtime.transport

        for owner, pages in pending.items():
            endpoint = self.runtime.discovery.control_of(owner)
            if endpoint is None:
                continue
            try:
                prep = self._data_rpc.call(
                    endpoint,
                    "data_prepare_writes",
                    {"pages": [{"key": p["key"], "length": p["length"]} for p in pages]},
                )
                slots = prep.get("slots") or []
            except Exception as e:
                logger.debug("peercache: data_prepare_writes to %s failed: %s", owner, e)
                continue

            # RDMA WRITE (or TCP write fallback) for each reserved slot.
            w_nodes: List[str] = []
            w_local: List[int] = []
            w_remote: List[int] = []
            w_len: List[int] = []
            w_keys: List[str] = []
            w_idx: List[int] = []
            rail_eps: Dict[str, List[str]] = {}
            rail_rks: Dict[str, List[int]] = {}

            for page, slot in zip(pages, slots):
                if not slot:
                    continue
                nk = slot["rdma_endpoint"]
                if nk not in rail_eps:
                    rail_eps[nk] = list(slot.get("rail_endpoints") or [nk])
                    rail_rks[nk] = [int(x) for x in (slot.get("rail_rkeys") or [slot["rkey"]])]
                w_nodes.append(nk)
                w_local.append(page["local_ptr"])
                w_remote.append(int(slot["remote_addr"]))
                w_len.append(int(slot["length"]))
                w_keys.append(page["key"])
                w_idx.append(page["idx"])

            write_ok: Dict[str, bool] = {}
            if w_keys:
                try:
                    oks = transport.batch_write_multi(
                        w_nodes, w_local, w_remote, w_len, rail_eps, rail_rks
                    )
                except Exception as e:
                    logger.debug("peercache: batch_write_multi to %s failed: %s", owner, e)
                    oks = [False] * len(w_keys)
                commit_keys = [k for k, ok in zip(w_keys, oks) if ok]
                for k, ok in zip(w_keys, oks):
                    write_ok[k] = bool(ok)
                if commit_keys:
                    try:
                        resp = self._data_rpc.call(
                            endpoint, "data_commit_writes", {"keys": commit_keys}
                        )
                        committed = {
                            k: v
                            for k, v in zip(commit_keys, resp.get("ok") or [])
                            if v
                        }
                    except Exception as e:
                        logger.debug("peercache: data_commit_writes failed: %s", e)
                        committed = {}
                else:
                    committed = {}
            else:
                committed = {}

            for key, ok in write_ok.items():
                if not ok or key not in committed:
                    continue
                idx = key_to_idx.get(key)
                if idx is not None:
                    results[idx] = True
                    published_bytes += sizes[idx]

            # RPC copy fallback for pages the data-plane write could not commit.
            fallback = [
                p for p in pages
                if not results[p["idx"]]
            ]
            if fallback:
                ingest_pages = [{
                    "key": p["key"],
                    "data": ctypes.string_at(p["local_ptr"], p["length"]),
                } for p in fallback]
                try:
                    resp = self._data_rpc.call(
                        endpoint, "data_ingest", {"pages": ingest_pages}
                    )
                    for p, ok in zip(fallback, resp.get("ok") or []):
                        if ok:
                            results[p["idx"]] = True
                            published_bytes += p["length"]
                except Exception as e:
                    logger.debug("peercache: data_ingest fallback failed: %s", e)

        self._metrics.record_write(published_bytes, time.perf_counter() - t0)
        return results

    def _publish_p2p(
        self,
        comp_keys: List[str],
        ptrs: List[int],
        sizes: List[int],
        *,
        update_directory: bool = True,
    ) -> List[bool]:
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
        if entries and update_directory:
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

    def _prime(self, comp_keys: List[str], locs: List[Optional[DataLocation]]) -> None:
        """Stash resident locations resolved by batch_exists so the following
        batch_get for the same keys can skip a second directory lookup."""
        now = time.monotonic()
        exp = now + self._primed_ttl
        with self._primed_lock:
            if len(self._primed) > self._primed_cap:
                self._primed = {k: v for k, v in self._primed.items() if v[1] > now}
            for k, loc in zip(comp_keys, locs):
                if loc is not None and loc.resident:
                    self._primed[k] = (loc, exp)

    def _take_primed(self, comp_keys: List[str]) -> List[Optional[DataLocation]]:
        """Resolve fetch locations, consuming any primed by a preceding
        batch_exists (one-shot). Falls back to the directory for the rest."""
        out: List[Optional[DataLocation]] = [None] * len(comp_keys)
        miss_keys: List[str] = []
        miss_idx: List[int] = []
        now = time.monotonic()
        saved = 0
        with self._primed_lock:
            for i, k in enumerate(comp_keys):
                ent = self._primed.pop(k, None)
                if ent is not None and ent[1] > now:
                    out[i] = ent[0]
                    saved += 1
                else:
                    miss_keys.append(k)
                    miss_idx.append(i)
        if saved:
            self._metrics.inc("directory_lookups_saved", saved)
        if miss_keys:
            fresh = self._dir_get(miss_keys)
            for j, loc in enumerate(fresh):
                out[miss_idx[j]] = loc
        return out

    def _fetch(self, comp_keys: List[str], ptrs: List[int], sizes: List[int]) -> List[bool]:
        if self.config.is_slotmap():
            return self._fetch_slotmap(comp_keys, ptrs, sizes)
        t0 = time.perf_counter()
        locations = self._take_primed(comp_keys)
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
            # Hybrid dual-write: prefer the local pool copy before READing storage.
            if (
                self.config.is_hybrid()
                and self.config.write_policy == "both"
                and self._pool is not None
            ):
                al = self._pool.address_of(comp_keys[i])
                if al is not None and al[1] == sizes[i]:
                    ctypes.memmove(ptrs[i], al[0], al[1])
                    results[i] = True
                    sources[i] = "local"
                    continue
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
        return self.registered_pools.get(str(name))

    def _v2_component_keys(self, transfer):
        keys = transfer.keys or []
        if str(transfer.name) in (str(PoolName.KV), "kv"):
            return self._component_keys(keys)
        keys = self._tag_keys(keys)
        # Extra pools: one storage object per page, tagged by pool + tp suffix.
        # Special-case DRAFT: the draft model's MLA/MHA layout is independent
        # from the target (e.g. EAGLE-MHA draft on an MLA target), so the
        # suffix scheme follows the draft pool's own class (mirrors mooncake):
        #   MLA draft      -> single component  _{rank}_draft_k
        #   MHA draft      -> K/V components    _{rank}_draft_k + _{rank}_draft_v
        # Other sidecar pools (MAMBA/SWA/INDEXER/DeepSeek-V4) are single
        # page-packed objects: _{rank}_{pool}.
        name = str(transfer.name)
        if name in ("draft", "draft_mla") or name == str(getattr(PoolName, "DRAFT", "draft")):
            draft_pool = self.registered_pools.get(name) or self.registered_pools.get(
                "draft"
            )
            is_mla_draft = draft_pool is not None and not hasattr(
                draft_pool, "v_buffer"
            )
            if is_mla_draft:
                suffix = f"_{self.mla_suffix}_draft_k"
                return [f"{k}{suffix}" for k in keys], 1
            suffix_k = f"_{self.mha_suffix}_draft_k"
            suffix_v = f"_{self.mha_suffix}_draft_v"
            out = []
            for k in keys:
                out.append(f"{k}{suffix_k}")
                out.append(f"{k}{suffix_v}")
            return out, 2
        suffix = f"_{self.mha_suffix}_{name}"
        return [f"{k}{suffix}" for k in keys], 1

    def _pack_multi_buffer(
        self, comp_keys, ptrs, sizes, keepalive
    ):
        """DeepSeek-V4 style pools: one logical page = N host buffers.

        mooncake packs them with `_pack_multi_buffer_meta`; PeerCache stores
        one blob per key, so the N buffers of a page are concatenated into a
        single published blob (read back then scatter into the buffers).
        Returns (packed_ptrs, packed_sizes) with one entry per comp_key.
        """
        if len(ptrs) == len(comp_keys):
            return ptrs, sizes
        assert len(ptrs) % len(comp_keys) == 0, (
            "multi-buffer meta mismatch: %d buffers for %d keys"
            % (len(ptrs), len(comp_keys))
        )
        nbuf = len(ptrs) // len(comp_keys)
        packed_ptrs, packed_sizes = [], []
        for i in range(len(comp_keys)):
            chunk = ptrs[i * nbuf:(i + 1) * nbuf]
            chunk_sizes = sizes[i * nbuf:(i + 1) * nbuf]
            total = sum(chunk_sizes)
            buf = (ctypes.c_byte * total)()
            off = 0
            for p, sz in zip(chunk, chunk_sizes):
                ctypes.memmove(ctypes.addressof(buf) + off, p, sz)
                off += sz
            keepalive.append(buf)
            packed_ptrs.append(ctypes.addressof(buf))
            packed_sizes.append(total)
        return packed_ptrs, packed_sizes

    def _scatter_multi_buffer(
        self, comp_keys, ptrs, sizes, keepalive, results
    ):
        """Inverse of _pack_multi_buffer: copy a fetched blob back into the
        per-page host buffers after a multi-buffer get. `keepalive` holds the
        packed blobs (in comp_keys order) that `_fetch` wrote into."""
        if len(ptrs) == len(comp_keys):
            return
        nbuf = len(ptrs) // len(comp_keys)
        for i, key in enumerate(comp_keys):
            if not results[i]:
                continue
            if i >= len(keepalive):
                results[i] = False
                continue
            src = keepalive[i]
            chunk = ptrs[i * nbuf:(i + 1) * nbuf]
            chunk_sizes = sizes[i * nbuf:(i + 1) * nbuf]
            off = 0
            for p, sz in zip(chunk, chunk_sizes):
                ctypes.memmove(p, ctypes.addressof(src) + off, sz)
                off += sz

    def batch_set_v2(self, transfers, extra_info=None) -> dict:
        results: dict = {}
        for t in transfers:
            host_pool = self._v2_host_pool(t.name)
            comp_keys, mult = self._v2_component_keys(t)
            ptrs, sizes = host_pool.get_page_buffer_meta(t.host_indices)
            keepalive: list = []
            p_ptrs, p_sizes = self._pack_multi_buffer(comp_keys, ptrs, sizes, keepalive)
            comp = self._publish(comp_keys, p_ptrs, p_sizes)
            results[t.name] = self._page_results(comp, mult)
        return results

    def batch_get_v2(self, transfers, extra_info=None) -> dict:
        results: dict = {}
        for t in transfers:
            host_pool = self._v2_host_pool(t.name)
            comp_keys, mult = self._v2_component_keys(t)
            ptrs, sizes = host_pool.get_page_buffer_meta(t.host_indices)
            keepalive: list = []
            p_ptrs, p_sizes = self._pack_multi_buffer(comp_keys, ptrs, sizes, keepalive)
            comp = self._fetch(comp_keys, p_ptrs, p_sizes)
            # Scatter fetched blobs back into the per-page host buffers.
            self._scatter_multi_buffer(comp_keys, ptrs, sizes, keepalive, comp)
            results[t.name] = self._page_results(comp, mult)
        return results

    def batch_exists_v2(self, keys, pool_transfers=None, extra_info=None):
        # Lazy import with graceful fallback: sglang releases differ in whether
        # they export PoolHitPolicy / PoolTransferResult (0.5.9 predates the v2
        # interface; main adds it). PeerCache must work against both.
        try:
            from sglang.srt.mem_cache.hicache_storage import (
                PoolHitPolicy,
                PoolTransferResult,
            )
        except Exception:
            class _PoolHitPolicy:
                ALL_PAGES = "all_pages"
                TRAILING_PAGES = "trailing_pages"
            PoolHitPolicy = _PoolHitPolicy

            class PoolTransferResult:
                def __init__(self, kv_hit_pages, extra_pool_hit_pages=None):
                    self.kv_hit_pages = kv_hit_pages
                    self.extra_pool_hit_pages = extra_pool_hit_pages or {}
                # Alias for older callers that read .prefix_keys
                @property
                def prefix_keys(self):
                    return self.kv_hit_pages

        kv_pages = self.batch_exists(keys, extra_info)
        hit_count = {PoolName.KV: kv_pages} if kv_pages else {}
        final_pages = kv_pages

        for transfer in pool_transfers or []:
            if final_pages == 0:
                break
            comp_keys, mult = self._v2_component_keys(transfer)
            locs = self._dir_get(comp_keys)
            self._prime(comp_keys, locs)
            ex = [loc is not None for loc in locs]
            page_exists = [
                all(ex[i * mult : (i + 1) * mult]) for i in range(kv_pages)
            ]
            boundary = 0
            if transfer.hit_policy == PoolHitPolicy.ALL_PAGES:
                boundary = next(
                    (i for i in range(kv_pages) if not page_exists[i]), kv_pages
                )
            else:  # trailing pages
                # The sidecar pool holds only a trailing window
                # (e.g. Mamba/SWA state). `transfer.keys` covers exactly that
                # window; if every window page exists, the whole KV prefix is
                # usable (the sidecar only guards the tail), else nothing is.
                window = len(transfer.keys) if transfer.keys else 1
                ex_window = [loc is not None for loc in locs][:window]
                boundary = kv_pages if all(ex_window) and window > 0 else 0
            if boundary:
                hit_count[transfer.name] = boundary
            final_pages = min(final_pages, boundary)

        return PoolTransferResult(final_pages, hit_count)

    # ------------------------------------------------------------------ #
    # Abstract single-key / batch API.
    #
    # SGLang's HiCache controller drives this in two shapes:
    #   * zero-copy:    set(key, target_location=ptr, target_sizes=nbytes)
    #   * value-based:  batch_set(keys, data)  where data is a list of host KV
    #                   page tensors/bytes (the generic page backup path).
    # We accept both. `target_locations`/`values` entries may be raw int ptrs,
    # bytes-like, or objects exposing data_ptr()/numel()/element_size() (tensors).
    # ------------------------------------------------------------------ #
    @staticmethod
    def _obj_ptr_size(x, keepalive: list):
        """(addr, nbytes) for an int ptr, a bytes-like, or a tensor-like object.
        Bytes-like objects are copied into a ctypes buffer appended to `keepalive`
        so the address stays valid for the duration of the publish/fetch."""
        if isinstance(x, int):
            return x, None
        dp = getattr(x, "data_ptr", None)
        if callable(dp):
            n = getattr(x, "numel", None)
            es = getattr(x, "element_size", None)
            nbytes = (n() * es()) if callable(n) and callable(es) else getattr(x, "nbytes", None)
            return dp(), (int(nbytes) if nbytes is not None else None)
        if isinstance(x, (bytes, bytearray, memoryview)):
            b = bytes(x)
            buf = (ctypes.c_char * len(b)).from_buffer_copy(b)
            keepalive.append(buf)
            return ctypes.addressof(buf), len(b)
        # numpy array
        if hasattr(x, "ctypes") and hasattr(x, "nbytes"):
            keepalive.append(x)
            return x.ctypes.data, int(x.nbytes)
        raise TypeError(f"peercache: unsupported value/location type {type(x)}")

    def _ptrs_sizes(self, locs, sizes, keepalive: list):
        ptrs, out_sizes = [], []
        for i, x in enumerate(locs):
            p, n = self._obj_ptr_size(x, keepalive)
            ptrs.append(p)
            out_sizes.append(int(sizes[i]) if sizes is not None else n)
        return ptrs, out_sizes

    def set(self, key, value=None, target_location=None, target_sizes=None) -> bool:
        return self.batch_set(
            [key], None if value is None else [value],
            None if target_location is None else [target_location],
            None if target_sizes is None else [target_sizes],
        )

    def batch_set(self, keys, values=None, target_locations=None, target_sizes=None) -> bool:
        keys = list(keys)
        # The generic value/pointer API stores one blob per raw key (no K/V
        # split), so exists()/batch_exists() must look up raw keys too.
        self._raw_keys = True
        self._keyspace_detected = True
        keepalive: list = []
        src = target_locations if target_locations is not None else values
        if src is None:
            return False
        ptrs, sizes = self._ptrs_sizes(list(src), target_sizes, keepalive)
        res = self._publish(self._tag_keys(keys), ptrs, sizes)
        return all(res)

    def get(self, key, target_location=None, target_sizes=None):
        res = self.batch_get(
            [key],
            None if target_location is None else [target_location],
            None if target_sizes is None else [target_sizes],
        )
        return res[0]

    def batch_get(self, keys, target_locations=None, target_sizes=None):
        """Fill the given destinations (host page tensors or ptrs) from the cache.
        Returns a list aligned with `keys`: the destination on a hit, else None."""
        keys = list(keys)
        self._raw_keys = True
        self._keyspace_detected = True
        if target_locations is None:
            return [None] * len(keys)
        dsts = list(target_locations)
        keepalive: list = []
        ptrs, sizes = self._ptrs_sizes(dsts, target_sizes, keepalive)
        oks = self._fetch(self._tag_keys(keys), ptrs, sizes)
        return [dsts[i] if oks[i] else None for i in range(len(keys))]

    def exists(self, key) -> bool:
        return self.batch_exists([key]) > 0

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

    def get_stats(self):
        """Return a StorageMetrics-like object consumed by SGLang's HiCache.

        SGLang's cache controller calls get_stats() on the backend to collect
        prefetch/backup page counts and bandwidth (see
        sglang/srt/observability/metrics_collector.py StorageMetrics). We fill
        the same four fields from our internal counters; each entry is a page
        count / bytes-per-second sample. Falls back to a plain object when the
        sglang dataclass is unavailable (standalone / no sglang installed).
        """
        snap = self._metrics.snapshot() if hasattr(self._metrics, "snapshot") else {}
        counters = snap.get("counters", {}) if isinstance(snap, dict) else {}

        def _bandwidth_estimate(total_bytes: int, elapsed: float) -> float:
            return (total_bytes / elapsed) if elapsed > 0 else 0.0

        # Keep a lightweight running sample window (reset on each call is fine:
        # SGLang polls get_stats() per operation batch).
        prefetch_pgs = [counters.get("read_requests", 0)]
        backup_pgs = [counters.get("write_requests", 0)]
        try:
            elapsed = max(1e-9, time.time() - self._metrics._start)
        except Exception:
            elapsed = 1.0
        prefetch_bandwidth = [
            _bandwidth_estimate(counters.get("bytes_read", 0), elapsed)
        ]
        backup_bandwidth = [
            _bandwidth_estimate(counters.get("bytes_written", 0), elapsed)
        ]
        try:
            from sglang.srt.observability.metrics_collector import StorageMetrics

            return StorageMetrics(
                prefetch_pgs=prefetch_pgs,
                backup_pgs=backup_pgs,
                prefetch_bandwidth=prefetch_bandwidth,
                backup_bandwidth=backup_bandwidth,
            )
        except Exception:
            from types import SimpleNamespace

            return SimpleNamespace(
                prefetch_pgs=prefetch_pgs,
                backup_pgs=backup_pgs,
                prefetch_bandwidth=prefetch_bandwidth,
                backup_bandwidth=backup_bandwidth,
            )

    def check_server(self) -> bool:
        """Lightweight readiness probe: discovery reachable + ring formed.

        Mirrors MooncakeStore.check_server() so SGLang can fail fast at attach
        time instead of surfacing errors on the first request.
        """
        try:
            if self.runtime is None or not getattr(self.runtime, "started", True):
                return False
            # At least ourselves must be in the membership ring.
            if len(self.runtime.ring) < 1:
                return False
            return True
        except Exception:
            return False

    def warmup(self) -> None:
        """No-op warmup hook (kept for interface parity with other backends).

        PeerCache's data plane is lazy: the first publish/fetch allocates the
        pool and opens channels, which SGLang exercises during server warmup
        anyway. Explicit pre-allocation would waste host memory on idle nodes.
        """
        return None

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

