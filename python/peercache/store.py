"""PeerCacheStore: the SGLang HiCache L3 storage backend.

Registered with SGLang via the ``dynamic`` backend mechanism (no SGLang patch):

    --hicache-storage-backend dynamic
    --hicache-storage-backend-extra-config
        '{"backend_name":"peercache","module_path":"peercache.store",
          "class_name":"PeerCacheStore","discovery_addr":"META:9100", ...}'

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
from typing import Any, List, Optional

from peercache.config import PeerCacheConfig
from peercache.pool import PublishedPool
from peercache.server import NodeRuntime
from peercache.transport import ReadOp
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


def _alloc_host_buffer(size: int):
    """Allocate a pinned/host buffer and return (keepalive_obj, base_addr)."""
    try:
        import torch

        # Pinned memory is required for real RDMA registration; falls back to
        # pageable if pinning fails (still fine for the TCP transport).
        try:
            t = torch.empty(size, dtype=torch.uint8, pin_memory=True)
        except Exception:
            t = torch.empty(size, dtype=torch.uint8)
        return t, t.data_ptr()
    except Exception:
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

    # ------------------------------------------------------------------ #
    # Registration
    # ------------------------------------------------------------------ #
    def register_mem_pool_host(self, mem_pool_host):
        self.mem_pool_host = mem_pool_host

        # 1) Receive MR: SGLang's host KV buffer is the destination of READs.
        kv = mem_pool_host.kv_buffer
        kv_ptr = kv.data_ptr()
        kv_bytes = kv.numel() * kv.element_size()
        self._recv_mr = self.runtime.transport.register_mr(kv_ptr, kv_bytes)

        # 2) Published-pool MR: backend-owned source of remote READs (per-TP slice).
        capacity = max(1, self.config.global_segment_size // self.tp_size)
        self._pool_keepalive, base_addr = _alloc_host_buffer(capacity)
        pool_mr = self.runtime.transport.register_mr(base_addr, capacity)
        self._pool = PublishedPool(
            base_addr=base_addr,
            capacity=capacity,
            rkey=pool_mr.rkey,
            on_evict=self._on_pool_evict,
        )
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

    def _on_pool_evict(self, evicted_keys: List[str]) -> None:
        # A page left the published pool -> drop its directory entry so readers
        # stop resolving a now-invalid address.
        try:
            self.runtime.directory.delete(evicted_keys)
        except Exception as e:  # best-effort
            logger.debug("peercache: directory delete on evict failed: %s", e)

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
        ex = self.runtime.directory.exists(comp_keys)
        for i, present in enumerate(ex):
            if not present:
                return i // mult
        return len(comp_keys) // mult

    # ------------------------------------------------------------------ #
    # Core publish / fetch over component objects
    # ------------------------------------------------------------------ #
    def _publish(self, comp_keys: List[str], ptrs: List[int], sizes: List[int]) -> List[bool]:
        # Skip components already present in the directory (idempotent set).
        existing = self.runtime.directory.exists(comp_keys)
        entries = {}
        results = [False] * len(comp_keys)
        endpoint = self.runtime.local_rdma_endpoint
        for i, key in enumerate(comp_keys):
            if existing[i]:
                results[i] = True
                continue
            remote_addr = self._pool.publish(key, ptrs[i], sizes[i])
            if remote_addr is None:
                continue  # pool could not fit this page
            entries[key] = DataLocation(
                node_id=self.config.node_id,
                rdma_endpoint=endpoint,
                remote_addr=remote_addr,
                rkey=self._pool.rkey,
                length=sizes[i],
            )
            results[i] = True
        if entries:
            self.runtime.directory.put(entries)
        return results

    def _fetch(self, comp_keys: List[str], ptrs: List[int], sizes: List[int]) -> List[bool]:
        locations = self.runtime.directory.get(comp_keys)
        results = [False] * len(comp_keys)
        ops: List[ReadOp] = []
        op_index: List[int] = []
        for i, loc in enumerate(locations):
            if loc is None:
                continue
            if loc.length != sizes[i]:
                continue  # size mismatch -> treat as miss
            if loc.node_id == self.config.node_id:
                # Data is local: a plain memcpy, no network.
                ctypes.memmove(ptrs[i], loc.remote_addr, loc.length)
                results[i] = True
                continue
            ops.append(
                ReadOp(
                    remote_endpoint=loc.rdma_endpoint,
                    local_addr=ptrs[i],
                    remote_addr=loc.remote_addr,
                    rkey=loc.rkey,
                    length=loc.length,
                )
            )
            op_index.append(i)
        if ops:
            oks = self.runtime.transport.batch_read(ops)
            for j, ok in enumerate(oks):
                results[op_index[j]] = bool(ok)
        return results

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
        if self._pool is not None:
            keys = list(self._pool._entries.keys())  # snapshot
            self.runtime.directory.delete(keys)
            for k in keys:
                self._pool.remove(k)

    def close(self) -> None:
        self.runtime.stop()
