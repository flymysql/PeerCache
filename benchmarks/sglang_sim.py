"""Faithful SGLang HiCache stand-ins for the PeerCache benchmark.

Reproduces just enough of SGLang's HiCache host side to drive PeerCache's
``HiCacheStorage`` interface exactly as SGLang does:

* ``HostKVPool`` mimics SGLang's ``HostKVCache``: it owns the host KV buffer and
  implements ``get_page_buffer_meta(host_indices) -> (ptrs, sizes)``. PeerCache
  reads/writes pages by these pointers (zero copy), so the page byte layout here
  matches what ``batch_set_v1`` / ``batch_get_v1`` expect:
    - **MLA** layout: 1 storage object per page (``_<pp>_k``).
    - **MHA** layout: 2 objects per page (``_<tp>_k`` and ``_<tp>_v``),
      interleaved k,v per slot -- mirroring ``PeerCacheStore._component_keys``.

* ``Cluster`` brings up an embedded discovery service plus N ``PeerCacheStore``
  nodes in one process, modelling PD-disaggregation: a producer node (prefill)
  publishes pages, a consumer node (decode) reads them back over the fabric.

This module performs no measurement; ``bench_hicache.py`` drives it.
"""

from __future__ import annotations

import ctypes
import time
from types import SimpleNamespace
from typing import List, Optional


def alloc_host_buffer(size: int):
    """(keepalive, base_addr). Prefer torch pinned memory for real RDMA; the
    ctypes fallback still registers fine, just not page-locked."""
    try:
        import torch

        try:
            t = torch.empty(size, dtype=torch.uint8, pin_memory=True)
        except Exception:
            t = torch.empty(size, dtype=torch.uint8)
        return t, t.data_ptr()
    except Exception:
        buf = (ctypes.c_byte * size)()
        return buf, ctypes.addressof(buf)


class _KVBuffer:
    """Matches the duck-typed interface PeerCache reads off ``kv_buffer``."""

    def __init__(self, base_addr: int, nbytes: int, keepalive) -> None:
        self._base = base_addr
        self._n = nbytes
        self._keepalive = keepalive

    def data_ptr(self) -> int:
        return self._base

    def numel(self) -> int:
        return self._n

    def element_size(self) -> int:
        return 1


class HostKVPool:
    """SGLang HostKVCache stand-in.

    page_bytes : bytes of one KV component object (k or v) for one page.
    num_slots  : number of page slots in the host buffer (host_indices range).
    layout     : "mla" (1 object/page) or "mha" (2 objects/page: k and v).
    """

    def __init__(self, page_bytes: int, num_slots: int, layout: str = "mla") -> None:
        assert layout in ("mla", "mha")
        self.page_bytes = page_bytes
        self.num_slots = num_slots
        self.layout = layout
        self.comps = 1 if layout == "mla" else 2
        self.slot_stride = page_bytes * self.comps
        total = self.slot_stride * num_slots
        self._keepalive, self._base = alloc_host_buffer(total)
        self.kv_buffer = _KVBuffer(self._base, total, self._keepalive)

    def get_page_buffer_meta(self, host_indices):
        """Return interleaved component (ptr, size) lists for the given slots.

        For MHA this yields [k0, v0, k1, v1, ...] aligned with the component
        keys PeerCache derives for each page key.
        """
        ptrs: List[int] = []
        sizes: List[int] = []
        pb = self.page_bytes
        for i in host_indices:
            base = self._base + i * self.slot_stride
            for c in range(self.comps):
                ptrs.append(base + c * pb)
                sizes.append(pb)
        return ptrs, sizes

    def fill_slot(self, idx: int, byte_val: int) -> None:
        seg = (ctypes.c_byte * self.slot_stride).from_address(
            self._base + idx * self.slot_stride
        )
        for j in range(self.slot_stride):
            seg[j] = (byte_val + j) % 251

    def page_bytes_total(self) -> int:
        """Total bytes transferred for one logical page (all components)."""
        return self.slot_stride


def _store_extra(discovery_addr, node_id, protocol, device_name, seg_bytes,
                 disk, metrics, ib_port, gid_index):
    extra = {
        "discovery_addr": discovery_addr,
        "protocol": protocol,
        "device_name": device_name,
        "local_hostname": "127.0.0.1",
        "node_id": node_id,
        "heartbeat_interval": 0.2,
        "member_ttl": 30.0,
        "global_segment_size": seg_bytes,
        "metrics_enabled": metrics,
        "disk_enabled": disk,
        "ib_port": ib_port,
        "gid_index": gid_index,
    }
    return extra


class Cluster:
    """Embedded discovery + N PeerCacheStore nodes (PD-disaggregation model)."""

    def __init__(
        self,
        n_nodes: int = 2,
        protocol: str = "rdma",
        device_name: str = "",
        seg_bytes: int = 1 << 30,
        layout: str = "mla",
        disk: bool = False,
        metrics: bool = False,
        ib_port: int = 1,
        gid_index: int = 3,
        ring_timeout: float = 15.0,
    ) -> None:
        from peercache.discovery import DiscoveryServer
        from peercache.store import PeerCacheStore

        self._meta = DiscoveryServer("127.0.0.1", 0)
        port = self._meta.start()
        addr = f"127.0.0.1:{port}"
        is_mla = layout == "mla"

        self.stores = []
        for i in range(n_nodes):
            cfg = SimpleNamespace(
                tp_rank=0, tp_size=1, pp_rank=0, pp_size=1,
                is_mla_model=is_mla,
                extra_config=_store_extra(
                    addr, f"node{i}", protocol, device_name, seg_bytes,
                    disk, metrics, ib_port, gid_index,
                ),
            )
            self.stores.append(PeerCacheStore(cfg))

        deadline = time.time() + ring_timeout
        while time.time() < deadline:
            if all(len(s.runtime.ring) >= n_nodes for s in self.stores):
                break
            time.sleep(0.05)
        else:
            self.close()
            raise TimeoutError(f"cluster ring did not reach {n_nodes} nodes")

    def producer(self):
        return self.stores[0]

    def consumer(self):
        return self.stores[-1]

    def register_pools(self, page_bytes: int, slots_per_node: int, layout: str):
        """Attach a HostKVPool to every node and return them (same order)."""
        pools = []
        for s in self.stores:
            p = HostKVPool(page_bytes, slots_per_node, layout=layout)
            s.register_mem_pool_host(p)
            pools.append(p)
        return pools

    def close(self) -> None:
        for s in getattr(self, "stores", []):
            try:
                s.close()
            except Exception:
                pass
        try:
            self._meta.stop()
        except Exception:
            pass
