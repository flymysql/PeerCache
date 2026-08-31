"""SGLang-integration contract tests (no GPU required).

These tests verify PeerCacheStore against a *real* sglang installation's
HiCacheStorage interface — the same contract sglang's `dynamic` backend and
`StorageBackendFactory` rely on. They run in CI on CPU runners.

Skip conditions:
  * sglang not installed            -> skipped (standalone PeerCache dev box)
  * sglang installed, interface OK  -> run (full contract)

Usage:
    pytest tests/sglang/test_sglang_contract.py -v
"""
import ctypes
import sys
import time
from types import SimpleNamespace

import pytest

peercache_store = pytest.importorskip("peercache.store")
from peercache.store import PeerCacheStore  # noqa: E402

sglang = pytest.importorskip("sglang")
hicache = pytest.importorskip("sglang.srt.mem_cache.hicache_storage")
from sglang.srt.mem_cache.hicache_storage import (  # noqa: E402
    HiCacheStorage,
    HiCacheStorageConfig,
)


class _Buf:
    def __init__(self, n):
        self._b = (ctypes.c_byte * n)()

    def data_ptr(self):
        return ctypes.addressof(self._b)

    def numel(self):
        return len(self._b)

    def element_size(self):
        return 1


class _MemPoolHost:
    def __init__(self, page_bytes, num_pages):
        self.page_bytes = page_bytes
        self.kv_buffer = _Buf(page_bytes * num_pages)

    def get_page_buffer_meta(self, host_indices):
        base = self.kv_buffer.data_ptr()
        return ([base + i * self.page_bytes for i in host_indices],
                [self.page_bytes] * len(host_indices))


def _cfg(addr, node_id):
    return SimpleNamespace(
        tp_rank=0, tp_size=1, pp_rank=0, pp_size=1, is_mla_model=True,
        extra_config={
            "discovery_addr": addr, "protocol": "tcp", "device_name": "",
            "local_hostname": "127.0.0.1", "node_id": node_id,
            "heartbeat_interval": 0.2, "member_ttl": 30.0,
            "global_segment_size": 8 << 20, "metrics_enabled": False,
        },
    )


@pytest.fixture
def cluster():
    from peercache.discovery import DiscoveryServer

    meta = DiscoveryServer("127.0.0.1", 0)
    addr = "127.0.0.1:%d" % meta.start()
    a = PeerCacheStore(_cfg(addr, "A"))
    b = PeerCacheStore(_cfg(addr, "B"))
    deadline = time.time() + 10
    while time.time() < deadline and (len(a.runtime.ring) < 2 or len(b.runtime.ring) < 2):
        time.sleep(0.05)
    try:
        yield a, b
    finally:
        a.close()
        b.close()
        meta.stop()


def test_real_sglang_subclass():
    """PeerCacheStore must subclass the REAL sglang HiCacheStorage."""
    assert issubclass(PeerCacheStore, HiCacheStorage)


def test_real_sglang_config_roundtrip_v1(cluster):
    """Drive batch_set_v1/batch_get_v1 with a REAL HiCacheStorageConfig."""
    a, b = cluster
    page, n = 4096, 64
    a.register_mem_pool_host(_MemPoolHost(page, n))
    b.register_mem_pool_host(_MemPoolHost(page, n))
    keys = ["real%d" % i for i in range(6)]
    assert all(a.batch_set_v1(keys, list(range(6))))
    assert b.batch_exists(keys) == 6
    assert all(b.batch_get_v1(keys, list(range(6))))


def _sglang_v2_types():
    """Return (PoolName, PoolTransfer, PoolTransferResult) from the installed
    sglang, or lightweight stand-ins when this sglang release predates the
    v2 interface (0.5.9 has HiCacheStorage v1 only; main adds v2)."""
    from sglang.srt.mem_cache import hicache_storage as hs

    PoolName = getattr(hs, "PoolName", None)
    PoolTransfer = getattr(hs, "PoolTransfer", None)
    PoolTransferResult = getattr(hs, "PoolTransferResult", None)

    if PoolName is None:
        class _PoolName(str):
            KV = "kv"
            MAMBA = "mamba"
            SWA = "swa"
            INDEXER = "indexer"
        PoolName = _PoolName

    if PoolTransfer is None:
        PoolTransfer = SimpleNamespace

    if PoolTransferResult is None:
        class _PTR:
            def __init__(self, kv_hit_pages, extra_pool_hit_pages=None):
                self.kv_hit_pages = kv_hit_pages
                self.extra_pool_hit_pages = extra_pool_hit_pages or {}
        PoolTransferResult = _PTR

    return PoolName, PoolTransfer, PoolTransferResult


def test_real_sglang_v2_pool_transfers(cluster):
    """batch_set_v2/get_v2 with real sglang PoolTransfer + PoolName."""
    PoolName, PoolTransfer, _ = _sglang_v2_types()

    a, b = cluster
    page, n = 4096, 64
    a.register_mem_pool_host(_MemPoolHost(page, n))
    b.register_mem_pool_host(_MemPoolHost(page, n))
    keys = ["v2real%d" % i for i in range(4)]
    t = PoolTransfer(name=PoolName.KV, keys=keys, host_indices=list(range(4)))
    res = a.batch_set_v2([t])
    assert all(res[PoolName.KV])
    res = b.batch_get_v2([PoolTransfer(name=PoolName.KV, keys=keys, host_indices=list(range(4)))])
    assert all(res[PoolName.KV])


def test_real_sglang_batch_exists_v2_result(cluster):
    """batch_exists_v2 must return a PoolTransferResult-shaped object."""
    _, _, PoolTransferResult = _sglang_v2_types()

    a, b = cluster
    page, n = 4096, 64
    a.register_mem_pool_host(_MemPoolHost(page, n))
    b.register_mem_pool_host(_MemPoolHost(page, n))
    keys = ["exreal%d" % i for i in range(3)]
    assert all(a.batch_set_v1(keys, list(range(3))))
    out = b.batch_exists_v2(keys, pool_transfers=None)
    # Accept either the real sglang PoolTransferResult or PeerCache's local
    # fallback (older sglang releases): assert on shape, not the exact class.
    kv_hits = getattr(out, "kv_hit_pages", getattr(out, "prefix_keys", None))
    assert kv_hits == 3
    assert getattr(out, "extra_pool_hit_pages", None) is not None


def test_real_sglang_get_stats_shape(cluster):
    """get_stats() returns the real StorageMetrics dataclass when sglang present."""
    a, _ = cluster
    try:
        from sglang.srt.observability.metrics_collector import StorageMetrics
    except Exception:
        pytest.skip("StorageMetrics not importable in this sglang version")
    stats = a.get_stats()
    assert isinstance(stats, StorageMetrics)
    assert hasattr(stats, "prefetch_pgs") and hasattr(stats, "backup_pgs")

