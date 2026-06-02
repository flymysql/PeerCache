"""Contract tests for the SGLang HiCacheStorage interface PeerCacheStore exposes.

These guard against accidental API drift (method removal / signature changes)
and exercise the v1 and v2 zero-copy paths end-to-end over the in-process TCP
transport (functional only -- not a performance scenario).
"""

import ctypes
import sys
import time
import types
from types import SimpleNamespace

import pytest

from peercache.store import PeerCacheStore


# --------------------------------------------------------------------------- #
# Minimal SGLang-side stand-ins (mem pool host + v2 transfer + sglang module)
# --------------------------------------------------------------------------- #
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
            "disk_enabled": False,
        },
    )


@pytest.fixture
def cluster():
    from peercache.discovery import DiscoveryServer

    meta = DiscoveryServer("127.0.0.1", 0)
    addr = f"127.0.0.1:{meta.start()}"
    a = PeerCacheStore(_cfg(addr, "A"))
    b = PeerCacheStore(_cfg(addr, "B"))
    deadline = time.time() + 10
    while time.time() < deadline and (len(a.runtime.ring) < 2 or len(b.runtime.ring) < 2):
        time.sleep(0.05)
    try:
        yield a, b
    finally:
        a.close(); b.close(); meta.stop()


class _FakeTensor:
    """Host buffer exposing the tensor-like API SGLang hands to batch_set/get."""

    def __init__(self, nbytes, fill=0):
        self._buf = (ctypes.c_byte * nbytes)()
        for i in range(nbytes if fill else 0):
            self._buf[i] = (fill + i) % 251
        self._n = nbytes

    def data_ptr(self):
        return ctypes.addressof(self._buf)

    def numel(self):
        return self._n

    def element_size(self):
        return 1

    def to_bytes(self):
        return bytes(self._buf)


def test_generic_value_set_get_roundtrip(cluster):
    # SGLang's generic page backup calls batch_set(hash_values, data) where data
    # is a list of host KV page tensors (not the zero-copy ptr form). Reading it
    # back via batch_get(keys, dst_tensors) must fill the destinations.
    a, b = cluster
    a.register_mem_host_pool_v2(_MemPoolHost(4096, 8), "kv")
    b.register_mem_host_pool_v2(_MemPoolHost(4096, 8), "kv")
    keys = ["g0", "g1", "g2"]
    vals = [_FakeTensor(4096, fill=i + 1) for i in range(3)]
    assert a.batch_set(keys, vals) is True          # value form, no target_locations
    dsts = [_FakeTensor(4096) for _ in range(3)]
    out = b.batch_get(keys, dsts)                    # fill-target form
    assert all(o is not None for o in out)
    for i in range(3):
        assert dsts[i].to_bytes() == vals[i].to_bytes()
    # bytes values + single-key set/get also work
    assert a.batch_set(["gb"], [b"\x01\x02\x03\x04" * 8])
    d = _FakeTensor(32)
    assert b.get("gb", target_location=d) is d
    assert d.to_bytes()[:4] == b"\x01\x02\x03\x04"


def test_contract_methods_present():
    # The exact surface SGLang's `dynamic` backend calls.
    for name in (
        "register_mem_pool_host", "register_mem_host_pool_v2",
        "batch_set_v1", "batch_get_v1", "batch_exists",
        "batch_set_v2", "batch_get_v2", "batch_exists_v2",
        "set", "get", "batch_set", "batch_get", "exists", "clear", "close",
    ):
        assert callable(getattr(PeerCacheStore, name, None)), f"missing {name}"


def test_v1_set_exists_get_roundtrip(cluster):
    a, b = cluster
    page, n = 4096, 64
    a.register_mem_pool_host(_MemPoolHost(page, n))
    b.register_mem_pool_host(_MemPoolHost(page, n))
    keys = [f"k{i}" for i in range(8)]
    assert all(a.batch_set_v1(keys, list(range(8))))
    # exists on the consumer sees the published prefix
    assert b.batch_exists(keys) == 8
    oks = b.batch_get_v1(keys, list(range(8)))
    assert all(oks) and len(oks) == 8


def test_exists_get_handoff_saves_directory_lookup(cluster):
    # batch_exists() primes the resident hit locations; the following
    # batch_get() must consume them (skipping a second directory RPC) and the
    # primes are one-shot, so a later get without a fresh exists re-resolves.
    a, b = cluster
    page, n = 4096, 64
    a.register_mem_pool_host(_MemPoolHost(page, n))
    b.register_mem_pool_host(_MemPoolHost(page, n))
    keys = [f"h{i}" for i in range(8)]
    assert all(a.batch_set_v1(keys, list(range(8))))

    def saved():
        return b._metrics.snapshot()["counters"]["directory_lookups_saved"]

    base = saved()
    assert b.batch_exists(keys) == 8
    assert all(b.batch_get_v1(keys, list(range(8))))
    assert saved() - base == 8  # the get reused all 8 primed locations

    # No preceding exists -> nothing primed -> directory is queried again.
    assert all(b.batch_get_v1(keys, list(range(8))))
    assert saved() - base == 8


def test_generic_set_then_batch_exists_finds_pages(cluster):
    # Regression: SGLang's generic backup writes via batch_set (raw keys) while
    # prefetch probes via batch_exists. batch_exists must look up the SAME (raw)
    # keyspace -- otherwise it misses every page (exists_pages_found stays 0)
    # even though data is being written, and SGLang never issues a get.
    a, b = cluster
    a.register_mem_host_pool_v2(_MemPoolHost(4096, 8), "kv")
    b.register_mem_host_pool_v2(_MemPoolHost(4096, 8), "kv")
    keys = [f"gx{i}" for i in range(5)]
    vals = [_FakeTensor(4096, fill=i + 1) for i in range(5)]
    assert a.batch_set(keys, vals) is True
    # b never wrote, so it must self-detect the producer's raw keyspace.
    assert b.batch_exists(keys) == 5
    assert b._metrics.snapshot()["counters"]["exists_pages_found"] >= 5
    dsts = [_FakeTensor(4096) for _ in range(5)]
    out = b.batch_get(keys, dsts)
    assert all(o is not None for o in out)
    for i in range(5):
        assert dsts[i].to_bytes() == vals[i].to_bytes()


def test_v2_kv_pool_roundtrip(cluster):
    a, b = cluster
    page, n = 4096, 64
    a.register_mem_pool_host(_MemPoolHost(page, n))
    b.register_mem_pool_host(_MemPoolHost(page, n))
    keys = [f"v2k{i}" for i in range(4)]
    t_set = SimpleNamespace(name="kv", keys=keys, host_indices=list(range(4)))
    res = a.batch_set_v2([t_set])
    assert all(res["kv"])
    t_get = SimpleNamespace(name="kv", keys=keys, host_indices=list(range(4)))
    res = b.batch_get_v2([t_get])
    assert all(res["kv"]) and len(res["kv"]) == 4


def test_v2_only_registration_creates_pool_and_roundtrips(cluster):
    # SGLang versions that register the KV pool via register_mem_host_pool_v2
    # (and never call register_mem_pool_host) must still get a published pool,
    # otherwise PeerCache can't publish anything (pool_capacity_bytes stays 0).
    a, b = cluster
    page, n = 4096, 64
    pool_a = _MemPoolHost(page, n)
    pool_b = _MemPoolHost(page, n)
    a.register_mem_host_pool_v2(pool_a, "kv")
    b.register_mem_host_pool_v2(pool_b, "kv")
    # The published pool + mem pool must now exist on the v2 path.
    assert a._pool is not None and a._pool.capacity > 0
    assert a.mem_pool_host is pool_a
    keys = [f"v2only{i}" for i in range(4)]
    res = a.batch_set_v2([SimpleNamespace(name="kv", keys=keys, host_indices=list(range(4)))])
    assert all(res["kv"])
    res = b.batch_get_v2([SimpleNamespace(name="kv", keys=keys, host_indices=list(range(4)))])
    assert all(res["kv"]) and len(res["kv"]) == 4


def test_v2_exists_with_mocked_sglang(cluster, monkeypatch):
    # batch_exists_v2 lazily imports PoolHitPolicy/PoolTransferResult from sglang;
    # inject a minimal fake so the contract (return type) can be exercised.
    fake = types.ModuleType("sglang.srt.mem_cache.hicache_storage")

    class PoolHitPolicy:
        ALL_PAGES = "all"

    class PoolTransferResult:
        def __init__(self, prefix_keys, hit_count):
            self.prefix_keys = prefix_keys
            self.hit_count = hit_count

    fake.PoolHitPolicy = PoolHitPolicy
    fake.PoolTransferResult = PoolTransferResult
    for mod in ("sglang", "sglang.srt", "sglang.srt.mem_cache",
                "sglang.srt.mem_cache.hicache_storage"):
        monkeypatch.setitem(sys.modules, mod, fake)

    a, b = cluster
    page, n = 4096, 64
    a.register_mem_pool_host(_MemPoolHost(page, n))
    b.register_mem_pool_host(_MemPoolHost(page, n))
    keys = [f"ex2k{i}" for i in range(4)]
    assert all(a.batch_set_v1(keys, list(range(4))))
    out = b.batch_exists_v2(keys, pool_transfers=None)
    assert out.prefix_keys == 4
