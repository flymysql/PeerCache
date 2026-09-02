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


def test_v2_sidecar_indexer_pool_roundtrip(cluster, monkeypatch):
    """DSA / MiniMax sidecar: SGLang injects an INDEXER PoolTransfer
    (ALL_PAGES, indices_from_pool=KV). Writes via batch_set_v2 must land in a
    pool-namespaced keyspace, and reads via batch_get_v2 must return them;
    batch_exists_v2 must combine KV + INDEXER into a usable prefix."""
    fake = types.ModuleType("sglang.srt.mem_cache.hicache_storage")

    class PoolHitPolicy:
        ALL_PAGES = "all_pages"
        TRAILING_PAGES = "trailing_pages"

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
    # Sidecar pool (e.g. DSA indexer) registered under its own name.
    a.register_mem_host_pool_v2(_MemPoolHost(page, n), "indexer")
    b.register_mem_host_pool_v2(_MemPoolHost(page, n), "indexer")

    keys = [f"side{i}" for i in range(4)]
    # KV pool write (v2 path)
    kv_set = SimpleNamespace(name="kv", keys=keys, host_indices=list(range(4)))
    res = a.batch_set_v2([kv_set])
    assert all(res["kv"])
    # INDEXER pool write (v2 path, same host_indices = indices_from_pool=KV)
    idx_set = SimpleNamespace(name="indexer", keys=keys, host_indices=list(range(4)))
    res = a.batch_set_v2([idx_set])
    assert all(res["indexer"])

    # exists_v2: KV + INDEXER both present -> full prefix usable
    transfers = [
        SimpleNamespace(name="indexer", keys=keys, host_indices=list(range(4)),
                        hit_policy=PoolHitPolicy.ALL_PAGES, indices_from_pool="kv")
    ]
    out = b.batch_exists_v2(keys, pool_transfers=transfers)
    assert out.prefix_keys == 4
    assert out.hit_count.get("indexer") == 4

    # get_v2: both pools read back
    kv_get = SimpleNamespace(name="kv", keys=keys, host_indices=list(range(4)))
    idx_get = SimpleNamespace(name="indexer", keys=keys, host_indices=list(range(4)))
    res = b.batch_get_v2([kv_get, idx_get])
    assert all(res["kv"]) and all(res["indexer"])


def test_v2_sidecar_missing_clamps_prefix(cluster, monkeypatch):
    """If the sidecar pool is missing pages, batch_exists_v2 must clamp the
    usable prefix (ALL_PAGES semantics) instead of returning the full KV."""
    fake = types.ModuleType("sglang.srt.mem_cache.hicache_storage")

    class PoolHitPolicy:
        ALL_PAGES = "all_pages"
        TRAILING_PAGES = "trailing_pages"

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
    a.register_mem_host_pool_v2(_MemPoolHost(page, n), "indexer")
    b.register_mem_host_pool_v2(_MemPoolHost(page, n), "indexer")

    keys = [f"clamp{i}" for i in range(4)]
    # Only KV written; INDEXER sidecar is empty.
    kv_set = SimpleNamespace(name="kv", keys=keys, host_indices=list(range(4)))
    res = a.batch_set_v2([kv_set])
    assert all(res["kv"])

    transfers = [
        SimpleNamespace(name="indexer", keys=keys, host_indices=list(range(4)),
                        hit_policy=PoolHitPolicy.ALL_PAGES, indices_from_pool="kv")
    ]
    out = b.batch_exists_v2(keys, pool_transfers=transfers)
    assert out.prefix_keys == 0  # clamped: no INDEXER pages -> unusable prefix
    assert out.hit_count.get("indexer") in (None, 0)


def test_get_stats_and_check_server(cluster):
    """get_stats() returns a StorageMetrics-compatible object; check_server()
    reports readiness once the ring is formed."""
    a, b = cluster
    # ring formed by the fixture (>= 2 members)
    assert a.check_server() is True

    stats = a.get_stats()
    for attr in ("prefetch_pgs", "backup_pgs", "prefetch_bandwidth", "backup_bandwidth"):
        assert hasattr(stats, attr), f"get_stats missing {attr}"
    assert isinstance(stats.prefetch_pgs, list)
    assert isinstance(stats.backup_pgs, list)

    # After a write, backup_pgs reflects activity.
    page, n = 4096, 64
    a.register_mem_pool_host(_MemPoolHost(page, n))
    keys = [f"st{i}" for i in range(2)]
    assert all(a.batch_set_v1(keys, list(range(2))))
    stats2 = a.get_stats()
    assert stats2.backup_pgs and stats2.backup_pgs[0] >= 1


def test_prefix_isolation_two_tenants():
    """Two deployments sharing one discovery node with different `prefix`
    values must not see each other's keys (tenant/model isolation)."""
    from peercache.discovery import DiscoveryServer

    meta = DiscoveryServer("127.0.0.1", 0)
    addr = "127.0.0.1:%d" % meta.start()
    page, n = 4096, 64

    def make(node, prefix):
        cfg = _cfg(addr, node)
        cfg.extra_config = dict(cfg.extra_config)
        cfg.extra_config["prefix"] = prefix
        s = PeerCacheStore(cfg)
        s.register_mem_pool_host(_MemPoolHost(page, n))
        return s

    t1a = make("T1A", "tenant1")
    t2a = make("T2A", "tenant2")
    try:
        deadline = time.time() + 10
        while time.time() < deadline and (len(t1a.runtime.ring) < 2 or len(t2a.runtime.ring) < 2):
            time.sleep(0.05)

        keys = ["shared-key-%d" % i for i in range(3)]
        # Tenant 1 writes; tenant 2 must NOT see it (keys are namespaced).
        assert all(t1a.batch_set_v1(keys, list(range(3))))
        assert t2a.batch_exists(keys) == 0, "tenant2 saw tenant1's keys!"
        # Tenant 2 writes the same logical keys; both coexist.
        assert all(t2a.batch_set_v1(keys, list(range(3))))
        assert t1a.batch_exists(keys) == 3
        assert t2a.batch_exists(keys) == 3
    finally:
        t1a.close()
        t2a.close()
        meta.stop()


def test_v2_trailing_pages_hit_policy(cluster, monkeypatch):
    """Mamba/SWA: batch_exists_v2 with TRAILING_PAGES only requires the LAST
    N pages of the prefix to exist in the sidecar pool."""
    fake = types.ModuleType("sglang.srt.mem_cache.hicache_storage")

    class PoolHitPolicy:
        ALL_PAGES = "all_pages"
        TRAILING_PAGES = "trailing_pages"

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
    a.register_mem_host_pool_v2(_MemPoolHost(page, n), "swa")
    b.register_mem_host_pool_v2(_MemPoolHost(page, n), "swa")

    keys = [f"swa{i}" for i in range(5)]
    # KV pool: all 5 pages written.
    assert all(a.batch_set_v2([SimpleNamespace(name="kv", keys=keys, host_indices=list(range(5)))]).get("kv"))
    # SWA sidecar: only the last 2 pages written (trailing window).
    swa_keys = keys[-2:]
    assert all(a.batch_set_v2([SimpleNamespace(name="swa", keys=swa_keys, host_indices=[3, 4])]).get("swa"))

    transfers = [
        SimpleNamespace(name="swa", keys=swa_keys, host_indices=[3, 4],
                        hit_policy=PoolHitPolicy.TRAILING_PAGES, indices_from_pool="kv")
    ]
    # 5 KV pages + trailing-2 SWA window present -> full prefix usable.
    out = b.batch_exists_v2(keys, pool_transfers=transfers)
    assert out.prefix_keys == 5
    assert out.hit_count.get("swa") == 5

    # Only the middle pages of SWA exist -> the trailing window (the pages the
    # transfer actually guards) is missing -> nothing usable.
    keys2 = [f"swb{i}" for i in range(5)]
    assert all(a.batch_set_v2([SimpleNamespace(name="kv", keys=keys2, host_indices=list(range(5)))]).get("kv"))
    # SWA holds pages 2..3 (middle); the transfer guards the trailing window 3..4.
    assert all(a.batch_set_v2([SimpleNamespace(name="swa", keys=keys2[2:4], host_indices=[2, 3])]).get("swa"))
    transfers2 = [
        SimpleNamespace(name="swa", keys=keys2[3:5], host_indices=[3, 4],
                        hit_policy=PoolHitPolicy.TRAILING_PAGES, indices_from_pool="kv")
    ]
    out2 = b.batch_exists_v2(keys2, pool_transfers=transfers2)
    assert out2.prefix_keys == 0


# --------------------------------------------------------------------------- #
# DeepSeek-V4 HostPoolGroup registration (anchor KV + side pools as a group)
# --------------------------------------------------------------------------- #
class _HPGEntry:
    """One pool inside a HostPoolGroup (mirrors sglang pool_host/group.py)."""

    def __init__(self, name, host_pool, is_anchor=False):
        self.name = name
        self.host_pool = host_pool
        self.is_primary_index_anchor = is_anchor


class _HostPoolGroup:
    """sglang HostPoolGroup facade: anchor KV pool + side pools."""

    def __init__(self, entries):
        self.entries = list(entries)
        self.entry_map = {e.name: e for e in entries}
        self.anchor_entry = next(
            (e for e in entries if e.is_primary_index_anchor), entries[0]
        )

    def get_entry(self, name=None):
        return self.anchor_entry if name is None else self.entry_map[name]


def _side_pool(page_bytes, num_pages):
    """Single-buffer side pool (swa/c4/state etc.) with get_page_buffer_meta."""
    buf = _Buf(page_bytes * num_pages)
    return SimpleNamespace(
        page_bytes=page_bytes,
        kv_buffer=buf,
        v_buffer=_Buf(page_bytes * num_pages),
        bufs=[buf],
        get_page_buffer_meta=lambda idx: (
            [buf.data_ptr() + i * page_bytes for i in idx],
            [page_bytes] * len(idx),
        ),
    )


def test_v4_host_pool_group_registers_all_pools(cluster):
    """DeepSeek-V4 HiCache passes a HostPoolGroup; every pool (anchor + sidecars)
    must land in registered_pools so v2 batch ops can resolve them by name."""
    a, b = cluster
    page, n = 4096, 8
    anchor = _MemPoolHost(page, n)
    side_names = ["swa", "deepseek_v4_c4", "deepseek_v4_c4_indexer",
                  "deepseek_v4_c128", "deepseek_v4_c4_state",
                  "deepseek_v4_c4_indexer_state"]
    entries = [_HPGEntry("kv", anchor, is_anchor=True)]
    entries += [_HPGEntry(name, _side_pool(page, n)) for name in side_names]
    group = _HostPoolGroup(entries)

    a.register_mem_pool_host(group)
    b.register_mem_pool_host(group)

    # Anchor pool became mem_pool_host; every side pool is registered by name.
    assert a.mem_pool_host is anchor
    for name in ["kv"] + side_names:
        assert name in a.registered_pools, f"pool {name} not registered"
        assert b.registered_pools.get(name) is not None

    # KV round-trip through the group-registered pools.
    keys = [f"v4g{i}" for i in range(4)]
    res = a.batch_set_v2([
        SimpleNamespace(name="kv", keys=keys, host_indices=list(range(4))),
        SimpleNamespace(name="deepseek_v4_c4", keys=keys, host_indices=list(range(4))),
        SimpleNamespace(name="deepseek_v4_c128", keys=keys, host_indices=list(range(4))),
    ])
    assert all(res.get("kv")) and all(res.get("deepseek_v4_c4")) and all(res.get("deepseek_v4_c128"))
    res = b.batch_get_v2([
        SimpleNamespace(name="kv", keys=keys, host_indices=list(range(4))),
        SimpleNamespace(name="deepseek_v4_c4", keys=keys, host_indices=list(range(4))),
        SimpleNamespace(name="deepseek_v4_c128", keys=keys, host_indices=list(range(4))),
    ])
    assert all(res.get("kv")) and all(res.get("deepseek_v4_c4")) and all(res.get("deepseek_v4_c128"))


def test_config_parses_interface_v1_and_known_keys():
    """interface_v1 (a pass-through flag sglang reads to pick the zero-copy v1
    path) must survive from_extra_config without being dropped."""
    from peercache.config import PeerCacheConfig

    cfg = PeerCacheConfig.from_extra_config({
        "discovery_addr": "127.0.0.1:31998",
        "interface_v1": 1,
    })
    assert cfg.interface_v1 is True
    # Unknown keys are ignored; interface_v1 is accepted (not dropped silently).
    cfg2 = PeerCacheConfig.from_extra_config({
        "discovery_addr": "127.0.0.1:31998",
        "interface_v1": 0,
    })
    assert cfg2.interface_v1 is False


def test_logical_anchor_without_backing_buffer_is_tolerated(cluster):
    """DeepSeek-V4 real registration: sglang passes the anchor *host pool* of a
    HostPoolGroup — a LogicalHostPool whose kv_buffer is None — to
    register_mem_pool_host, then registers each backing side pool via
    register_mem_host_pool_v2. PeerCache must tolerate the no-buffer anchor
    (no recv MR, but a published pool is still created) and keep the
    v2-registered pools fully usable."""
    a, b = cluster
    page, n = 4096, 64

    class _LogicalAnchor:
        """Mirror sglang LogicalHostPool: owns page indices, no backing buffer."""

        def __init__(self, page_bytes, num_pages):
            self.kv_buffer = None  # pure-logical anchor
            self.page_bytes = page_bytes
            self._buf = _Buf(page_bytes * num_pages)

        def get_page_buffer_meta(self, host_indices):
            base = self._buf.data_ptr()
            return ([base + i * self.page_bytes for i in host_indices],
                    [self.page_bytes] * len(host_indices))

    anchor_a = _LogicalAnchor(page, n)
    anchor_b = _LogicalAnchor(page, n)
    for s in (a, b):
        s.register_mem_pool_host(anchor_a if s is a else anchor_b)

    # Logical anchor tolerated: mem_pool_host set, no crash, published pool made.
    assert a.mem_pool_host is anchor_a
    assert a._pool is not None and a._pool.capacity > 0

    # Backing side pools registered via v2 and fully usable.
    side = ["swa", "deepseek_v4_c4", "deepseek_v4_c128"]
    for s in (a, b):
        for name in side:
            s.register_mem_host_pool_v2(_MemPoolHost(page, n), name)
    keys = [f"lan{i}" for i in range(4)]
    for name in side:
        t = SimpleNamespace(name=name, keys=keys, host_indices=list(range(4)))
        assert all(a.batch_set_v2([t])[name])
        assert all(b.batch_get_v2([t])[name])

    # v1 ops on the logical anchor degrade gracefully (no crash), not a bare assert.
    assert all(not x for x in a.batch_set_v1(keys, list(range(4))))
    assert all(not x for x in b.batch_get_v1(keys, list(range(4))))


