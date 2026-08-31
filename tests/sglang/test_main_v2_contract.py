"""Validate PeerCacheStore against sglang MAIN-branch v2 interfaces.

sglang releases (e.g. 0.5.9) ship only the v1 HiCacheStorage surface; the v2
interfaces (PoolName / PoolTransfer / PoolTransferResult / PoolHitPolicy) and
the DeepSeek-V4 / DRAFT pool names live on the main branch. This test loads
the main-branch `hicache_storage.py` in place of the release's module and
drives PeerCache's v2 + sidecar paths against those REAL main-branch types.

The main-branch file is fetched from GitHub at test time (or supplied via
SGLANG_MAIN_HICACHE_STORAGE); the rest of sglang comes from the installed
release, which is enough because PeerCache only touches the storage module.

Usage:
    pytest tests/sglang/test_main_v2_contract.py -v
"""
import ctypes
import importlib.util
import os
import sys
import time
import urllib.request
from types import SimpleNamespace

import pytest

pytest.importorskip("peercache.store")
pytest.importorskip("sglang")
from peercache.store import PeerCacheStore  # noqa: E402

MAIN_URL = ("https://raw.githubusercontent.com/sgl-project/sglang/main/"
            "python/sglang/srt/mem_cache/hicache_storage.py")


@pytest.fixture(scope="module")
def main_hs(tmp_path_factory):
    """Load the main-branch hicache_storage.py and swap it into sys.modules."""
    local = os.environ.get("SGLANG_MAIN_HICACHE_STORAGE")
    if local and os.path.exists(local):
        path = local
    else:
        path = tmp_path_factory.mktemp("sglang-main") / "hicache_storage.py"
        with urllib.request.urlopen(MAIN_URL, timeout=30) as r:
            path.write_bytes(r.read())
    spec = importlib.util.spec_from_file_location("hicache_storage_main", str(path))
    mod = importlib.util.module_from_spec(spec)
    sys.modules["hicache_storage_main"] = mod
    spec.loader.exec_module(mod)
    # Install in place of the release module so PeerCache's lazy imports
    # resolve to the main-branch types.
    sys.modules["sglang.srt.mem_cache.hicache_storage"] = mod
    return mod


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
        extra_config={"discovery_addr": addr, "protocol": "tcp", "device_name": "",
                      "local_hostname": "127.0.0.1", "node_id": node_id,
                      "heartbeat_interval": 0.2, "member_ttl": 30.0,
                      "global_segment_size": 8 << 20, "metrics_enabled": False},
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


def test_main_has_v2_types(main_hs):
    for name in ("PoolName", "PoolTransfer", "PoolTransferResult", "PoolHitPolicy"):
        assert hasattr(main_hs, name), "main hicache_storage missing %s" % name
    # DeepSeek-V4 / draft pool names exist on main.
    names = {v.value for v in main_hs.PoolName}
    assert "deepseek_v4_c4" in names
    assert "deepseek_v4_c128_state" in names
    assert "draft" in names


def test_main_v2_kv_pool_roundtrip(main_hs, cluster):
    PoolName, PoolTransfer = main_hs.PoolName, main_hs.PoolTransfer
    a, b = cluster
    page, n = 4096, 64
    a.register_mem_pool_host(_MemPoolHost(page, n))
    b.register_mem_pool_host(_MemPoolHost(page, n))
    keys = ["main%d" % i for i in range(4)]
    t = PoolTransfer(name=PoolName.KV, keys=keys, host_indices=list(range(4)))
    assert all(a.batch_set_v2([t])[PoolName.KV])
    res = b.batch_get_v2([PoolTransfer(name=PoolName.KV, keys=keys, host_indices=list(range(4)))])
    assert all(res[PoolName.KV])


def test_main_v2_indexer_sidecar_dsa(main_hs, cluster):
    """DSA: INDEXER sidecar pool with ALL_PAGES hit policy."""
    PoolName, PoolTransfer, PoolHitPolicy, PoolTransferResult = (
        main_hs.PoolName, main_hs.PoolTransfer, main_hs.PoolHitPolicy, main_hs.PoolTransferResult)
    a, b = cluster
    page, n = 4096, 64
    a.register_mem_pool_host(_MemPoolHost(page, n))
    b.register_mem_pool_host(_MemPoolHost(page, n))
    a.register_mem_host_pool_v2(_MemPoolHost(page, n), "indexer")
    b.register_mem_host_pool_v2(_MemPoolHost(page, n), "indexer")
    keys = ["dsa%d" % i for i in range(4)]
    assert all(a.batch_set_v2([PoolTransfer(name=PoolName.KV, keys=keys, host_indices=list(range(4)))])[PoolName.KV])
    assert all(a.batch_set_v2([PoolTransfer(name=PoolName.INDEXER, keys=keys, host_indices=list(range(4)))])[PoolName.INDEXER])
    out = b.batch_exists_v2(keys, pool_transfers=[
        PoolTransfer(name=PoolName.INDEXER, keys=keys, host_indices=list(range(4)),
                     hit_policy=PoolHitPolicy.ALL_PAGES)])
    assert isinstance(out, PoolTransferResult)
    assert out.kv_hit_pages == 4
    assert out.extra_pool_hit_pages[PoolName.INDEXER] == 4


def test_main_v2_mamba_trailing_pages(main_hs, cluster):
    """Mamba/SWA: sidecar pool with TRAILING_PAGES hit policy."""
    PoolName, PoolTransfer, PoolHitPolicy = (
        main_hs.PoolName, main_hs.PoolTransfer, main_hs.PoolHitPolicy)
    a, b = cluster
    page, n = 4096, 64
    a.register_mem_pool_host(_MemPoolHost(page, n))
    b.register_mem_pool_host(_MemPoolHost(page, n))
    a.register_mem_host_pool_v2(_MemPoolHost(page, n), "mamba")
    b.register_mem_host_pool_v2(_MemPoolHost(page, n), "mamba")
    keys = ["mam%d" % i for i in range(4)]
    assert all(a.batch_set_v2([PoolTransfer(name=PoolName.KV, keys=keys, host_indices=list(range(4)))])[PoolName.KV])
    m_keys = keys[-2:]
    assert all(a.batch_set_v2([PoolTransfer(name=PoolName.MAMBA, keys=m_keys, host_indices=[2, 3])])[PoolName.MAMBA])
    out = b.batch_exists_v2(keys, pool_transfers=[
        PoolTransfer(name=PoolName.MAMBA, keys=m_keys, host_indices=[2, 3],
                     hit_policy=PoolHitPolicy.TRAILING_PAGES)])
    assert out.kv_hit_pages == 4


class _MultiBufHostPool:
    """Host pool whose get_page_buffer_meta returns N buffers per page index,
    mimicking DeepSeek-V4 compressed pools (C4 = 2 buffers, C128 = 3)."""

    def __init__(self, page_bytes, num_pages, bufs_per_page):
        self.page_bytes = page_bytes
        self.bufs_per_page = bufs_per_page
        self.bufs = [_Buf(page_bytes * bufs_per_page) for _ in range(num_pages)]

    def get_page_buffer_meta(self, host_indices):
        ptrs, sizes = [], []
        for idx in host_indices:
            b = self.bufs[idx]
            base = b.data_ptr()
            for k in range(self.bufs_per_page):
                ptrs.append(base + k * self.page_bytes)
                sizes.append(self.page_bytes)
        return ptrs, sizes


def test_main_v2_deepseek_v4_multibuffer_pools(main_hs, cluster):
    """DeepSeek-V4: compressed pools (C4/C128/STATE) are page-packed
    single-object pools where one logical page spans multiple host buffers.
    batch_set_v2 / batch_get_v2 must pack+scatter transparently."""
    PoolName, PoolTransfer = main_hs.PoolName, main_hs.PoolTransfer
    a, b = cluster
    page, n = 4096, 8
    a.register_mem_pool_host(_MemPoolHost(page, n))
    b.register_mem_pool_host(_MemPoolHost(page, n))
    # C4 pool: 2 buffers per page; C128: 3 buffers per page.
    c4a = _MultiBufHostPool(page, n, 2)
    c4b = _MultiBufHostPool(page, n, 2)
    c128a = _MultiBufHostPool(page, n, 3)
    c128b = _MultiBufHostPool(page, n, 3)
    a.register_mem_host_pool_v2(c4a, "deepseek_v4_c4")
    b.register_mem_host_pool_v2(c4b, "deepseek_v4_c4")
    a.register_mem_host_pool_v2(c128a, "deepseek_v4_c128")
    b.register_mem_host_pool_v2(c128b, "deepseek_v4_c128")

    keys = ["v4k%d" % i for i in range(4)]
    assert all(a.batch_set_v2([PoolTransfer(name=PoolName.KV, keys=keys, host_indices=list(range(4)))])[PoolName.KV])

    # Fill C4 page buffers with distinct content, write, then read back on B.
    c4 = PoolTransfer(name=PoolName.DEEPSEEK_V4_C4, keys=keys, host_indices=list(range(4)))
    for i in range(4):
        buf = c4a.bufs[i]
        ctypes.memset(buf.data_ptr(), 40 + i, buf.numel())
    assert all(a.batch_set_v2([c4])[PoolName.DEEPSEEK_V4_C4])

    c128 = PoolTransfer(name=PoolName.DEEPSEEK_V4_C128, keys=keys, host_indices=list(range(4)))
    for i in range(4):
        buf = c128a.bufs[i]
        ctypes.memset(buf.data_ptr(), 80 + i, buf.numel())
    assert all(a.batch_set_v2([c128])[PoolName.DEEPSEEK_V4_C128])

    # Cross-node read-back on B: data must land in B's per-page buffers.
    res = b.batch_get_v2([
        PoolTransfer(name=PoolName.DEEPSEEK_V4_C4, keys=keys, host_indices=list(range(4))),
        PoolTransfer(name=PoolName.DEEPSEEK_V4_C128, keys=keys, host_indices=list(range(4))),
    ])
    assert all(res[PoolName.DEEPSEEK_V4_C4])
    assert all(res[PoolName.DEEPSEEK_V4_C128])
    for i in range(4):
        assert c4b.bufs[i]._b[0] == 40 + i, "C4 buffer %d content mismatch" % i
        assert c128b.bufs[i]._b[0] == 80 + i, "C128 buffer %d content mismatch" % i
    print("DeepSeek-V4 multi-buffer C4/C128 cross-node roundtrip OK")


class _MHAHostPool(_MemPoolHost):
    """MHA-style host pool exposing a v_buffer (K/V split) for draft tests.
    get_page_buffer_meta returns K,V interleaved per page (matching the
    `_k`,`_v` component-key order)."""

    def __init__(self, page_bytes, num_pages):
        super().__init__(page_bytes, num_pages)
        self.v_buffer = _Buf(page_bytes * num_pages)

    def get_page_buffer_meta(self, host_indices):
        k_base = self.kv_buffer.data_ptr()
        v_base = self.v_buffer.data_ptr()
        ptrs, sizes = [], []
        for idx in host_indices:
            ptrs.append(k_base + idx * self.page_bytes)
            sizes.append(self.page_bytes)
            ptrs.append(v_base + idx * self.page_bytes)
            sizes.append(self.page_bytes)
        return ptrs, sizes


def test_main_v2_draft_pools_mha_and_mla(main_hs, cluster):
    """Speculative decoding: DRAFT / DRAFT_INDEXER / DRAFT_SWA pools.

    The draft model's layout is independent of the target: an MHA draft pool
    (has v_buffer) must use `_k`+`_v` double components, an MLA draft pool a
    single `_k` component (mirrors mooncake's suffix scheme).
    """
    PoolName, PoolTransfer = main_hs.PoolName, main_hs.PoolTransfer
    a, b = cluster
    page, n = 4096, 8
    a.register_mem_pool_host(_MemPoolHost(page, n))
    b.register_mem_pool_host(_MemPoolHost(page, n))

    # MHA draft pool on node A/B.
    da = _MHAHostPool(page, n)
    db = _MHAHostPool(page, n)
    a.register_mem_host_pool_v2(da, "draft")
    b.register_mem_host_pool_v2(db, "draft")

    keys = ["drf%d" % i for i in range(4)]
    # Fill K and V pages with distinct content.
    for i in range(4):
        ctypes.memset(da.kv_buffer.data_ptr() + i * page, 10 + i, page)
        ctypes.memset(da.v_buffer.data_ptr() + i * page, 20 + i, page)

    t = PoolTransfer(name=PoolName.DRAFT, keys=keys, host_indices=list(range(4)))
    res = a.batch_set_v2([t])
    assert all(res[PoolName.DRAFT])

    # Cross-node read-back on B.
    res = b.batch_get_v2([PoolTransfer(name=PoolName.DRAFT, keys=keys, host_indices=list(range(4)))])
    assert all(res[PoolName.DRAFT])
    for i in range(4):
        assert db.kv_buffer._b[i * page] == 10 + i, "draft K content mismatch"
        assert db.v_buffer._b[i * page] == 20 + i, "draft V content mismatch"
    print("DRAFT MHA K/V cross-node roundtrip OK")

    # MLA draft pool: single component.
    a.register_mem_host_pool_v2(_MemPoolHost(page, n), "draft")
    b.register_mem_host_pool_v2(_MemPoolHost(page, n), "draft")
    # Re-register as MLA-style (no v_buffer) by replacing the pool object; the
    # suffix logic keys off `hasattr(pool, 'v_buffer')`.
    a.registered_pools["draft"] = _MemPoolHost(page, n)
    b.registered_pools["draft"] = _MemPoolHost(page, n)
    t2 = PoolTransfer(name=PoolName.DRAFT, keys=keys, host_indices=list(range(4)))
    res = a.batch_set_v2([t2])
    assert all(res[PoolName.DRAFT])
    res = b.batch_get_v2([PoolTransfer(name=PoolName.DRAFT, keys=keys, host_indices=list(range(4)))])
    assert all(res[PoolName.DRAFT])
    print("DRAFT MLA single-component roundtrip OK")

