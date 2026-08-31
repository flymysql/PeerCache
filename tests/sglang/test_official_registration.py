"""Validate the "official backend" registration path: once the upstream PR
lands, `StorageBackendFactory.create_backend("peercache", ...)` must work
exactly like `--hicache-storage-backend peercache`.

Loads the main-branch backend_factory.py (which carries the builtin registry
and `_create_builtin_backend`), applies the two PR patches (register_backend +
_create_builtin_backend branch), then drives create_backend end-to-end.

Usage:
    pytest tests/sglang/test_official_registration.py -v
"""
import ctypes
import os
import sys
import time
import types
import urllib.request
from types import SimpleNamespace

import pytest

pytest.importorskip("peercache.store")
pytest.importorskip("sglang")

FACTORY_URL = ("https://raw.githubusercontent.com/sgl-project/sglang/main/"
               "python/sglang/srt/mem_cache/storage/backend_factory.py")


def _load_main_factory():
    """Load the main-branch backend_factory module (patched with the
    peercache registration the PR would add)."""
    local = os.environ.get("SGLANG_MAIN_BACKEND_FACTORY")
    if local and os.path.exists(local):
        src = open(local, encoding="utf-8").read()
    else:
        src = urllib.request.urlopen(FACTORY_URL, timeout=30).read().decode()

    # Apply PR patch A: register_backend("peercache", ...)
    patch = (
        'StorageBackendFactory.register_backend(\n'
        '    "peercache",\n'
        '    "peercache.store",\n'
        '    "PeerCacheStore",\n'
        ')\n'
    )
    if 'register_backend(\n    "peercache"' not in src:
        src = src.rstrip() + "\n\n" + patch

    # Apply PR patch B: _create_builtin_backend branch for peercache.
    if 'backend_name == "peercache"' not in src:
        src = src.replace(
            '        elif backend_name == "shm":\n'
            '            return backend_class(storage_config, mem_pool_host)\n'
            '        else:',
            '        elif backend_name == "shm":\n'
            '            return backend_class(storage_config, mem_pool_host)\n'
            '        elif backend_name == "peercache":\n'
            '            return backend_class(storage_config, mem_pool_host)\n'
            '        else:',
        )

    mod = types.ModuleType("backend_factory_main")
    mod.__file__ = "backend_factory_main.py"
    # The module imports HiCacheStorage/HiCacheStorageConfig from the release;
    # make sure those resolve to the release's classes.
    sys.modules["backend_factory_main"] = mod
    code = compile(src, "backend_factory_main.py", "exec")
    exec(code, mod.__dict__)
    return mod


@pytest.fixture(scope="module")
def factory():
    # Guard against test-ordering: test_main_v2_contract may have swapped
    # sys.modules["sglang.srt.mem_cache.hicache_storage"] for the main-branch
    # module. The factory's issubclass(backend_class, HiCacheStorage) check
    # must see the SAME HiCacheStorage class PeerCacheStore inherits from
    # (the release one), so restore the release module around the load.
    import sglang.srt.mem_cache.hicache_storage as release_hs
    saved = sys.modules["sglang.srt.mem_cache.hicache_storage"]
    sys.modules["sglang.srt.mem_cache.hicache_storage"] = release_hs
    try:
        return _load_main_factory()
    finally:
        sys.modules["sglang.srt.mem_cache.hicache_storage"] = saved


class _Buf:
    def __init__(self, n):
        self._b = (ctypes.c_byte * n)()

    def data_ptr(self):
        return ctypes.addressof(self._b)

    def numel(self):
        return len(self._b)

    def element_size(self):
        return 1


class _HostPool:
    def __init__(self, page_bytes, num_pages):
        self.page_bytes = page_bytes
        self.layout = "page_first"
        self.kv_buffer = _Buf(page_bytes * num_pages)
        self.page_size = 1

    def get_page_buffer_meta(self, host_indices):
        base = self.kv_buffer.data_ptr()
        return ([base + i * self.page_bytes for i in host_indices],
                [self.page_bytes] * len(host_indices))


def _make_config(discovery_addr):
    # Use SimpleNamespace instead of the sglang HiCacheStorageConfig dataclass:
    # its required fields differ across releases (0.5.9 vs main), and
    # PeerCacheStore only reads attributes off the config object.
    return SimpleNamespace(
        tp_rank=0, tp_size=1, pp_rank=0, pp_size=1, is_mla_model=True,
        is_page_first_layout=True, model_name="test",
        extra_config={
            "discovery_addr": discovery_addr, "protocol": "tcp",
            "local_hostname": "127.0.0.1", "node_id": "reg",
            "heartbeat_interval": 0.2, "member_ttl": 30.0,
            "global_segment_size": 8 << 20, "metrics_enabled": False,
        },
    )


def test_factory_registers_peercache(factory):
    assert "peercache" in factory.StorageBackendFactory._registry


def test_create_backend_peercache_builtin(factory):
    """create_backend('peercache', ...) must go through the builtin path and
    return a working PeerCacheStore (the PR's _create_builtin_backend branch)."""
    from peercache.discovery import DiscoveryServer

    meta = DiscoveryServer("127.0.0.1", 0)
    addr = "127.0.0.1:%d" % meta.start()
    pool = _HostPool(4096, 8)
    try:
        store = factory.StorageBackendFactory.create_backend(
            "peercache", _make_config(addr), pool
        )
        assert store is not None
        # It must be a real PeerCacheStore that can publish+read.
        store.register_mem_pool_host(pool)
        deadline = time.time() + 10
        while time.time() < deadline and len(store.runtime.ring) < 1:
            time.sleep(0.05)
        keys = ["off%d" % i for i in range(3)]
        assert all(store.batch_set_v1(keys, list(range(3))))
        assert store.batch_exists(keys) == 3
        assert all(store.batch_get_v1(keys, list(range(3))))
        store.close()
    finally:
        meta.stop()


def test_unknown_backend_still_raises(factory):
    """The patch must not break the unknown-backend error path."""
    with pytest.raises(ValueError):
        factory.StorageBackendFactory.create_backend(
            "definitely-not-a-backend", _make_config("127.0.0.1:1"), _HostPool(4096, 8)
        )

