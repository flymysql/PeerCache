"""End-to-end tests for centralized (non-P2P) mode over TCP."""

import ctypes
import time
from types import SimpleNamespace

import pytest

from peercache.config import PeerCacheConfig
from peercache.discovery import DiscoveryServer
from peercache.storage_server import StorageServer
from peercache.store import PeerCacheStore


class FakeKVBuffer:
    def __init__(self, nbytes):
        self._b = (ctypes.c_byte * nbytes)()

    def data_ptr(self):
        return ctypes.addressof(self._b)

    def numel(self):
        return len(self._b)

    def element_size(self):
        return 1


class FakeMemPoolHost:
    def __init__(self, page_bytes, num_pages):
        self.page_bytes = page_bytes
        self.kv_buffer = FakeKVBuffer(page_bytes * num_pages)

    def get_page_buffer_meta(self, host_indices):
        base = self.kv_buffer.data_ptr()
        ptrs = [base + i * self.page_bytes for i in host_indices]
        sizes = [self.page_bytes] * len(host_indices)
        return ptrs, sizes

    def page_bytes_at(self, idx):
        return (ctypes.c_byte * self.page_bytes).from_address(
            self.kv_buffer.data_ptr() + idx * self.page_bytes
        )


def _storage_cfg(discovery_addr, node_id):
    return PeerCacheConfig(
        discovery_addr=discovery_addr,
        mode="centralized",
        role="storage",
        protocol="tcp",
        local_hostname="127.0.0.1",
        node_id=node_id,
        heartbeat_interval=0.2,
        member_ttl=30.0,
        global_segment_size=1 << 20,
        metrics_enabled=False,
        disk_enabled=False,
    )


def _infer_cfg(discovery_addr, node_id):
    return SimpleNamespace(
        tp_rank=0,
        tp_size=1,
        pp_rank=0,
        pp_size=1,
        is_mla_model=True,
        extra_config={
            "discovery_addr": discovery_addr,
            "mode": "centralized",
            "role": "inference",
            "protocol": "tcp",
            "local_hostname": "127.0.0.1",
            "node_id": node_id,
            "heartbeat_interval": 0.2,
            "member_ttl": 30.0,
            "metrics_enabled": False,
            "disk_enabled": False,
        },
    )


def _wait_ring(runtime, n, timeout=8.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if len(runtime.ring) >= n:
            return
        time.sleep(0.05)
    raise TimeoutError(f"ring did not reach {n} nodes")


def _wait_storage(runtime, n, timeout=8.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if len(runtime.storage_nodes()) >= n:
            return
        time.sleep(0.05)
    raise TimeoutError(f"storage ring did not reach {n} nodes")


@pytest.fixture
def centralized_cluster():
    meta = DiscoveryServer("127.0.0.1", 0)
    port = meta.start()
    addr = f"127.0.0.1:{port}"
    storage = StorageServer(_storage_cfg(addr, "storage-1"))
    storage.start()
    a = PeerCacheStore(_infer_cfg(addr, "infer-A"))
    b = PeerCacheStore(_infer_cfg(addr, "infer-B"))
    try:
        _wait_ring(a.runtime, 3)
        _wait_storage(a.runtime, 1)
        _wait_storage(b.runtime, 1)
        yield storage, a, b
    finally:
        b.close()
        a.close()
        storage.stop()
        meta.stop()


def test_centralized_write_on_infer_read_on_peer(centralized_cluster):
    storage, a, b = centralized_cluster
    page, npages = 256, 8

    host_a = FakeMemPoolHost(page, npages)
    a.register_mem_pool_host(host_a)
    host_b = FakeMemPoolHost(page, npages)
    b.register_mem_pool_host(host_b)

    for i in range(npages):
        seg = host_a.page_bytes_at(i)
        for j in range(page):
            seg[j] = (i * 11 + j) % 251

    keys = [f"ck{i}" for i in range(npages)]

    # Inference node A writes to the storage server (not its local pool).
    assert a.batch_set_v1(keys, list(range(npages))) == [True] * npages
    assert a._pool is None  # client-only: no local published pool

    # Inference node B reads from storage via RDMA/TCP READ.
    assert b.batch_get_v1(keys, list(range(npages))) == [True] * npages

    for i in range(npages):
        assert bytes(host_a.page_bytes_at(i)) == bytes(host_b.page_bytes_at(i))

    assert a.batch_exists(keys) == npages
    assert b.batch_exists(keys) == npages
    assert len(storage._pool) == npages


def test_centralized_requires_storage_nodes():
    meta = DiscoveryServer("127.0.0.1", 0)
    port = meta.start()
    addr = f"127.0.0.1:{port}"
    infer = PeerCacheStore(_infer_cfg(addr, "lonely"))
    try:
        _wait_storage(infer.runtime, 1, timeout=0.5)
        pytest.fail("expected timeout waiting for storage")
    except TimeoutError:
        pass
    host = FakeMemPoolHost(128, 2)
    infer.register_mem_pool_host(host)
    assert infer.batch_set_v1(["x"], [0]) == [False]
    infer.close()
    meta.stop()
