"""Hybrid cluster: P2P inference + storage servers in one discovery group."""

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


def _infer_cfg(discovery_addr, node_id, mode="p2p"):
    return SimpleNamespace(
        tp_rank=0,
        tp_size=1,
        pp_rank=0,
        pp_size=1,
        is_mla_model=True,
        extra_config={
            "discovery_addr": discovery_addr,
            "mode": mode,
            "protocol": "tcp",
            "local_hostname": "127.0.0.1",
            "node_id": node_id,
            "heartbeat_interval": 0.2,
            "member_ttl": 30.0,
            "global_segment_size": 1 << 20,
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
def hybrid_cluster():
    meta = DiscoveryServer("127.0.0.1", 0)
    port = meta.start()
    addr = f"127.0.0.1:{port}"
    storage = StorageServer(_storage_cfg(addr, "storage-1"))
    storage.start()
    p2p = PeerCacheStore(_infer_cfg(addr, "p2p-A", mode="p2p"))
    hybrid = PeerCacheStore(_infer_cfg(addr, "hybrid-B", mode="hybrid"))
    try:
        _wait_ring(p2p.runtime, 3)
        _wait_storage(p2p.runtime, 1)
        _wait_storage(hybrid.runtime, 1)
        yield storage, p2p, hybrid
    finally:
        hybrid.close()
        p2p.close()
        storage.stop()
        meta.stop()


def test_p2p_and_hybrid_coexist(hybrid_cluster):
    storage, p2p, hybrid = hybrid_cluster
    page, npages = 128, 4

    for store, prefix in ((p2p, "p2p"), (hybrid, "hyb")):
        host = FakeMemPoolHost(page, npages)
        store.register_mem_pool_host(host)
        keys = [f"{prefix}{i}" for i in range(npages)]
        for i in range(npages):
            seg = host.page_bytes_at(i)
            for j in range(page):
                seg[j] = (i * 3 + j) % 251
        assert store.batch_set_v1(keys, list(range(npages))) == [True] * npages

    # P2P node keeps a local published pool; hybrid also keeps one.
    assert p2p._pool is not None
    assert hybrid._pool is not None

    # Hybrid writes land on the storage server; pure P2P keys stay off storage.
    assert len(storage._pool) >= npages
    assert p2p.batch_get_v1([f"p2p{i}" for i in range(npages)], list(range(npages))) == [True] * npages
    assert hybrid.batch_get_v1([f"hyb{i}" for i in range(npages)], list(range(npages))) == [True] * npages

    # Cross-read: hybrid reader pulls P2P-published keys from the P2P node.
    host_h = FakeMemPoolHost(page, npages)
    hybrid.register_mem_pool_host(host_h)
    assert hybrid.batch_get_v1([f"p2p{i}" for i in range(npages)], list(range(npages))) == [True] * npages
