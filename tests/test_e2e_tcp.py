"""End-to-end test of the full backend over the TCP fallback transport.

Validates discovery -> consistent-hash directory -> published pool -> remote read
across two PeerCacheStore nodes in one process, with no RDMA hardware.
"""

import ctypes
import time
from types import SimpleNamespace

import pytest

from peercache.discovery import DiscoveryServer
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
    """Minimal stand-in for SGLang's HostKVCache (MLA-style: one object/page)."""

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


def _make_cfg(discovery_addr, node_id):
    return SimpleNamespace(
        tp_rank=0,
        tp_size=1,
        pp_rank=0,
        pp_size=1,
        is_mla_model=True,  # one object per page -> simpler assertions
        extra_config={
            "discovery_addr": discovery_addr,
            "protocol": "tcp",
            "local_hostname": "127.0.0.1",
            "node_id": node_id,
            "heartbeat_interval": 0.2,
            "member_ttl": 30.0,
            "global_segment_size": 1 << 20,
        },
    )


def _wait_ring(runtime, n, timeout=8.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if len(runtime.ring) >= n:
            return
        time.sleep(0.05)
    raise TimeoutError(f"ring did not reach {n} nodes")


@pytest.fixture
def cluster():
    meta = DiscoveryServer("127.0.0.1", 0)
    port = meta.start()
    addr = f"127.0.0.1:{port}"
    a = PeerCacheStore(_make_cfg(addr, "A"))
    b = PeerCacheStore(_make_cfg(addr, "B"))
    try:
        _wait_ring(a.runtime, 2)
        _wait_ring(b.runtime, 2)
        yield a, b
    finally:
        a.close()
        b.close()
        meta.stop()


def test_cross_node_write_then_read(cluster):
    a, b = cluster
    page, npages = 256, 8

    host_a = FakeMemPoolHost(page, npages)
    a.register_mem_pool_host(host_a)
    host_b = FakeMemPoolHost(page, npages)
    b.register_mem_pool_host(host_b)

    # Fill node A's pages with distinct, known data.
    for i in range(npages):
        seg = host_a.page_bytes_at(i)
        for j in range(page):
            seg[j] = (i * 7 + j) % 251

    keys = [f"key{i}" for i in range(npages)]

    # A publishes (data stays in A's pool; only locations go to the directory).
    assert a.batch_set_v1(keys, list(range(npages))) == [True] * npages

    # B reads them back into its own (initially zeroed) host buffer.
    assert b.batch_get_v1(keys, list(range(npages))) == [True] * npages

    for i in range(npages):
        assert bytes(host_a.page_bytes_at(i)) == bytes(host_b.page_bytes_at(i))

    # Both nodes observe existence through the distributed directory.
    assert a.batch_exists(keys) == npages
    assert b.batch_exists(keys) == npages


def test_missing_keys_report_miss(cluster):
    a, b = cluster
    a.register_mem_pool_host(FakeMemPoolHost(128, 4))
    host_b = FakeMemPoolHost(128, 4)
    b.register_mem_pool_host(host_b)

    keys = ["nope-0", "nope-1"]
    assert b.batch_get_v1(keys, [0, 1]) == [False, False]
    assert b.batch_exists(keys) == 0
