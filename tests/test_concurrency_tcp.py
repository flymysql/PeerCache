"""Concurrency test over the TCP fallback transport.

Exercises many threads issuing concurrent writes (publish) on one node and
concurrent reads (remote pull) on another, validating data integrity under the
per-endpoint socket pool (data plane) and the per-endpoint RPC connection pool
(control plane). This is the pure-Python analogue of the per-peer RDMA channel
pool used on real hardware.
"""

import ctypes
import time
from concurrent.futures import ThreadPoolExecutor
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
        is_mla_model=True,
        extra_config={
            "discovery_addr": discovery_addr,
            "protocol": "tcp",
            "local_hostname": "127.0.0.1",
            "node_id": node_id,
            "heartbeat_interval": 0.2,
            "member_ttl": 30.0,
            "global_segment_size": 4 << 20,
            "max_channels_per_peer": 8,
            # Keep the test self-contained: no disk tier, no metrics port bind.
            "disk_enabled": False,
            "metrics_enabled": False,
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


def test_concurrent_write_then_concurrent_read(cluster):
    a, b = cluster
    page, npages = 512, 64

    host_a = FakeMemPoolHost(page, npages)
    a.register_mem_pool_host(host_a)
    host_b = FakeMemPoolHost(page, npages)
    b.register_mem_pool_host(host_b)

    for i in range(npages):
        seg = host_a.page_bytes_at(i)
        for j in range(page):
            seg[j] = (i * 13 + j) % 251

    keys = [f"k{i}" for i in range(npages)]

    # Concurrent publish from many threads, one page each.
    def _set(i):
        return a.batch_set_v1([keys[i]], [i]) == [True]

    with ThreadPoolExecutor(max_workers=16) as ex:
        assert all(ex.map(_set, range(npages)))

    # Concurrent reads from B, each thread pulling a distinct page.
    def _get(i):
        return b.batch_get_v1([keys[i]], [i]) == [True]

    with ThreadPoolExecutor(max_workers=16) as ex:
        assert all(ex.map(_get, range(npages)))

    for i in range(npages):
        assert bytes(host_a.page_bytes_at(i)) == bytes(host_b.page_bytes_at(i))


def test_many_threads_read_same_pages(cluster):
    """Many threads concurrently read the SAME source keys (into disjoint
    destination pages) -> more concurrent readers than free channels, stressing
    the per-peer pool's lease/release/wait path."""
    a, b = cluster
    page, nsrc, threads = 256, 8, 24

    host_a = FakeMemPoolHost(page, nsrc)
    a.register_mem_pool_host(host_a)
    a_keys = [f"s{i}" for i in range(nsrc)]
    for i in range(nsrc):
        seg = host_a.page_bytes_at(i)
        for j in range(page):
            seg[j] = (i + j) % 251
    assert a.batch_set_v1(a_keys, list(range(nsrc))) == [True] * nsrc
    expected = [bytes(host_a.page_bytes_at(i)) for i in range(nsrc)]

    # One shared destination pool; each thread owns a disjoint slice of pages.
    host_b = FakeMemPoolHost(page, nsrc * threads)
    b.register_mem_pool_host(host_b)

    def _reader(t):
        dst = list(range(t * nsrc, t * nsrc + nsrc))
        if b.batch_get_v1(a_keys, dst) != [True] * nsrc:
            return False
        return all(
            bytes(host_b.page_bytes_at(dst[i])) == expected[i] for i in range(nsrc)
        )

    with ThreadPoolExecutor(max_workers=threads) as ex:
        assert all(ex.map(_reader, range(threads)))
