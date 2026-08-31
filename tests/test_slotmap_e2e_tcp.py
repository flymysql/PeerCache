"""End-to-end test of the directory-free slot-map placement (mode=slotmap).

Validates discovery -> consistent-hash ring -> deterministic slot region ->
one-sided remote read across two PeerCacheStore nodes in one process, over the
TCP fallback transport (no RDMA hardware, no directory service).

The key guarantees under test:
  * a page written on one node is readable on the other purely by hashing the
    key (no directory lookup);
  * a key that was never written is a CLEAN miss, never a dirty hit.
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
            "mode": "slotmap",
            "slot_max_page_bytes": 4096,
            "slot_ways": 4,
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


def test_slotmap_cross_node_write_then_read(cluster):
    a, b = cluster
    page, npages = 256, 16

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

    # A writes: each page goes to hash(key)'s owner slot (A's own or B's),
    # directory-free.
    assert a.batch_set_v1(keys, list(range(npages))) == [True] * npages

    # B reads them back purely by hashing -- no directory lookup.
    assert b.batch_get_v1(keys, list(range(npages))) == [True] * npages

    for i in range(npages):
        assert bytes(host_a.page_bytes_at(i)) == bytes(host_b.page_bytes_at(i))


def test_slotmap_missing_keys_are_clean_miss(cluster):
    a, b = cluster
    a.register_mem_pool_host(FakeMemPoolHost(128, 4))
    host_b = FakeMemPoolHost(128, 4)
    b.register_mem_pool_host(host_b)

    keys = ["never-written-0", "never-written-1"]
    # A read of a key that was never written must be a clean miss (never a
    # dirty hit from a stale / colliding slot).
    assert b.batch_get_v1(keys, [0, 1]) == [False, False]


def test_slotmap_no_dirty_hit_on_len_mismatch(cluster):
    """A key written at one length, then read expecting another, is a miss."""
    a, b = cluster
    host_a = FakeMemPoolHost(256, 4)
    a.register_mem_pool_host(host_a)
    # B expects a different page size -> the slot header length gate rejects it.
    host_b = FakeMemPoolHost(128, 4)
    b.register_mem_pool_host(host_b)

    for j in range(256):
        host_a.page_bytes_at(0)[j] = j % 251
    a.batch_set_v1(["k0"], [0])
    # B reads 128 bytes for a key stored as 256 -> header length mismatch -> miss.
    assert b.batch_get_v1(["k0"], [0]) == [False]


def test_slotmap_many_keys_full_hit_rate(cluster):
    """Write a key set spanning both nodes and require EVERY key to read back.

    This exercises the remote read-modify-write way selection AND the intra-batch
    reservation: many keys land in the same buckets, so a naive fixed-way writer
    (or one that ignores same-batch peers) would evict and miss. The key count is
    kept comfortably under bucket capacity (63 buckets x 4 ways) so 100% is the
    correct expectation; capacity-overflow eviction is covered separately."""
    a, b = cluster
    page, npages = 256, 120
    host_a = FakeMemPoolHost(page, npages)
    a.register_mem_pool_host(host_a)
    host_b = FakeMemPoolHost(page, npages)
    b.register_mem_pool_host(host_b)

    for i in range(npages):
        seg = host_a.page_bytes_at(i)
        for j in range(page):
            seg[j] = (i * 31 + j) % 251

    keys = [f"page-{i}" for i in range(npages)]
    assert a.batch_set_v1(keys, list(range(npages))) == [True] * npages

    got = b.batch_get_v1(keys, list(range(npages)))
    hits = sum(got)
    assert hits == npages, f"only {hits}/{npages} keys read back"
    for i in range(npages):
        assert bytes(host_a.page_bytes_at(i)) == bytes(host_b.page_bytes_at(i))


def test_slotmap_capacity_overflow_is_clean(cluster):
    """When a bucket overflows (more colliding keys than ways), the losers are
    evicted -> clean misses, and every reported hit still returns correct data
    (never a dirty hit)."""
    a, b = cluster
    page, npages = 128, 400  # deliberately over 63*4 capacity
    host_a = FakeMemPoolHost(page, npages)
    a.register_mem_pool_host(host_a)
    host_b = FakeMemPoolHost(page, npages)
    b.register_mem_pool_host(host_b)

    for i in range(npages):
        seg = host_a.page_bytes_at(i)
        for j in range(page):
            seg[j] = (i * 13 + j) % 251

    keys = [f"ovf-{i}" for i in range(npages)]
    a.batch_set_v1(keys, list(range(npages)))
    got = b.batch_get_v1(keys, list(range(npages)))
    # Some keys are legitimately evicted, but every hit must be byte-correct.
    for i, ok in enumerate(got):
        if ok:
            assert bytes(host_a.page_bytes_at(i)) == bytes(host_b.page_bytes_at(i)), (
                f"dirty hit at {keys[i]}"
            )
    assert sum(got) > 0  # under capacity pressure we still serve a good fraction



def test_slotmap_overwrite_same_key(cluster):
    """Re-writing a key updates its slot in place (same way), read sees new data."""
    a, b = cluster
    page = 256
    host_a = FakeMemPoolHost(page, 2)
    a.register_mem_pool_host(host_a)
    host_b = FakeMemPoolHost(page, 2)
    b.register_mem_pool_host(host_b)

    for j in range(page):
        host_a.page_bytes_at(0)[j] = 11
    assert a.batch_set_v1(["hot"], [0]) == [True]
    assert b.batch_get_v1(["hot"], [0]) == [True]
    assert bytes(host_b.page_bytes_at(0))[0] == 11

    # Overwrite with new content; the reader must see the fresh page.
    for j in range(page):
        host_a.page_bytes_at(1)[j] = 99
    assert a.batch_set_v1(["hot"], [1]) == [True]
    assert b.batch_get_v1(["hot"], [1]) == [True]
    assert bytes(host_b.page_bytes_at(1))[0] == 99

