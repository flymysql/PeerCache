"""End-to-end: pages evicted from the memory pool spill to disk and are promoted
back into the pool (and re-published to the directory) on a later read."""

import ctypes
import socket
import time
from types import SimpleNamespace

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
        return ([base + i * self.page_bytes for i in host_indices],
                [self.page_bytes] * len(host_indices))

    def page_at(self, idx):
        return (ctypes.c_byte * self.page_bytes).from_address(
            self.kv_buffer.data_ptr() + idx * self.page_bytes
        )


def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def _wait(cond, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return True
        time.sleep(0.03)
    return False


def _cfg(addr, node_id, disk_dir, seg_bytes, metrics=False):
    return SimpleNamespace(
        tp_rank=0, tp_size=1, pp_rank=0, pp_size=1, is_mla_model=True,
        extra_config={
            "discovery_addr": addr,
            "protocol": "tcp",
            "local_hostname": "127.0.0.1",
            "node_id": node_id,
            "heartbeat_interval": 0.2,
            "member_ttl": 30.0,
            "global_segment_size": seg_bytes,
            "disk_enabled": True,
            "disk_path": disk_dir,
            "disk_size": 1 << 20,
            "metrics_enabled": metrics,
        },
    )


def test_remote_promote_from_disk_across_nodes(tmp_path):
    from peercache.discovery import DiscoveryServer

    page = 128
    meta = DiscoveryServer("127.0.0.1", 0)
    port = meta.start()
    addr = f"127.0.0.1:{port}"
    # Producer A has a small pool (holds 4 pages) so an early page spills to disk.
    a = PeerCacheStore(_cfg(addr, "A", str(tmp_path / "a"), page * 4))
    b = PeerCacheStore(_cfg(addr, "B", str(tmp_path / "b"), 1 << 20))
    try:
        _wait(lambda: len(a.runtime.ring) >= 2 and len(b.runtime.ring) >= 2)
        host_a = FakeMemPoolHost(page, 5)
        a.register_mem_pool_host(host_a)
        host_b = FakeMemPoolHost(page, 5)
        b.register_mem_pool_host(host_b)

        for i in range(5):
            seg = host_a.page_at(i)
            for j in range(page):
                seg[j] = (i * 7 + j) % 251
        snap = [bytes(host_a.page_at(i)) for i in range(5)]

        keys = [f"key{i}" for i in range(5)]
        assert a.batch_set_v1(keys, list(range(5))) == [True] * 5
        assert _wait(lambda: a._disk.stats()[1] >= 5)

        # key0 was evicted from A's pool to disk; key4 is still resident.
        # B reads both: key4 directly, key0 via a remote promote on A.
        assert b.batch_get_v1(["key0", "key4"], [0, 4]) == [True, True]
        assert bytes(host_b.page_at(0)) == snap[0]
        assert bytes(host_b.page_at(4)) == snap[4]
        assert a._metrics.snapshot()["counters"]["promotes"] >= 1
    finally:
        a.close()
        b.close()
        meta.stop()


def test_evicted_pages_spill_and_promote_from_disk(tmp_path):
    page, npages = 128, 6
    pool_pages = 4  # pool holds only 4 -> writing 6 evicts 2 to disk
    cfg = SimpleNamespace(
        tp_rank=0, tp_size=1, pp_rank=0, pp_size=1, is_mla_model=True,
        extra_config={
            "discovery_addr": f"127.0.0.1:{_free_port()}",
            "protocol": "tcp",
            "local_hostname": "127.0.0.1",
            "node_id": "solo",
            "heartbeat_interval": 0.2,
            "member_ttl": 30.0,
            "global_segment_size": page * pool_pages,
            "disk_enabled": True,
            "disk_path": str(tmp_path),
            "disk_size": 1 << 20,
            "metrics_enabled": False,
        },
    )
    store = PeerCacheStore(cfg)
    try:
        assert store.runtime.is_meta
        assert store._disk is not None
        _wait(lambda: len(store.runtime.ring) >= 1)

        host = FakeMemPoolHost(page, npages)
        store.register_mem_pool_host(host)
        for i in range(npages):
            seg = host.page_at(i)
            for j in range(page):
                seg[j] = (i * 7 + j) % 251
        snap = [bytes(host.page_at(i)) for i in range(npages)]

        keys = [f"k{i}" for i in range(npages)]
        assert store.batch_set_v1(keys, list(range(npages))) == [True] * npages

        # All pages were written-through to disk (async); wait for the spill.
        assert _wait(lambda: store._disk.stats()[1] >= npages)

        # Read back the two oldest keys (k0, k1) which were evicted to disk.
        # Zero their destination pages first, then get -> served via promote.
        for i in (0, 1):
            seg = host.page_at(i)
            for j in range(page):
                seg[j] = 0
        assert store.batch_get_v1(["k0", "k1"], [0, 1]) == [True, True]
        assert bytes(host.page_at(0)) == snap[0]
        assert bytes(host.page_at(1)) == snap[1]

        snapshot = store._metrics.snapshot()
        assert snapshot["counters"]["evictions"] >= 2
        assert snapshot["counters"]["promotes"] >= 2
        assert snapshot["counters"]["read_disk_hits"] >= 2
    finally:
        store.close()
