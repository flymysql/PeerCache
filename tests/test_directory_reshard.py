"""Directory re-shards (and stays readable) when ring membership changes.

When a node joins, the consistent-hash owner of some keys moves to it; entries
are not migrated automatically, so each producer re-publishes the locations of
its pages onto the new owners. This verifies a consumer that joins after publish
still reads every key (the bench "re-run" scenario, as a unit test)."""

import ctypes
import time
from types import SimpleNamespace

import pytest

from peercache.discovery import DiscoveryServer
from peercache.store import PeerCacheStore


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
            "heartbeat_interval": 0.1, "member_ttl": 30.0,
            "global_segment_size": 8 << 20, "metrics_enabled": False,
            "disk_enabled": False,
        },
    )


def _wait(cond, timeout=10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return True
        time.sleep(0.05)
    return False


def test_consumer_joining_after_publish_still_reads_all():
    meta = DiscoveryServer("127.0.0.1", 0)
    addr = f"127.0.0.1:{meta.start()}"
    page, n = 4096, 64
    a = PeerCacheStore(_cfg(addr, "A"))
    try:
        # Publish while A is the only node: all entries land on A's shard.
        a.register_mem_pool_host(_MemPoolHost(page, n))
        keys = [f"k{i}" for i in range(n)]
        assert all(a.batch_set_v1(keys, list(range(n))))
        republishes_before = a._metrics._counters["directory_republishes"]

        # B joins -> ownership of ~half the keys moves to B's shard; A must
        # re-publish so those entries land on B.
        b = PeerCacheStore(_cfg(addr, "B"))
        try:
            assert _wait(lambda: len(a.runtime.ring) >= 2 and len(b.runtime.ring) >= 2)
            # A's membership listener fires a re-shard off the discovery thread.
            assert _wait(
                lambda: a._metrics._counters["directory_republishes"] > republishes_before
            ), "producer did not re-publish after the consumer joined"
            b.register_mem_pool_host(_MemPoolHost(page, n))
            # Every key resolves and reads back, even those now owned by B.
            assert _wait(lambda: b.batch_exists(keys) == n)
            oks = b.batch_get_v1(keys, list(range(n)))
            assert all(oks) and len(oks) == n
        finally:
            b.close()
    finally:
        a.close()
        meta.stop()
