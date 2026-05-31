"""The meta service is embedded: the node whose IP == discovery_addr hosts it.

This test starts a single node pointing discovery_addr at itself, with no
separately-launched meta server, and verifies it self-elects as meta and can
publish + read back through the directory.
"""

import ctypes
import socket
import time
from types import SimpleNamespace

import pytest

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

    def page_bytes_at(self, idx):
        return (ctypes.c_byte * self.page_bytes).from_address(
            self.kv_buffer.data_ptr() + idx * self.page_bytes
        )


def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _wait_ring(runtime, n, timeout=8.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if len(runtime.ring) >= n:
            return
        time.sleep(0.05)
    raise TimeoutError


def test_node_self_elects_as_meta_and_serves_itself():
    meta_port = _free_port()
    cfg = SimpleNamespace(
        tp_rank=0, tp_size=1, pp_rank=0, pp_size=1, is_mla_model=True,
        extra_config={
            "discovery_addr": f"127.0.0.1:{meta_port}",
            "protocol": "tcp",
            "local_hostname": "127.0.0.1",
            "node_id": "solo",
            "heartbeat_interval": 0.2,
            "member_ttl": 30.0,
            "global_segment_size": 1 << 20,
        },
    )
    store = PeerCacheStore(cfg)
    try:
        # No separate meta server was started; this node must host it.
        assert store.runtime.is_meta is True
        _wait_ring(store.runtime, 1)

        page, n = 128, 4
        host = FakeMemPoolHost(page, n)
        store.register_mem_pool_host(host)
        for i in range(n):
            seg = host.page_bytes_at(i)
            for j in range(page):
                seg[j] = (i + j) % 251
        snapshot = [bytes(host.page_bytes_at(i)) for i in range(n)]

        keys = [f"k{i}" for i in range(n)]
        assert store.batch_set_v1(keys, list(range(n))) == [True] * n

        # Zero the buffer, then read back from the published pool (local memcpy).
        for i in range(n):
            seg = host.page_bytes_at(i)
            for j in range(page):
                seg[j] = 0
        assert store.batch_get_v1(keys, list(range(n))) == [True] * n
        for i in range(n):
            assert bytes(host.page_bytes_at(i)) == snapshot[i]

        assert store.batch_exists(keys) == n
    finally:
        store.close()
