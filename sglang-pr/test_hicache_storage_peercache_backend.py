"""
E2E tests for HiCache Storage with the PeerCache backend.

PeerCache (github.com/flymysql/PeerCache) is a P2P L3 KV-cache backend for
HiCache with embedded discovery, a consistent-hash distributed directory and
zero-copy one-sided RDMA READ. On CI runners (no RDMA device) the backend
falls back to its pure-Python TCP transport, which exercises the full control
plane (discovery -> directory -> published pool -> cross-node fetch).

This test launches a real sglang server with `--hicache-storage-backend
dynamic` pointing at PeerCacheStore, drives shared-prefix requests and asserts
PeerCache's Prometheus metrics show KV pages were published (write_requests /
pool_keys > 0).

Usage:
    python3 -m pytest test/registered/hicache/test_hicache_storage_peercache_backend.py -v
"""

import json
import os
import time
import unittest

import requests

from sglang.test.ci.ci_register import register_cuda_ci
from sglang.test.test_utils import (
    DEFAULT_MODEL_NAME_FOR_TEST,
    DEFAULT_TIMEOUT_FOR_SERVER_LAUNCH,
    CustomTestCase,
    is_in_ci,
    popen_launch_server,
)
from sglang.utils import wait_for_http_ready

register_cuda_ci(est_time=180, stage="base-b", runner_config="2-gpu-large")


PEERCACHE_METRICS_PORT = 31997
PEERCACHE_DISCOVERY = "127.0.0.1:31998"
PEERCACHE_EXTRA_CONFIG = (
    '{"backend_name":"peercache","module_path":"peercache.store",'
    '"class_name":"PeerCacheStore","discovery_addr":"%s","protocol":"tcp",'
    '"local_hostname":"127.0.0.1","node_id":"ci","global_segment_size":"1gb",'
    '"metrics_enabled":true}'
) % PEERCACHE_DISCOVERY

SHARED_PREFIX = (
    "The theory of general relativity, published by Albert Einstein in 1915, "
    "describes gravity as a geometric property of spacetime. Massive objects "
    "curve spacetime, and this curvature dictates the motion of other objects. "
    "Key predictions include the bending of light around massive bodies, the "
    "precession of planetary orbits, and gravitational time dilation."
)


def _peercache_metric(name):
    """Scrape a single PeerCache Prometheus counter/gauge value."""
    try:
        r = requests.get("http://127.0.0.1:%d/metrics" % PEERCACHE_METRICS_PORT,
                         timeout=5)
        for line in r.text.splitlines():
            if line.startswith(name):
                return float(line.split()[-1])
    except Exception:
        pass
    return None


class HiCacheStoragePeerCacheBackendBaseMixin:
    """Base mixin: launch sglang with the PeerCache dynamic backend once."""

    @classmethod
    def setUpClass(cls):
        cls.model = cls._get_model_name()
        cls.base_url = "http://127.0.0.1:30000"

        cls.process = popen_launch_server(
            cls.model,
            cls.base_url,
            timeout=DEFAULT_TIMEOUT_FOR_SERVER_LAUNCH,
            other_args=[
                "--enable-hierarchical-cache",
                "--hicache-write-policy", "write_through",
                "--hicache-ratio", "1.05",
                "--hicache-storage-backend", "dynamic",
                "--hicache-storage-backend-extra-config", PEERCACHE_EXTRA_CONFIG,
            ],
        )
        wait_for_http_ready(cls.base_url, timeout=180)

    @classmethod
    def tearDownClass(cls):
        from sglang.test.test_utils import terminate_and_kill_process_tree

        if getattr(cls, "process", None) is not None:
            terminate_and_kill_process_tree(cls.process.pid)

    @classmethod
    def _get_model_name(cls):
        return DEFAULT_MODEL_NAME_FOR_TEST


class TestPeerCacheBackendPublish(HiCacheStoragePeerCacheBackendBaseMixin,
                                  CustomTestCase):
    """PeerCache receives KV pages from real sglang requests."""

    def test_shared_prefix_publishes_pages(self):
        # Warm-up requests sharing a long prefix; write_through publishes each
        # page to PeerCache as it is produced.
        for i in range(3):
            r = requests.post(
                self.base_url + "/generate",
                json={
                    "text": SHARED_PREFIX + " Question %d: what follows?" % i,
                    "sampling_params": {"max_new_tokens": 8, "temperature": 0},
                },
                timeout=180,
            )
            self.assertEqual(r.status_code, 200)

        # Give the backup thread a moment to drain.
        time.sleep(3)

        write_reqs = _peercache_metric("peercache_write_requests_total")
        pool_keys = _peercache_metric("peercache_pool_keys")
        members = _peercache_metric("peercache_members")

        self.assertIsNotNone(write_reqs, "PeerCache metrics not reachable")
        self.assertGreater(write_reqs, 0, "no KV pages written to PeerCache")
        self.assertGreater(pool_keys, 0, "PeerCache published pool is empty")
        self.assertGreaterEqual(members, 1, "PeerCache membership ring empty")

    def test_second_pass_hits(self):
        # Same prefix again: sglang's radix cache should report cached tokens,
        # and PeerCache must still be serving its published pages.
        r = requests.post(
            self.base_url + "/generate",
            json={
                "text": SHARED_PREFIX + " Question 0: what follows?",
                "sampling_params": {"max_new_tokens": 8, "temperature": 0},
            },
            timeout=180,
        )
        self.assertEqual(r.status_code, 200)
        meta = r.json().get("meta_info", {}) or {}
        # The 3 warm-up requests cached the prefix; expect most tokens cached.
        self.assertGreater(meta.get("cached_tokens", 0), 0,
                           "expected radix-cache hits on the second pass")


if __name__ == "__main__":
    unittest.main()

