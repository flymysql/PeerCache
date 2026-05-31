"""Smoke test for the benchmark harness (benchmarks/).

Keeps the harness importable and runnable over the TCP fallback so it does not
bit-rot. Uses tiny durations; does not assert on throughput magnitudes (those
depend on the host and, for real numbers, on RDMA hardware).
"""

import math

import pytest

from common import BaselineReport, Workload, Latencies, make_result, render_markdown
import bench_peercache


def test_latency_percentiles_basic():
    lat = Latencies()
    for v in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]:
        lat.add(v / 1e6)  # microseconds -> seconds
    assert math.isclose(lat.percentile(50) * 1e6, 5.5, rel_tol=1e-6)
    assert lat.percentile(99) * 1e6 <= 10.0
    assert lat.mean() * 1e6 == 5.5


def test_make_result_and_render():
    wl = Workload(block_size=4096, batch_size=8, duration=1.0)
    r = make_result("peercache", "transport-read", "tcp", wl,
                    ops=1000, bytes_total=4096 * 1000, elapsed_s=1.0)
    assert r.ok and r.throughput_gbps > 0 and r.ops_per_s == 1000
    rep = BaselineReport()
    rep.add(r)
    md = render_markdown(rep)
    assert "transport-read" in md and "throughput" in md
    # JSON round-trips.
    assert '"system": "peercache"' in rep.to_json()


def test_peercache_transport_read_runs():
    wl = Workload(block_size=4096, batch_size=8, duration=0.3, warmup=0.1, threads=1)
    r = bench_peercache.bench_transport_read(wl, protocol="tcp")
    assert r.ok
    assert r.ops > 0
    assert r.bytes_total > 0
    assert r.throughput_gbps > 0


def test_peercache_store_get_runs():
    wl = Workload(block_size=4096, batch_size=8, duration=0.3, warmup=0.1, threads=1)
    r = bench_peercache.bench_store_get(wl, protocol="tcp")
    assert r.ok
    assert r.ops > 0
    assert r.throughput_gbps > 0
