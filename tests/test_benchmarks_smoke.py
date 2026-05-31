"""Functional smoke tests for the benchmark harness (benchmarks/).

These keep the harness importable and runnable so it does not bit-rot. They run
over the in-process TCP fallback purely for *functional* validation -- this is
NOT a performance scenario, and the throughput/latency values are not asserted
on (real numbers require RDMA hardware).
"""

import argparse

import pytest

from common import BaselineReport, Histogram, Workload, make_result, render_hicache_markdown


def test_histogram_percentiles():
    h = Histogram()
    for v in range(1, 1001):  # 1..1000 microseconds
        h.record(v / 1e6)
    assert len(h) == 1000
    # p50 ~ 500us, p99 ~ 990us, within bucket precision (~0.1%).
    assert 495 <= h.percentile_us(50) <= 505
    assert 980 <= h.percentile_us(99) <= 1000
    assert h.max_us() >= 999
    assert 499 <= h.mean_us() <= 501


def test_histogram_merge():
    a = Histogram(); b = Histogram()
    for _ in range(10):
        a.record(1e-3)
    for _ in range(10):
        b.record(2e-3)
    a.merge(b)
    assert len(a) == 20
    assert a.max_us() >= 1999


def test_make_result_and_render():
    wl = Workload(block_size=4096, batch_size=8, threads=4, duration=1.0)
    h = Histogram()
    for v in range(1, 101):
        h.record(v / 1e6)
    r = make_result("peercache", "hicache-get", "rdma", wl, ops=1000,
                    bytes_total=4096 * 1000, elapsed_s=1.0, hist=h, op="get",
                    pages=1000, tokens_per_page=64, hit_rate=1.0)
    assert r.ok and r.throughput_gbps > 0 and r.pages_per_s == 1000
    assert r.tokens_per_s == 64000
    assert r.lat_us_p999 >= r.lat_us_p99 >= r.lat_us_p50
    rep = BaselineReport()
    rep.add(r)
    md = render_hicache_markdown(rep)
    assert "get" in md and "tokens/s" in md
    assert '"op": "get"' in rep.to_json()


def _hicache_args(**over):
    base = dict(
        protocol="tcp", device_name="", ib_port=1, gid_index=3, layout="mla",
        page_size=4096, tokens_per_page=64, batch_size=4, duration=0.3,
        warmup=0.1, working_set=32, disk=False, max_bytes=512 * 1024 * 1024,
        out_dir="/tmp/hb_test", tag="",
    )
    base.update(over)
    return argparse.Namespace(**base)


def test_hicache_get_runs():
    import bench_hicache
    rows = bench_hicache.run_get(_hicache_args(), [1, 2])
    assert len(rows) == 2
    for r in rows:
        assert r.ok and r.op == "get" and r.pages_per_s > 0


def test_hicache_set_runs():
    import bench_hicache
    rows = bench_hicache.run_set(_hicache_args(), [1])
    assert rows[0].ok and rows[0].op == "set" and rows[0].pages_per_s > 0


def test_hicache_exists_runs():
    import bench_hicache
    rows = bench_hicache.run_exists(_hicache_args(), [1])
    assert rows[0].ok and rows[0].op == "exists" and rows[0].pages_per_s > 0


def test_hicache_mha_layout_runs():
    import bench_hicache
    rows = bench_hicache.run_get(_hicache_args(layout="mha"), [1])
    assert rows[0].ok and rows[0].pages_per_s > 0


def test_peercache_transport_microbench_runs():
    import bench_peercache
    wl = Workload(block_size=4096, batch_size=8, duration=0.3, warmup=0.1)
    r = bench_peercache.bench_transport_read(wl, protocol="tcp")
    assert r.ok and r.ops > 0 and r.throughput_gbps > 0
