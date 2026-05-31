"""Unit tests for metrics collection and the HTTP exposition/dashboard."""

import urllib.request

from peercache.metrics import Metrics, MetricsServer


def test_counters_and_prometheus_render():
    m = Metrics(node_id="nodeA")
    m.record_read(hit=True, nbytes=100, seconds=0.001, source="remote")
    m.record_read(hit=True, nbytes=200, seconds=0.002, source="disk")
    m.record_read(hit=False, nbytes=0, seconds=0.003)
    m.record_write(nbytes=50, seconds=0.004)
    m.set_gauge_provider("pool_bytes_used", lambda: 4096)

    snap = m.snapshot()
    assert snap["counters"]["read_requests"] == 3
    assert snap["counters"]["read_hits"] == 2
    assert snap["counters"]["read_misses"] == 1
    assert snap["counters"]["read_disk_hits"] == 1
    assert abs(snap["read_hit_rate"] - 2 / 3) < 1e-6
    assert snap["gauges"]["pool_bytes_used"] == 4096
    assert snap["latency"]["read"]["count"] == 3

    text = m.render_prometheus()
    assert "peercache_read_requests_total" in text
    assert "peercache_read_hits_total" in text
    assert "peercache_pool_bytes_used" in text
    assert "peercache_read_hit_rate" in text
    assert 'quantile="0.99"' in text


def test_http_metrics_and_dashboard():
    m = Metrics(node_id="srv")
    m.record_read(hit=True, nbytes=10, seconds=0.001, source="local")
    server = MetricsServer(m, "127.0.0.1", 0, dashboard=True)
    port = server.start()
    assert port is not None
    try:
        body = urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=3).read()
        assert b"peercache_read_requests_total" in body
        html = urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=3).read()
        assert b"PeerCache" in html and b"/metrics" in html
        health = urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz", timeout=3).read()
        assert health == b"ok"
    finally:
        server.stop()
