"""Unit tests for the async disk persistence tier."""

import time

from peercache.diskstore import DiskStore


def _wait(cond, timeout=3.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return True
        time.sleep(0.02)
    return False


def test_put_get_exists_remove(tmp_path):
    d = DiskStore(str(tmp_path), max_bytes=1 << 20, node_id="n1")
    try:
        d.put("a", b"hello")
        d.put("b", b"world!!")
        assert _wait(lambda: d.exists("a") and d.exists("b"))
        assert d.get("a") == b"hello"
        assert d.get("b") == b"world!!"
        assert d.get("missing") is None
        used, n = d.stats()
        assert n == 2 and used == len(b"hello") + len(b"world!!")
        d.remove("a")
        assert not d.exists("a")
        assert d.stats()[1] == 1
    finally:
        d.close()


def test_lru_capacity_eviction(tmp_path):
    evicted = []
    payload = b"x" * 100
    # Capacity for a single 100-byte entry -> writing a second evicts the first.
    d = DiskStore(str(tmp_path), max_bytes=100, on_evict=evicted.extend, node_id="n1")
    try:
        d.put("a", payload)
        assert _wait(lambda: d.exists("a"))
        d.put("b", payload)
        assert _wait(lambda: d.exists("b"))
        assert _wait(lambda: not d.exists("a"))
        assert evicted == ["a"]
        assert d.get("a") is None
        assert d.get("b") == payload
    finally:
        d.close()


def test_index_persists_across_restart(tmp_path):
    d = DiskStore(str(tmp_path), max_bytes=1 << 20, node_id="n1")
    d.put("k1", b"persist-me")
    assert _wait(lambda: d.exists("k1"))
    d.close()

    d2 = DiskStore(str(tmp_path), max_bytes=1 << 20, node_id="n1")
    try:
        assert d2.exists("k1")
        assert d2.get("k1") == b"persist-me"
    finally:
        d2.close()


def test_rebuild_index_by_scan(tmp_path):
    d = DiskStore(str(tmp_path), max_bytes=1 << 20, node_id="n1")
    d.put("scan-key", b"data-123")
    assert _wait(lambda: d.exists("scan-key"))
    d.close()

    # Drop the sidecar index so the next instance must rebuild from file headers.
    (tmp_path / "n1" / "index.json").unlink()
    d2 = DiskStore(str(tmp_path), max_bytes=1 << 20, node_id="n1")
    try:
        assert d2.exists("scan-key")
        assert d2.get("scan-key") == b"data-123"
    finally:
        d2.close()
