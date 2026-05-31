import ctypes

from peercache.pool import PublishedPool


def _filled(nbytes, seed=0):
    src = (ctypes.c_byte * nbytes)()
    for i in range(nbytes):
        src[i] = (seed + i) % 127
    return src


def test_publish_copies_data_and_returns_addr():
    cap = 4096
    backing = (ctypes.c_byte * cap)()
    pool = PublishedPool(ctypes.addressof(backing), cap, rkey=11)

    src = _filled(256, seed=3)
    addr = pool.publish("k1", ctypes.addressof(src), 256)
    assert addr is not None
    assert pool.rkey == 11

    out = bytes((ctypes.c_byte * 256).from_address(addr))
    assert out == bytes(src)
    assert pool.address_of("k1") == (addr, 256)
    assert "k1" in pool


def test_publish_is_idempotent_per_key():
    cap = 4096
    backing = (ctypes.c_byte * cap)()
    pool = PublishedPool(ctypes.addressof(backing), cap, rkey=1)
    src = _filled(128)
    a1 = pool.publish("dup", ctypes.addressof(src), 128)
    a2 = pool.publish("dup", ctypes.addressof(src), 128)
    assert a1 == a2
    assert len(pool) == 1


def test_lru_eviction_invokes_callback_and_reuses_slot():
    cap = 512  # exactly two 256-byte pages
    evicted = []
    backing = (ctypes.c_byte * cap)()
    pool = PublishedPool(
        ctypes.addressof(backing), cap, rkey=1, on_evict=evicted.extend
    )
    src = _filled(256)
    pool.publish("a", ctypes.addressof(src), 256)
    pool.publish("b", ctypes.addressof(src), 256)
    # Touch "a" so "b" becomes least-recently-used.
    pool.address_of("a")
    pool.publish("c", ctypes.addressof(src), 256)

    assert evicted == ["b"]
    assert "b" not in pool
    assert "a" in pool and "c" in pool
    assert len(pool) == 2


def test_oversized_page_is_rejected():
    cap = 128
    backing = (ctypes.c_byte * cap)()
    pool = PublishedPool(ctypes.addressof(backing), cap, rkey=1)
    src = _filled(256)
    assert pool.publish("too-big", ctypes.addressof(src), 256) is None
