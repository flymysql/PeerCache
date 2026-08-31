"""Deterministic slot-addressed KV store (``mode=slotmap``).

This module implements PeerCache's *directory-free* placement: every key maps
to a fixed physical location purely by hashing, so a reader can issue a
one-sided RDMA READ with **no metadata lookup** (no ``dir_get`` round-trip).

Model
-----
- The node that owns a key is ``ring.get_node(key)`` (same consistent-hash ring
  the rest of PeerCache uses).
- On that node, the key lands in a **bucket** chosen by ``hash2(key) % num_buckets``.
- Each bucket is **N-way**: ``ways`` fixed-size slots. A write picks a way inside
  the bucket (empty slot first, else the oldest by ``seq``); a read pulls the
  whole bucket in one READ and matches locally.
- Each slot is ``[header | payload]``. The header carries the key's 128-bit hash,
  the payload length, and a ``seq`` (seqlock) so a reader can tell a valid,
  fully-written page for *this* key from an empty slot, a different key
  (hash collision / eviction), or a torn write.

Correctness (never a dirty hit)
-------------------------------
A reader accepts a slot only if ``key_hash`` matches, ``payload_len`` equals the
expected size, and ``seq`` is even (stable). Anything else -> treat as a miss
and let SGLang recompute. Because KV cache is recomputable soft state, a miss is
harmless; a dirty hit would silently corrupt context, so it must never happen.

The header layout is versioned so the on-wire slot format can evolve.
"""

from __future__ import annotations

import ctypes
import hashlib
import struct
import threading
from typing import Dict, List, Optional, Tuple

# ------------------------------------------------------------------ #
# Slot header
# ------------------------------------------------------------------ #
# magic(4) | ver(2) | flags(2) | key_hi(8) | key_lo(8) | payload_len(4) |
# pad(4) | seq(8)  == 40 bytes, then padded to 64 for cacheline alignment.
_MAGIC = 0x50434B31  # "PCK1"
_HDR_VER = 1
HEADER_SIZE = 64
_HDR_STRUCT = struct.Struct("<IHHQQIIQ")  # 4+2+2+8+8+4+4+8 = 40 bytes
assert _HDR_STRUCT.size == 40


def key_hash128(key: str) -> Tuple[int, int]:
    """128-bit key identity (hi, lo). Distinct from the ring/bucket hash so a
    bucket collision does not imply a header collision."""
    d = hashlib.blake2b(key.encode("utf-8"), digest_size=16).digest()
    return (
        int.from_bytes(d[:8], "little"),
        int.from_bytes(d[8:], "little"),
    )


def bucket_hash(key: str) -> int:
    """Hash used to pick the bucket (independent of key_hash128)."""
    return int.from_bytes(
        hashlib.blake2b(key.encode("utf-8"), digest_size=8, salt=b"bucket").digest(),
        "little",
    )


def encode_header(key: str, payload_len: int, seq: int) -> bytes:
    hi, lo = key_hash128(key)
    body = _HDR_STRUCT.pack(_MAGIC, _HDR_VER, 0, hi, lo, payload_len, 0, seq)
    return body + b"\x00" * (HEADER_SIZE - len(body))


def decode_header(buf: bytes):
    """Return (valid_magic, key_hi, key_lo, payload_len, seq) or None if too short."""
    if len(buf) < _HDR_STRUCT.size:
        return None
    magic, ver, _flags, hi, lo, plen, _pad, seq = _HDR_STRUCT.unpack(
        buf[: _HDR_STRUCT.size]
    )
    if magic != _MAGIC or ver != _HDR_VER:
        return (False, 0, 0, 0, 0)
    return (True, hi, lo, plen, seq)


def slot_matches(header_bytes: bytes, key: str, expected_len: int) -> bool:
    """True iff the slot holds a valid, stable page for exactly this key.

    Rejects: bad magic, different key (collision/eviction), wrong length, or an
    odd seq (a write in progress / torn write). This is the single gate that
    guarantees a reader never returns another key's or a half-written page.
    """
    dec = decode_header(header_bytes)
    if dec is None or not dec[0]:
        return False
    _ok, hi, lo, plen, seq = dec
    if seq & 1:  # odd -> write in progress
        return False
    if plen != expected_len:
        return False
    khi, klo = key_hash128(key)
    return hi == khi and lo == klo


def slot_present(header_bytes: bytes, key: str):
    """Existence probe: return the stored payload length if the slot holds a
    valid, stable page for exactly this key, else None.

    Identical to ``slot_matches`` except it is *length-agnostic* -- the caller
    (``batch_exists``) does not know the expected size, so we accept any valid
    page for this key and report its length. Still rejects bad magic, a
    different key (collision/eviction), and an odd seq (torn/in-progress write),
    so a present() can never be a dirty hit.
    """
    dec = decode_header(header_bytes)
    if dec is None or not dec[0]:
        return None
    _ok, hi, lo, plen, seq = dec
    if seq & 1:  # odd -> write in progress
        return None
    khi, klo = key_hash128(key)
    if hi == khi and lo == klo:
        return int(plen)
    return None


def pick_way_from_bucket(bucket_bytes: bytes, geom: "SlotGeometry", key: str):
    """Choose (way, new_seq) for writing ``key`` given a snapshot of its bucket.

    Selection order mirrors the local writer:
      1. a way already holding this key (overwrite/refresh),
      2. an empty/invalid way,
      3. the way with the smallest seq (LRU-ish victim).
    ``new_seq`` is the next even seq to stamp for the chosen way (so a later
    reader / writer sees a monotonically fresher page).
    """
    khi, klo = key_hash128(key)
    empty_way = None
    oldest_way, oldest_seq = 0, None
    chosen = None
    for way in range(geom.ways):
        off = way * geom.slot_stride
        dec = decode_header(bucket_bytes[off:off + HEADER_SIZE])
        if dec is None or not dec[0]:
            if empty_way is None:
                empty_way = way
            seq = 0
        else:
            _ok, hi, lo, _plen, seq = dec
            if (hi, lo) == (khi, klo):
                chosen = way
        if oldest_seq is None or seq < oldest_seq:
            oldest_seq, oldest_way = seq, way
    if chosen is None:
        chosen = empty_way if empty_way is not None else oldest_way
    # Next even seq strictly greater than what's there.
    base = oldest_seq if oldest_seq is not None else 0
    new_seq = (base + 2) & ~1
    if new_seq == 0:
        new_seq = 2
    return chosen, new_seq


# ------------------------------------------------------------------ #
# Slot geometry
# ------------------------------------------------------------------ #

def _align(n: int, a: int = 64) -> int:
    return (n + a - 1) // a * a


class SlotGeometry:
    """Fixed layout of a single size-class region: num_buckets x ways x stride."""

    def __init__(self, max_payload: int, num_buckets: int, ways: int):
        self.max_payload = int(max_payload)
        self.num_buckets = int(num_buckets)
        self.ways = int(ways)
        self.slot_stride = _align(HEADER_SIZE + self.max_payload)
        self.bucket_stride = self.slot_stride * self.ways

    @property
    def capacity(self) -> int:
        return self.bucket_stride * self.num_buckets

    def bucket_index(self, key: str) -> int:
        return bucket_hash(key) % self.num_buckets

    def bucket_offset(self, key: str) -> int:
        return self.bucket_index(key) * self.bucket_stride

    def slot_offset(self, bucket_idx: int, way: int) -> int:
        return bucket_idx * self.bucket_stride + way * self.slot_stride


# ------------------------------------------------------------------ #
# Local slot region (the owner side): one registered MR laid out as slots.
# ------------------------------------------------------------------ #

class SlotRegion:
    """The owner-side backing store for one size class.

    Holds a base host buffer (registered as an RDMA MR by the caller) laid out
    as ``num_buckets x ways`` fixed slots. Provides local write (own-key path)
    and way selection. Remote writers target the same geometry via RDMA WRITE
    computed on the reader/writer side, so this class is only needed where a
    node writes its *own* keys locally, or to seed empty headers.
    """

    def __init__(self, base_addr: int, geom: SlotGeometry):
        self._base = base_addr
        self._geom = geom
        self._lock = threading.Lock()
        # Per-slot local bookkeeping so the owner can pick a victim way and bump
        # seq without RDMA-reading its own memory. Keyed by (bucket, way).
        self._seq: Dict[Tuple[int, int], int] = {}

    @property
    def base_addr(self) -> int:
        return self._base

    @property
    def geometry(self) -> SlotGeometry:
        return self._geom

    def _slot_key_hash(self, bucket: int, way: int) -> Optional[Tuple[int, int]]:
        off = self._geom.slot_offset(bucket, way)
        hdr = ctypes.string_at(self._base + off, HEADER_SIZE)
        dec = decode_header(hdr)
        if dec is None or not dec[0]:
            return None
        return (dec[1], dec[2])

    def pick_way(self, key: str) -> Tuple[int, int]:
        """Choose (bucket, way) for writing ``key``.

        Prefer: (1) a way already holding this key (overwrite/refresh), else
        (2) an empty way, else (3) the way with the smallest local seq (LRU-ish).
        Caller holds no lock; we take ours.
        """
        bucket = self._geom.bucket_index(key)
        khi, klo = key_hash128(key)
        with self._lock:
            empty_way = None
            oldest_way, oldest_seq = 0, None
            for way in range(self._geom.ways):
                kh = self._slot_key_hash(bucket, way)
                if kh == (khi, klo):
                    return bucket, way
                if kh is None and empty_way is None:
                    empty_way = way
                s = self._seq.get((bucket, way), 0)
                if oldest_seq is None or s < oldest_seq:
                    oldest_seq, oldest_way = s, way
            return bucket, (empty_way if empty_way is not None else oldest_way)

    def write_local(self, key: str, src_ptr: int, length: int) -> Optional[int]:
        """Write a page for an own-key into its slot. Returns slot offset or None
        if the page is too big for the size class."""
        if length > self._geom.max_payload:
            return None
        bucket, way = self.pick_way(key)
        off = self._geom.slot_offset(bucket, way)
        with self._lock:
            seq = self._seq.get((bucket, way), 0)
            new_seq = (seq + 2) if (seq & 1) == 0 else (seq + 1)  # keep even
            # seqlock: mark odd (write in progress), write payload, then even.
            self._mark_seq(off, new_seq | 1, key, length)
            ctypes.memmove(self._base + off + HEADER_SIZE, src_ptr, length)
            self._write_header(off, key, length, new_seq)
            self._seq[(bucket, way)] = new_seq
        return off

    def _mark_seq(self, off: int, seq: int, key: str, length: int) -> None:
        hdr = encode_header(key, length, seq)
        buf = (ctypes.c_char * HEADER_SIZE).from_buffer_copy(hdr)
        ctypes.memmove(self._base + off, buf, HEADER_SIZE)

    def _write_header(self, off: int, key: str, length: int, seq: int) -> None:
        self._mark_seq(off, seq, key, length)

    def read_local(self, key: str, dst_ptr: int, length: int) -> bool:
        """Serve an own-key read locally (memmove). True on a validated hit."""
        bucket = self._geom.bucket_index(key)
        for way in range(self._geom.ways):
            off = self._geom.slot_offset(bucket, way)
            hdr = ctypes.string_at(self._base + off, HEADER_SIZE)
            if slot_matches(hdr, key, length):
                ctypes.memmove(dst_ptr, self._base + off + HEADER_SIZE, length)
                return True
        return False

    def exists_local(self, key: str):
        """Own-key existence probe (header only, no memmove). Returns the stored
        payload length if a valid stable page for this key is present, else
        None. Length-agnostic: the exists path does not know the expected size."""
        bucket = self._geom.bucket_index(key)
        for way in range(self._geom.ways):
            off = self._geom.slot_offset(bucket, way)
            hdr = ctypes.string_at(self._base + off, HEADER_SIZE)
            plen = slot_present(hdr, key)
            if plen is not None:
                return plen
        return None
