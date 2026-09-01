# Slotmap mode: directory-free deterministic KV placement

PeerCache's default `p2p` mode keeps a distributed directory (consistent-hash
shard of `key -> {node, addr, rkey, len}`). Every read therefore needs a
directory lookup RPC before the one-sided RDMA READ.

`mode="slotmap"` removes the directory entirely. A key maps to its **owner** by
the ring and to a **fixed physical slot** by an independent hash, so a reader
computes the address locally and issues **one one-sided RDMA READ of the whole
N-way bucket** — no `dir_get` round-trip, no metadata at all.

```
p2p:      read = dir_get(key) ──RPC──> directory shard ──> {node,addr,rkey,len}
                                      └── one-sided RDMA READ
slotmap:  read = owner = ring(key); bucket = hash(key); slot = way(bucket)
                                      └── one-sided RDMA READ of the bucket
                                          (header-validated locally, no lookup)
```

## Why

- **Read latency**: one RTT saved per read (no directory RPC). On the hot
  path this is the dominant saving for small pages.
- **No metadata SPOF / resharding**: with no directory there is nothing to
  reshard when nodes join/leave; placement is a pure function of the ring +
  key hash. Ring membership still changes on join/leave (owner reassignment),
  but there is no per-entry metadata to migrate.
- **Deterministic addressing**: any node can compute any key's location from
  the ring and the geometry — a property `p2p` gets only after a directory
  lookup.

## How it works

### Geometry (`python/peercache/slotmap.py`)

Each node owns a fixed **slot region** — one registered MR of

```
num_buckets × ways × slot_stride bytes
```

where `slot_stride = HEADER + max_payload` (one size class for now). Peers
advertise `(base_addr, geometry, rkeys)` via a `_on_slot_layout` RPC; a reader
caches that layout and never asks again.

- `key -> owner`: existing consistent-hash ring (`data_owner_all`).
- `key -> bucket`: independent hash (`bucket_hash`) over the key.
- `key -> way`: N-way associativity. `pick_way` prefers an **empty** slot,
  then the **same key** (overwrite), then the **oldest sequence** (evict).

### Header / seqlock (`slot_matches`)

Every slot starts with a versioned header carrying `key_hash128(key)` and
`payload_len`. `slot_matches(header, key, expected_len)` is the **single
gate** that guarantees a reader never returns:

- another key's page (hash mismatch → clean miss),
- a torn page (odd seq → write in progress → clean miss),
- a wrong-length page (payload mismatch → clean miss).

A "miss" is therefore always *clean*: the caller simply treats the slot as
empty. `exists` probes the header only (no payload copy).

### Write / read paths (`store.py`)

| path | behavior |
|---|---|
| `batch_set_v1` | `_publish_slotmap`: own keys → local `write_local` (memmove + seqlock); remote keys → one batched READ of the target buckets, pick a way, one batched WRITE (RMW, no reservation RPC). An intra-batch reservation map stops same-bucket keys from evicting each other. |
| `batch_get_v1` | `_fetch_slotmap`: per key, one READ of its bucket, `slot_matches` locally, memmove payload on a hit. |
| `batch_exists` | `_exists_slotmap`: header-only bucket READs, no payload copy. |

Both RDMA and TCP transports implement `batch_read_multi` / `batch_write_multi`,
so slotmap works over **TCP too** (functional validation without RDMA hardware)
and over **one-sided RDMA** (the intended production path).

## Configuration

```json
{
  "backend_name": "peercache",
  "module_path": "peercache.store",
  "class_name": "PeerCacheStore",
  "discovery_addr": "NODE0_IP:31998",
  "protocol": "rdma",
  "mode": "slotmap",
  "slot_max_page_bytes": 262144,
  "slot_ways": 4,
  "slot_num_buckets": 0
}
```

| key | default | meaning |
|---|---|---|
| `mode` | `"p2p"` | `"slotmap"` enables directory-free placement |
| `slot_max_page_bytes` | `262144` (256 KiB) | max payload of one KV page; **must be ≥ the KV page size sglang emits** (one size class for now). Tune to your page size; pages larger than this are cleanly skipped. |
| `slot_ways` | `4` | N-way associativity per bucket. More ways → higher hit rate under collisions, at the cost of a larger bucket READ. |
| `slot_num_buckets` | `0` | bucket count per node; `0` derives from `global_segment_size / (ways × slot_stride)`. |

## Run with SGLang

```bash
python -m sglang.launch_server --enable-hierarchical-cache \
  --hicache-write-policy write_through \
  --hicache-storage-backend dynamic \
  --hicache-storage-backend-extra-config \
  '{"backend_name":"peercache","module_path":"peercache.store","class_name":"PeerCacheStore",
    "discovery_addr":"NODE0_IP:31998","protocol":"tcp","mode":"slotmap",
    "slot_max_page_bytes":262144,"slot_ways":4,"slot_num_buckets":0}'
```

Startup log confirms the slot region:

```
PeerCacheStore slot region ready: 1023 buckets x 4 ways x 262208 B (=1072955136 bytes) across 1 rail(s)
```

Metrics: `write_requests` / `bytes_written` count published pages. There is no
`pool_keys` in slotmap mode (no published pool, no directory) — that is by
design, not a bug.

## Verified (L20, sglang 0.5.9 + Qwen2.5-0.5B, TCP)

- **Store-level byte-identical cross-node roundtrip**:
  `tests/test_slotmap_e2e_tcp.py` (write on A, read on B, content verified;
  clean miss on missing keys; no dirty hit on length mismatch).
- **SGLang e2e, single node**: `tests/sglang/test_sglang_e2e.py --mode slotmap`
  launches a real server, drives shared-prefix requests, asserts
  `write_requests` / `bytes_written` (PASS).
- **Cross-node read against a live server**:
  `tests/sglang/test_sglang_2node_slotmap_read.py` joins the cluster as a
  second node, resolves the producer's slot layout (no directory), and reads
  the published pages back with one-shot bucket READs — 7 pages × 12 KiB all
  recovered, 256 bucket reads OK.

## Maturity

slotmap is **functional and integration-tested, pre-production for
performance**: the addressing design, seqlock gate, local + cross-node
read/write, and the real-sglang-server integration are all verified above,
but the one-sided RDMA path has only been *built* (compiles against
libibverbs, `HAS_RDMA=True`, full suite passes) — it has **not** yet been
measured on cross-host RoCE hardware. `peercache-bench` also does not drive
slotmap yet (p2p only), so there are no published slotmap throughput numbers.
See the overall [maturity notes](architecture.md#maturity).

## Limitations / roadmap

- **One size class**: `slot_max_page_bytes` is global; a page larger than it
  is skipped (clean miss). Multi-size classes would raise slot utilization on
  mixed workloads.
- **Deterministic, not load-balanced**: keys land on their hash bucket
  regardless of hot/cold; `p2p`'s LRU published pool absorbs hotspots better.
  slotmap trades that for zero-lookup reads.
- **Collisions evict**: with `ways` small and high collision, older pages are
  evicted silently (readers see a clean miss, never stale data). Sizing
  `slot_num_buckets` high enough matters for hit rate.
- **bench tooling**: `peercache-bench` does not yet drive slotmap mode
  (p2p only); the read path is covered by the tests above.
- **RDMA**: the design targets one-sided RDMA READ; TCP exercises the same
  code path functionally. The C++ data plane compiles against real
  libibverbs (verified: `HAS_RDMA=True`, full suite passes) and degrades to
  TCP cleanly on NIC-less machines. A cross-host RoCE measurement for
  slotmap is the remaining production validation.

## When to use which

| | `p2p` | `slotmap` |
|---|---|---|
| read path | directory RPC + RDMA READ | one RDMA READ |
| metadata | distributed DHT, reshard on membership change | none (pure hash) |
| hotspots | LRU published pool absorbs | fixed buckets, collisions evict |
| page sizes | any | one size class |
| best for | mixed workloads, hot pages, small clusters | uniform pages, latency-sensitive reads, large clusters |
