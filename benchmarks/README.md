# PeerCache benchmark harness

A systematic, reproducible harness that drives PeerCache's `HiCacheStorage`
interface exactly as **SGLang HiCache** does, and reports the performance
numbers you can publish: throughput (pages/s, tokens/s, GB/s) and latency tail
(p50/p95/p99/p999/max) across a sweep of **thread models** (concurrency),
including the full-load **saturation / peak** throughput.

> [!IMPORTANT]
> **RDMA-first.** PeerCache's value is RDMA one-sided READ; headline numbers must
> be measured with `--protocol rdma` on a host with an RDMA NIC. A pure-Python
> TCP fallback exists in the transport for *functional smoke testing only* (CI,
> laptops). **TCP runs are not a performance scenario and must not be published.**

## What it models (PD-disaggregation)

```
prefill node  --batch_set_v1-->  publish KV pages   (write / offload)
decode node   --batch_exists-->  probe cached prefix (lookup)
              --batch_get_v1-->  load pages over RDMA (read / prefetch, zero copy)
```

The harness brings up an embedded discovery service plus two `PeerCacheStore`
nodes in one process: a **producer** (prefill) publishes pages and a
**consumer** (decode) reads them back across the fabric, exercising the full
path SGLang drives (directory lookup + RDMA READ into the registered host
buffer). KV page layout is faithful to SGLang:

- `--layout mla` — 1 storage object per page (`_<pp>_k`).
- `--layout mha` — 2 objects per page (`_<tp>_k`, `_<tp>_v`), interleaved per slot.

## Files

```
benchmarks/
  common.py          # workload, HDR-style latency histogram, result schema, renderers
  sglang_sim.py      # SGLang HostKVCache stand-in (MLA/MHA) + PD-disaggregated Cluster
  bench_hicache.py   # the systematic benchmark: latency/throughput/saturation/suite
  bench_peercache.py # low-level data-plane microbench (transport-read / store-get)
  bench_mooncake.py  # optional: wraps Mooncake's transfer_engine_bench
  run_baseline.py    # optional: PeerCache-vs-Mooncake comparison sweep
  results/           # JSON + Markdown artifacts (git-ignored; force-add a baseline)
```

## Metrics

| metric | meaning |
|---|---|
| **page** | one logical KV page (1 object for MLA, k+v for MHA) |
| **pages/s** | logical KV pages transferred per second |
| **tokens/s** | `pages/s × --tokens-per-page` (set via your model's page token count) |
| **GB/s** | payload bytes/s (10⁹), counting all components actually moved |
| **p50…p999, max** | per **batch call** latency distribution (one `batch_*` call) |
| **hit%** | fraction of requested pages found (read path) |
| **PEAK** | the concurrency row with the highest sustained throughput (saturation) |

For per-**page** latency, use the `latency` mode (batch size 1).

## Install

```bash
pip install .                       # RDMA build (needs libibverbs / librdmacm)
pip install mooncake-transfer-engine  # only for the optional comparison
```

## Run on RDMA hardware (publishable numbers)

Find your device with `ibv_devices`. Single-host run (two engines do RDMA
loopback through the NIC):

```bash
PYTHONPATH=python:benchmarks python benchmarks/bench_hicache.py suite \
    --device-name mlx5_0 --layout mla \
    --page-size 131072 --tokens-per-page 64 \
    --batch-size 32 --concurrencies 1,2,4,8,16,32,64 \
    --duration 10 --warmup 2 --tag rdma
```

This writes `results/hicache-suite-rdma-<ts>.{json,md}` containing:

1. single-op **latency baseline** (get/set/exists, batch 1, concurrency 1),
2. **get** saturation sweep (read/prefetch) with the PEAK row,
3. **set** saturation sweep (write/offload) with the PEAK row,
4. **exists** saturation sweep (directory lookup).

Other modes:

```bash
# one thread model, one op
python benchmarks/bench_hicache.py throughput --op get --concurrency 16 --device-name mlx5_0 ...
# concurrency sweep for one op
python benchmarks/bench_hicache.py saturation --op set --concurrencies 1,4,16,64 --device-name mlx5_0 ...
# pure single-op latency tail
python benchmarks/bench_hicache.py latency --device-name mlx5_0 ...
```

### True cross-node (two hosts)

The in-process cluster uses NIC loopback. For a real two-host number, run a
producer `PeerCacheStore` on node A and a consumer on node B pointed at one
`discovery_addr` (see `examples/sglang_launch.md`): A publishes a key range with
`batch_set_v1`, B drives `batch_exists` + `batch_get_v1`. The same `common.py`
histogram/result helpers apply.

## Knobs

| flag | meaning | default |
|---|---|---|
| `--protocol` | `rdma` (publishable) or `tcp` (smoke only) | `rdma` |
| `--device-name` | RDMA device (e.g. `mlx5_0`) | `""` |
| `--ib-port` / `--gid-index` | RDMA port / GID | `1` / `3` |
| `--layout` | `mla` or `mha` | `mla` |
| `--page-size` | bytes per component object (k or v) | `131072` |
| `--tokens-per-page` | tokens per page (for tokens/s) | `64` |
| `--batch-size` | pages per batch call | `32` |
| `--concurrencies` | thread-model sweep | `1,2,4,8,16,32` |
| `--duration` / `--warmup` | seconds | `10` / `2` |
| `--working-set` | distinct pages for get/exists | `4096` |
| `--disk` | enable disk write-through tier | off |
| `--max-bytes` | host-memory budget guard | `8 GiB` |
| `--tag` | output filename suffix | `""` |

## Optional: compare against Mooncake

```bash
PYTHONPATH=python:benchmarks python benchmarks/run_baseline.py \
    --protocol rdma --device-name mlx5_0 \
    --block-sizes 4k,16k,64k,256k,1m --duration 10 --tag rdma
```

This runs PeerCache's data-plane microbench and Mooncake's official
`transfer_engine_bench` under matched block sizes. See `bench_mooncake.py` for
two-host commands.

## Caveats

1. **TCP ≠ RDMA, and TCP is not a scenario here.** Use it only to check the code
   runs; never publish TCP numbers.
2. **Loopback ≠ network.** Single-host RDMA uses NIC loopback; for fabric
   behaviour run cross-node.
3. **Latency is per batch call** unless you use `latency` mode (batch 1).
4. Always publish the `host`/`meta` blocks from the JSON artifact next to any
   figure (device, layout, page size, batch, concurrency).
