# PeerCache benchmark suite (`peercache.bench`)

A systematic, reproducible benchmark that drives PeerCache's `HiCacheStorage`
interface exactly as **SGLang HiCache** does, and reports the numbers you can
publish: throughput (pages/s, tokens/s, GB/s) and latency tail
(p50/p95/p99/p999/max) across a sweep of **thread models** (concurrency),
including the full-load **saturation / peak** throughput.

It is shipped *inside the package* and exposed as console commands — after
`pip install peercache` you run it from anywhere, no repo clone, no
`PYTHONPATH`:

| command | what it runs |
|---|---|
| `peercache-bench` | the systematic SGLang-HiCache benchmark (subcommands below) |
| `peercache-bench-micro` | low-level data-plane microbench (transport / store) |
| `peercache-bench-mooncake` | wraps Mooncake's official `transfer_engine_bench` |
| `peercache-bench-compare` | PeerCache-vs-Mooncake sweep under matched block sizes |

> [!IMPORTANT]
> **RDMA-first.** Headline numbers must be measured with `--protocol rdma` on a
> host with an RDMA NIC. The pure-Python TCP fallback exists for *functional
> smoke testing only* and must not be published.

## What it models (PD-disaggregation)

```
prefill node  --batch_set_v1-->  publish KV pages    (write / offload)
decode node   --batch_exists-->  probe cached prefix (lookup)
              --batch_get_v1-->  load pages over RDMA (read / prefetch, zero copy)
```

A producer `PeerCacheStore` publishes pages; a consumer reads them back across
the fabric — the exact path SGLang drives (directory lookup + one-sided RDMA
READ into the registered host buffer). Page layout is faithful to SGLang:
`--layout mla` (1 object/page) or `--layout mha` (k+v, 2 objects/page).

## Install

```bash
pip install peercache                  # RDMA build (needs libibverbs / librdmacm)
pip install "peercache[bench]"         # also pulls mooncake-transfer-engine for the comparison
```

## Run on RDMA hardware (publishable numbers)

Find your device with `ibv_devices`, then run the full suite:

```bash
peercache-bench suite \
    --device-name mlx5_0 --layout mla \
    --page-size 131072 --tokens-per-page 64 \
    --batch-size 32 --concurrencies 1,2,4,8,16,32,64 \
    --duration 10 --warmup 2 --tag rdma
```

Results are written to `./peercache-bench-results/hicache-suite-rdma-<ts>.{json,md}`
in your current directory and contain:

1. single-op **latency baseline** (get/set/exists, batch 1, concurrency 1),
2. **get** saturation sweep (read/prefetch) with the PEAK row,
3. **set** saturation sweep (write/offload) with the PEAK row,
4. **exists** saturation sweep (directory lookup).

### Sub-modes

```bash
peercache-bench latency     --device-name mlx5_0 ...                       # per-page latency tail
peercache-bench throughput  --op get --concurrency 16 --device-name mlx5_0 ...
peercache-bench saturation  --op set --concurrencies 1,4,16,64 --device-name mlx5_0 ...
```

### True cross-node (two hosts)

The in-process cluster uses NIC loopback. For a real two-host number, run a
producer `PeerCacheStore` on node A and a consumer on node B pointed at one
`discovery_addr` (see `examples/sglang_launch.md`): A publishes a key range with
`batch_set_v1`, B drives `batch_exists` + `batch_get_v1`.

## Metrics

| metric | meaning |
|---|---|
| page | one logical KV page (1 object MLA, k+v MHA) |
| pages/s · tokens/s | pages per second; `tokens/s = pages/s × tokens_per_page` |
| GB/s | payload bytes/s (10⁹) of components actually moved |
| p50…p999 / max | per **batch call** latency (use `latency` mode for per-page) |
| hit% | fraction of requested pages found (read path) |
| PEAK | concurrency row with the highest sustained throughput |

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
| `--out-dir` | results directory | `./peercache-bench-results` |
| `--tag` | output filename suffix | `""` |

## Optional: compare against Mooncake

```bash
peercache-bench-compare --protocol rdma --device-name mlx5_0 \
    --block-sizes 4k,16k,64k,256k,1m --duration 10 --tag rdma
```

## Caveats

1. **TCP ≠ RDMA**, and TCP is not a scenario — use it only to verify the code runs.
2. **Loopback ≠ network**: single-host RDMA uses NIC loopback; run cross-node for fabric behaviour.
3. Latency is **per batch call** unless you use `latency` mode (batch 1).
