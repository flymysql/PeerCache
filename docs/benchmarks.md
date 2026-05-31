# Benchmarks

PeerCache ships a systematic benchmark suite **inside the package**, exposed as
console commands. It drives PeerCache's `HiCacheStorage` interface exactly as
**SGLang HiCache** does, so you can produce publishable numbers: throughput
(pages/s, tokens/s, GB/s) and the latency tail (p50/p95/p99/p999/max) across a
sweep of **thread models** (concurrency), including the full-load **saturation /
peak** throughput.

No repo clone and no `PYTHONPATH` needed — after `pip install peercache` the
commands are on your `PATH`:

A single command, `peercache-bench`, with subcommands:

| subcommand | what it runs |
|---|---|
| `latency` / `throughput` / `saturation` / `suite` | systematic SGLang-HiCache benchmark |
| `micro` | low-level data-plane microbench |
| `mooncake` | wraps Mooncake's official `transfer_engine_bench` |
| `compare` | PeerCache-vs-Mooncake sweep |

!!! danger "RDMA-first — read before quoting any number"
    PeerCache's value is **RDMA one-sided READ**. Headline numbers must be
    measured with `--protocol rdma` on a host with an RDMA NIC. A pure-Python TCP
    fallback exists for *functional smoke testing only*; **TCP runs are not a
    performance scenario and must not be published.**

## What it models: PD-disaggregation

```
prefill node  --batch_set_v1-->  publish KV pages    (write / offload)
decode node   --batch_exists-->  probe cached prefix (lookup)
              --batch_get_v1-->  load pages over RDMA (read / prefetch, zero copy)
```

A **producer** `PeerCacheStore` (prefill) publishes pages and a **consumer**
(decode) reads them back across the fabric — the exact path SGLang drives
(directory lookup + RDMA READ into the registered host buffer). Page layout is
faithful to SGLang: `--layout mla` (1 object/page) or `--layout mha` (k+v).

## Modes

| subcommand | what it answers |
|---|---|
| `latency` | single in-flight op tail (concurrency 1, batch 1) — per-page latency |
| `throughput` | sustained throughput + tail at one fixed thread model |
| `saturation` | throughput/latency curve across a concurrency sweep + the PEAK |
| `suite` | the full baseline: latency + get/set/exists saturation |

## Metrics

| metric | meaning |
|---|---|
| page | one logical KV page (1 object MLA, k+v MHA) |
| pages/s · tokens/s | pages per second; `tokens/s = pages/s × tokens_per_page` |
| GB/s | payload bytes/s (10⁹) of components actually moved |
| p50…p999 / max | per **batch call** latency (use `latency` mode for per-page) |
| hit% | fraction of requested pages found (read path) |
| PEAK | concurrency row with the highest sustained throughput |

## Install & run on RDMA hardware

```bash
pip install peercache            # RDMA build (needs libibverbs/librdmacm)
# pip install "peercache[bench]" # also installs mooncake-transfer-engine

peercache-bench suite \
    --device-name mlx5_0 --layout mla \
    --page-size 131072 --tokens-per-page 64 \
    --batch-size 32 --concurrencies 1,2,4,8,16,32,64 \
    --duration 10 --warmup 2 --tag rdma
```

Writes `./peercache-bench-results/hicache-suite-rdma-<ts>.{json,md}` in your
current directory. Sub-modes:

```bash
peercache-bench latency     --device-name mlx5_0 ...
peercache-bench throughput  --op get --concurrency 16 --device-name mlx5_0 ...
peercache-bench saturation  --op set --concurrencies 1,4,16,64 --device-name mlx5_0 ...
```

For a real two-host result (instead of single-host NIC loopback), run a producer
`PeerCacheStore` on one node and a consumer on another pointed at the same
`discovery_addr`; see
[the bench README](https://github.com/flymysql/PeerCache/blob/main/python/peercache/bench/README.md).

## Baseline results template

Fill this in from your RDMA run's `peercache-bench-results/*.md` (numbers are
intentionally blank — they must come from your hardware, not a sandbox):

| op | layout | page | batch | threads | pages/s | tokens/s | GB/s | p50 µs | p99 µs | p999 µs |
|---|---|---|---|---|---|---|---|---|---|---|
| get (latency) | mla | 128 KB | 1 | 1 | | | | | | |
| get (peak) | mla | 128 KB | 32 | _N_ | | | | | | |
| set (peak) | mla | 128 KB | 32 | _N_ | | | | | | |
| exists (peak) | mla | 128 KB | 32 | _N_ | | | | | | |

Always publish the JSON artifact's `host` and `meta` blocks (device, layout,
page size, batch, concurrency) next to any figure.

## Optional: compare against Mooncake

```bash
peercache-bench compare --protocol rdma --device-name mlx5_0 \
    --block-sizes 4k,16k,64k,256k,1m --duration 10 --tag rdma
```

Runs PeerCache's data-plane microbench alongside Mooncake's official
`transfer_engine_bench` under matched block sizes.

## Caveats

1. **TCP ≠ RDMA**, and TCP is not a scenario — use it only to verify the code runs.
2. **Loopback ≠ network**: single-host RDMA uses NIC loopback; run cross-node for fabric behaviour.
3. Latency is **per batch call** unless you use `latency` mode (batch 1).
