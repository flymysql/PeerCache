# Benchmarks

PeerCache ships a reproducible harness (`benchmarks/`) that measures its KV-cache
data plane and compares it, under an identical workload, with
[Mooncake](https://github.com/kvcache-ai/Mooncake)'s official
`transfer_engine_bench`.

!!! danger "Read before quoting any number"
    Both PeerCache and Mooncake are built around **RDMA one-sided READ**. Their
    headline performance only appears on real RDMA hardware (RoCE/InfiniBand).
    The harness also runs over a **TCP fallback** so it works on a laptop, CI, or
    a GPU-less VM — but **TCP numbers validate correctness/plumbing only and must
    never be presented as RDMA performance or used for marketing.** To produce
    publishable figures, run the harness on RDMA hardware (recipe below).

## What is measured

For each block size, three rows are produced under the same batch size and
duration:

| row | system | what runs |
|---|---|---|
| `transport-read` | PeerCache | two `Transport`s; batched one-sided READ (raw data plane) |
| `store-get` | PeerCache | 2-node `PeerCacheStore`: `batch_set_v1` then `batch_get_v1` (full HiCache path) |
| `transfer-engine` | Mooncake | official `transfer_engine_bench` (initiator reads from target) |

Throughput is reported in GB/s (10⁹ bytes/s, matching Mooncake's default unit),
plus ops/s and PeerCache per-op latency percentiles.

## Reference baseline (TCP fallback, single host — NOT RDMA)

Captured on a 4-vCPU, 15 GiB Linux VM with **no RDMA NIC** (`protocol=tcp`,
`127.0.0.1` loopback). Batch size 64, 5 s measurement, 1 s warmup. PeerCache
submits single-threaded; Mooncake's bench uses 4 threads (its default model).

| block | PeerCache `transport-read` (GB/s) | PeerCache `store-get` (GB/s) | Mooncake `transfer-engine` (GB/s) |
|---|---|---|---|
| 4 KB   | 0.144 | 0.061 | 0.020 |
| 16 KB  | 0.465 | 0.242 | 0.080 |
| 64 KB  | 1.138 | 0.986 | 0.360 |
| 256 KB | 1.890 | 1.290 | 1.230 |
| 1 MB   | 1.677 | 1.386 | 2.840 |

PeerCache p50/p99 per-op latency (transport-read): 24.5 / 75 µs at 4 KB rising to
621 / 780 µs at 1 MB. Raw artifacts: `benchmarks/results/`.

!!! warning "How to read this table"
    These are **software-path, single-host loopback** numbers. They demonstrate
    the harness runs end-to-end and that PeerCache's design has no pathological
    overhead versus Mooncake on the same fabric — nothing more. Observations:

    - At small/medium blocks PeerCache's lightweight in-process path leads;
    - At 1 MB Mooncake's multi-threaded C++ transport pulls ahead (2.84 vs 1.68).
    - **None of this reflects RDMA.** On RDMA, both jump by 1–2 orders of
      magnitude and the comparison must be re-measured.

## Reproduce — sandbox (TCP)

```bash
pip install -e . --config-settings=cmake.define.PEERCACHE_NO_RDMA=ON
pip install mooncake-transfer-engine    # optional, for the comparison row

PYTHONPATH=python:benchmarks python benchmarks/run_baseline.py \
    --protocol tcp --block-sizes 4k,16k,64k,256k,1m \
    --batch-size 64 --duration 5 --warmup 1 --tag sandbox
```

## Reproduce — RDMA hardware (publishable numbers)

On a host with a RoCE/InfiniBand NIC (`ibv_devices` to find the device name):

```bash
pip install .                          # RDMA build (needs libibverbs/librdmacm)
pip install mooncake-transfer-engine

PYTHONPATH=python:benchmarks python benchmarks/run_baseline.py \
    --protocol rdma --device-name mlx5_0 \
    --block-sizes 4k,16k,64k,256k,1m,4m \
    --batch-size 64 --threads 1 --mooncake-threads 16 \
    --duration 10 --warmup 2 --tag rdma
```

For a true **cross-node** result (initiator and target on different hosts, each
binding its own NIC), drive each side directly — see
[`benchmarks/README.md`](https://github.com/flymysql/PeerCache/blob/main/benchmarks/README.md)
for the two-node commands and all caveats.

## Caveats

1. **TCP ≠ RDMA.** PeerCache's TCP transport is a pure-Python validation
   fallback; its fast path is C++ libibverbs one-sided READ. Mooncake's TCP
   backend is likewise not its optimized path.
2. **Loopback ≠ network.** Sandbox runs have no NIC and no wire.
3. **Submission models differ** (`--threads` vs `--mooncake-threads`); both are
   recorded in every row.
4. **`store-get` includes directory + pool work**; `transport-read` is the
   closest apples-to-apples PeerCache row to Mooncake's `transfer-engine`.
5. Always publish the `host` block from the JSON artifact next to any figure.
