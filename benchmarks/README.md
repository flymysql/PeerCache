# PeerCache vs Mooncake benchmark harness

A reproducible harness for measuring PeerCache's KV-cache data plane and
comparing it, under an identical workload, with
[Mooncake](https://github.com/kvcache-ai/Mooncake)'s official
`transfer_engine_bench`.

> [!IMPORTANT]
> **Read this before quoting any number.** The benchmark measures whatever
> fabric you run it on. Both PeerCache and Mooncake are built around
> **RDMA one-sided READ**; their headline performance only shows up on real
> RDMA hardware (RoCE/InfiniBand). The harness *also* runs over a TCP fallback
> so it can be exercised on a laptop / CI / a GPU-less VM — but **TCP numbers
> are for plumbing validation only and must never be presented as RDMA
> performance or used in marketing.** See [Caveats](#caveats).

## What it measures

For each block size in a sweep, three rows are produced:

| row | system | what runs | what it represents |
|---|---|---|---|
| `transport-read` | PeerCache | two `Transport`s, batched one-sided READ | PeerCache's raw data-plane read |
| `store-get` | PeerCache | 2-node `PeerCacheStore`: `batch_set_v1` then `batch_get_v1` | the full HiCache path SGLang drives (directory GET + remote READ) |
| `transfer-engine` | Mooncake | official `transfer_engine_bench` (initiator reads from target) | Mooncake's raw data-plane read |

Reported per row: throughput (GB/s, 10⁹ bytes/s — same unit as Mooncake's
default), ops/s, and (for PeerCache) per-op latency p50/p90/p99.

## Layout

```
benchmarks/
  common.py          # workload, latency percentiles, result schema, renderers
  bench_peercache.py # PeerCache transport-read + store-get
  bench_mooncake.py  # wraps Mooncake's transfer_engine_bench (subprocess)
  run_baseline.py    # orchestrates the sweep, writes results/*.json + *.md
  results/           # generated artifacts (committed sandbox baseline lives here)
```

## Install

```bash
# PeerCache (RDMA build on a host with libibverbs/librdmacm)
pip install .

# PeerCache without RDMA (TCP fallback only)
pip install -e . --config-settings=cmake.define.PEERCACHE_NO_RDMA=ON

# Mooncake (only needed for the comparison rows)
pip install mooncake-transfer-engine
```

## Run — sandbox (TCP, no RDMA)

This is what was used to produce the committed baseline. It validates the
harness end-to-end; **the numbers are not representative of RDMA**.

```bash
PYTHONPATH=python:benchmarks python benchmarks/run_baseline.py \
    --protocol tcp \
    --block-sizes 4k,16k,64k,256k,1m \
    --batch-size 64 --duration 5 --warmup 1 --tag sandbox
```

If Mooncake's wheel can't load its CUDA/RDMA shared objects on a GPU-less box,
either add `--skip-mooncake`, or provide stub libraries (the wheel links
`libcuda.so.1`, `libcudart.so.12`, `libibverbs.so.1` even for `protocol=tcp`):

```bash
# libcudart from pip; libcuda + ibverbs via distro / a tiny stub
pip install nvidia-cuda-runtime-cu12
sudo apt-get install -y rdma-core libibverbs1
export LD_LIBRARY_PATH=/path/to/cudart/lib:/path/to/stubs:$LD_LIBRARY_PATH
```

## Run — RDMA hardware (this is what produces publishable numbers)

You need two nodes (or two NICs) on a RoCE/InfiniBand fabric. Run the same
sweep with `--protocol rdma` and your device name (`ibv_devices` to list).

PeerCache spins up its own two in-process engines bound to the local device, so
the single command works on one RDMA node:

```bash
PYTHONPATH=python:benchmarks python benchmarks/run_baseline.py \
    --protocol rdma --device-name mlx5_0 \
    --block-sizes 4k,16k,64k,256k,1m,4m \
    --batch-size 64 --threads 1 --mooncake-threads 16 \
    --duration 10 --warmup 2 --tag rdma
```

For a true **cross-node** Mooncake number (initiator and target on different
hosts), drive Mooncake's bench directly so each side binds its own NIC:

```bash
# node A (target / producer)
transfer_engine_bench -mode=target  -protocol=rdma -device_name=mlx5_0 \
    -metadata_server=http://<meta-host>:8080/metadata \
    -local_server_name=<A-ip>:13300 -use_vram=false -gpu_id=-1

# node B (initiator / consumer)
transfer_engine_bench -mode=initiator -protocol=rdma -device_name=mlx5_0 \
    -metadata_server=http://<meta-host>:8080/metadata \
    -local_server_name=<B-ip>:13301 -segment_id=<A-ip>:13300 \
    -operation=read -block_size=65536 -batch_size=64 -threads=16 -duration=10
```

and run PeerCache cross-node via two `PeerCacheStore` processes pointed at one
`discovery_addr` (see `examples/sglang_launch.md`), publishing on one node and
`batch_get_v1` on the other.

## Knobs

| flag | meaning | default |
|---|---|---|
| `--protocol` | `tcp` or `rdma` | `tcp` |
| `--device-name` | RDMA device (rdma only) | `""` |
| `--block-sizes` | comma list, accepts `4k`,`1m` | `4096,65536,1048576` |
| `--batch-size` | reads per batch | `64` |
| `--threads` | PeerCache submit threads | `1` |
| `--mooncake-threads` | Mooncake submit threads | `4` |
| `--duration` / `--warmup` | seconds | `5` / `1` |
| `--skip-mooncake` / `--skip-store` | drop those rows | off |
| `--tag` | output filename suffix | `""` |

## Caveats

1. **TCP ≠ RDMA.** PeerCache's TCP transport is a *pure-Python validation
   fallback*; its production fast path is the C++ libibverbs one-sided READ,
   which is disabled in a no-RDMA build. Mooncake's TCP backend is likewise not
   its optimized path. TCP-loopback ranking does **not** predict RDMA ranking.
2. **Loopback ≠ network.** Sandbox runs use `127.0.0.1` on one host (no NIC, no
   wire). RRDMA runs should be cross-node to capture real fabric behaviour.
3. **Different submission models.** PeerCache's TCP `batch_read` is synchronous
   per op on a single connection; Mooncake's bench is multi-threaded by design.
   Keep `--threads` / `--mooncake-threads` in mind when comparing — they are
   reported in every row.
4. **`store-get` vs `transfer-engine` are not identical paths.** `store-get`
   includes directory lookup + pool bookkeeping; `transfer-engine` is pure
   transport. `transport-read` is the closest apples-to-apples PeerCache row.
5. Numbers vary with CPU, NUMA, NIC, MTU, and concurrency. Always publish the
   `host` block from the JSON artifact alongside any figure.
