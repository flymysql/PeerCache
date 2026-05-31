# Getting Started

## Requirements

- Python 3.9+
- For the RDMA data plane: Linux with `rdma-core` / MLNX_OFED development headers
  (`libibverbs`, `librdmacm`), CMake ≥ 3.18, and a C++17 compiler.
- For functional testing without RDMA: nothing extra — the pure-Python TCP
  fallback transport is used automatically.

## Install

```bash
# Linux with RDMA NICs
pip install peercache            # once published to PyPI
# or from source
pip install git+https://github.com/flymysql/PeerCache.git

# Without RDMA (control plane + TCP fallback only, e.g. on a laptop / CI)
pip install -e . --config-settings=cmake.define.PEERCACHE_NO_RDMA=ON
```

PeerCache must be importable from the SGLang process:

```bash
python -c "import peercache; print(peercache.__version__)"
```

## 1. Start the meta (discovery) node

Pick one reachable host. It does service discovery only.

```bash
python -m peercache.examples.launch_meta --bind 0.0.0.0:9100
# or, via the console script:
peercache-meta --bind 0.0.0.0:9100
```

## 2. Launch SGLang with the PeerCache backend

PeerCache plugs in through SGLang's **dynamic backend** mechanism — no SGLang
source changes required.

```bash
python -m sglang.launch_server \
  --model-path <model> \
  --enable-hierarchical-cache \
  --hicache-storage-backend dynamic \
  --hicache-storage-backend-extra-config '{
    "backend_name": "peercache",
    "module_path":  "peercache.store",
    "class_name":   "PeerCacheStore",
    "discovery_addr": "META_IP:9100",
    "protocol": "rdma",
    "device_name": "mlx5_0",
    "ib_port": 1,
    "gid_index": 3,
    "global_segment_size": "8gb"
  }'
```

## extra_config reference

| key | default | meaning |
|---|---|---|
| `backend_name` | — | must be `peercache` (required by the dynamic factory) |
| `module_path` | — | `peercache.store` (required) |
| `class_name` | — | `PeerCacheStore` (required) |
| `discovery_addr` | — | meta node `host:port` (**required**) |
| `protocol` | `rdma` | `rdma` or `tcp` (fallback transport) |
| `device_name` | `""` | RDMA device, e.g. `mlx5_0`; empty = first active |
| `ib_port` | `1` | HCA port |
| `gid_index` | `3` | GID index (RoCE v2 is typically 3) |
| `global_segment_size` | `4gb` | published-pool size per node (sliced by tp_size) |
| `vnodes` | `160` | virtual nodes per node on the hash ring |
| `directory_replicas` | `1` | replicate directory entries for HA when `> 1` |
| `rdma_port` / `control_port` | `0` | bind ports; `0` = auto-assign |

## TCP fallback (no RDMA)

Set `"protocol": "tcp"` to validate the full discovery + directory + pool design
without RDMA hardware. Data is still read remotely into the destination buffer,
just over TCP instead of one-sided RDMA. Use this for functional testing only.

## Run the tests

```bash
pip install pytest
pytest -q          # uses the TCP fallback; no RDMA hardware required
```
