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

## 1. Pick the discovery host (embedded meta)

There is **no separate meta process to launch**. Choose one node to host service
discovery and set `discovery_addr` to that node's IP on *every* node. The node
whose IP matches `discovery_addr` detects this at startup and auto-hosts the
discovery service in-process; all other nodes connect to it as clients.

So the only decision here is: which node's IP goes into `discovery_addr`.

> Optional: if you'd rather run a dedicated discovery host (e.g. a node that does
> not serve SGLang), start one with `peercache-meta --bind 0.0.0.0:9100` and point
> `discovery_addr` at it. The embedded behavior is unaffected — whichever node's
> IP equals `discovery_addr` and is free to bind the port will host it.

## 2. Launch SGLang with the PeerCache backend

PeerCache plugs in through SGLang's **dynamic backend** mechanism — no SGLang
source changes required. Use the **same** `discovery_addr` on all nodes.

```bash
# On the chosen discovery node, NODE0_IP is its own IP -> it hosts meta in-process.
# On every other node, the same NODE0_IP just points them at NODE0.
python -m sglang.launch_server \
  --model-path <model> \
  --enable-hierarchical-cache \
  --hicache-storage-backend dynamic \
  --hicache-storage-backend-extra-config '{
    "backend_name": "peercache",
    "module_path":  "peercache.store",
    "class_name":   "PeerCacheStore",
    "discovery_addr": "NODE0_IP:9100",
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
| `discovery_addr` | — | discovery host `host:port`, same on all nodes; the matching node auto-hosts meta (**required**) |
| `meta_bind_host` | `0.0.0.0` | interface the embedded meta binds when this node is the discovery host |
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
