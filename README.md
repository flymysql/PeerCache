# PeerCache

[![CI](https://github.com/flymysql/PeerCache/actions/workflows/ci.yml/badge.svg)](https://github.com/flymysql/PeerCache/actions/workflows/ci.yml)
[![Docs](https://github.com/flymysql/PeerCache/actions/workflows/docs.yml/badge.svg)](https://flymysql.github.io/PeerCache/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

A lightweight, peer-to-peer **L3 storage backend for SGLang HiCache**.

Docs: <https://flymysql.github.io/PeerCache/>

PeerCache gives you Mooncake-style RDMA zero-copy KV-cache sharing across nodes,
but **without** the centralized `master` + `metadata` services. Instead it uses:

- **One meta node for service discovery only** — nodes register their endpoint,
  heartbeat, and pull the live membership list.
- **A consistent-hash distributed directory (DHT)** — the mapping
  `key -> {data_node, remote_addr, rkey, length}` is sharded across all nodes by
  hashing the key. There is no central metadata store.
- **Data stays local on write** — `set()` copies the page into a node-local
  *published pool* (a host memcpy, no network, no master) and pushes only a tiny
  location record to the directory.
- **One-sided RDMA READ on read** — `get()` looks up the directory, then issues a
  zero-copy `IBV_WR_RDMA_READ` straight into SGLang's registered host buffer.

```
write:  set() ── local memcpy ──> published pool MR
                └── PUT key->{node,addr,rkey,len} ──> directory shard (hash(key))
read:   get() ── GET key ──> directory shard ──> {node,addr,rkey,len}
                └── one-sided RDMA READ ──> local host buffer (zero copy)
```

## Why simpler than Mooncake?

| | Mooncake | PeerCache |
|---|---|---|
| metadata | central master + metadata service | sharded directory (consistent hash) |
| data placement | dedicated managed pool | stays on producing node |
| coordination | master allocates / tracks objects | only service discovery on meta node |
| transfer | RDMA zero-copy | RDMA zero-copy (one-sided READ) |

## Architecture

- **C++ data plane** (`cpp/`): raw `libibverbs` + `librdmacm`. RC QPs, one-sided
  READ/WRITE, CQ polling, lazy per-peer connection pooling. Exposed to Python via
  `pybind11` as the `_peercache` module.
- **Python control plane** (`python/peercache/`): TCP RPC, service discovery,
  consistent-hash ring, distributed directory, and the published-pool with LRU.
- **TCP fallback transport**: a pure-Python transport that mirrors the RDMA API so
  the design can be validated end-to-end on machines without RDMA hardware.

## Two-MR model (correctness)

SGLang's host KV buffer is the L2 tier and is evicted/overwritten by HiCache, so we
cannot register *its* address into the directory directly (dangling reference). Each
node therefore registers **two memory regions**:

1. **Receive MR** = `mem_pool_host.kv_buffer` — destination of one-sided READ on `get`.
2. **Published pool MR** = a backend-owned host pool with LRU — source of READ on
   remote nodes. `set` memcpys the page into this pool (node-local, no network) and
   publishes its `addr+rkey+len` to the directory. Eviction from the pool deletes the
   corresponding directory entry, so a published address stays valid until evicted.

## Install

```bash
# Linux with RDMA (Mellanox OFED / rdma-core dev headers installed)
pip install .

# Without RDMA (control-plane + TCP fallback only, e.g. for tests on a laptop)
pip install -e . --config-settings=cmake.define.PEERCACHE_NO_RDMA=ON
```

## Run with SGLang

```bash
# 1. start the meta (discovery) node somewhere reachable
python -m peercache.examples.launch_meta --bind 0.0.0.0:9100

# 2. launch each SGLang server with the dynamic backend
python -m sglang.launch_server --enable-hierarchical-cache \
  --hicache-storage-backend dynamic \
  --hicache-storage-backend-extra-config \
  '{"backend_name":"peercache","module_path":"peercache.store","class_name":"PeerCacheStore","discovery_addr":"META_IP:9100","protocol":"rdma","device_name":"mlx5_0","global_segment_size":"4gb"}'
```

See [examples/sglang_launch.md](examples/sglang_launch.md) for details.

## Test

```bash
pip install pytest
PYTHONPATH=python pytest tests/ -v
```

## Maintainer setup (one-time)

- **GitHub Pages**: Settings → Pages → Build and deployment → Source = **GitHub
  Actions**. The `Docs` workflow then publishes to
  <https://flymysql.github.io/PeerCache/> on every push to `main`.
- **PyPI Trusted Publishing**: on the PyPI `peercache` project, add a GitHub
  publisher (owner `flymysql`, repo `PeerCache`, workflow `release.yml`,
  environment `pypi`). Tagging `vX.Y.Z` then builds the sdist, attaches it to a
  GitHub Release, and publishes to PyPI. Until configured, the PyPI step is
  non-blocking and the GitHub Release still ships the package.
