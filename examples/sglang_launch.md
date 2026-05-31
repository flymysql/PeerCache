# Running SGLang with the PeerCache backend

PeerCache plugs into SGLang HiCache as an L3 storage backend using SGLang's
**dynamic backend** mechanism, so **no SGLang source changes are required**. The
factory (`sglang/srt/mem_cache/storage/backend_factory.py`) imports your class by
`module_path` + `class_name` and instantiates it as
`PeerCacheStore(storage_config, kwargs)`.

## 1. Install PeerCache on every node

```bash
# On nodes with RDMA NICs (rdma-core / MLNX_OFED dev headers present):
pip install /path/to/peercache

# On a laptop / CI without RDMA (control plane + TCP fallback only):
pip install -e /path/to/peercache \
  --config-settings=cmake.define.PEERCACHE_NO_RDMA=ON
```

PeerCache must be importable from the SGLang process (`python -c "import peercache"`).

## 2. Choose the discovery host (embedded meta)

There is **no separate meta process**. Pick one node to host service discovery and
put its IP in `discovery_addr` on *every* node. That node detects its own IP at
startup and auto-hosts the discovery service in-process (no data, no metadata
there — discovery only); all other nodes connect to it as clients.

> Optional dedicated host: if you prefer a node that does not serve SGLang, run
> `peercache-meta --bind 0.0.0.0:31998` there and point `discovery_addr` at it.

## 3. Launch each SGLang server

Use the **same** `discovery_addr` on all nodes.

```bash
python -m sglang.launch_server \
  --model-path <model> \
  --enable-hierarchical-cache \
  --hicache-storage-backend dynamic \
  --hicache-storage-backend-extra-config '{
    "backend_name": "peercache",
    "module_path":  "peercache.store",
    "class_name":   "PeerCacheStore",
    "discovery_addr": "NODE0_IP:31998",
    "protocol": "rdma",
    "device_name": "mlx5_0",
    "ib_port": 1,
    "gid_index": 3,
    "global_segment_size": "8gb",
    "vnodes": 160,
    "directory_replicas": 1
  }'
```

You may also pass the config from a file by prefixing the path with `@`:

```bash
--hicache-storage-backend-extra-config @/etc/peercache/extra.json
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

## How it works

```
write:  set() ── local memcpy ──> published-pool MR (this node)
                └── PUT key->{node,addr,rkey,len} ──> directory shard = hash(key)

read:   get() ── GET key ──> directory shard ──> {node,addr,rkey,len}
                └── one-sided RDMA READ ──> SGLang host buffer (zero copy)
                    (or a local memcpy if the data is already on this node)
```

- The host KV buffer (`mem_pool_host.kv_buffer`) is registered as the **receive
  MR** (READ destination).
- A backend-owned **published pool** is registered as the source MR; LRU eviction
  deletes the matching directory entry, so a published address stays valid until
  it is evicted.
- Keys are suffixed exactly like Mooncake (`_<tp>_k` / `_<tp>_v` for MHA,
  `_<pp>_k` for MLA) so TP/PP/MLA shards never collide.

## TCP fallback (no RDMA)

Set `"protocol": "tcp"` to validate the full discovery + directory + pool design
without RDMA hardware. Data is still read remotely into the destination buffer,
just over TCP instead of one-sided RDMA. Use this for functional testing only.
