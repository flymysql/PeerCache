# SDK Reference

PeerCache is usable both as a drop-in SGLang backend and as a library. This page
documents the public Python API and the C++ data-plane binding.

## `peercache.store.PeerCacheStore`

The SGLang `HiCacheStorage` backend. Normally instantiated by SGLang's dynamic
backend factory, but can be driven directly.

```python
from types import SimpleNamespace
from peercache.store import PeerCacheStore

storage_config = SimpleNamespace(
    tp_rank=0, tp_size=1, pp_rank=0, pp_size=1, is_mla_model=False,
    extra_config={"discovery_addr": "10.0.0.1:31998", "protocol": "rdma",
                  "device_name": "mlx5_0", "global_segment_size": "8gb"},
)
store = PeerCacheStore(storage_config)
store.register_mem_pool_host(mem_pool_host)   # SGLang calls this
```

### Key methods

| method | description |
|---|---|
| `register_mem_pool_host(mem_pool_host)` | Register the receive MR (host KV buffer) and allocate + register the published pool. |
| `register_mem_host_pool_v2(host_pool, name)` | Register an extra (hybrid) pool for v2 transfers. |
| `batch_set_v1(keys, host_indices)` | Publish pages locally + record locations in the directory. Returns `list[bool]` per page. |
| `batch_get_v1(keys, host_indices)` | Look up locations + RDMA READ into host buffer. Returns `list[bool]` per page. |
| `batch_exists(keys)` | Number of consecutive existing pages from the start. |
| `batch_set_v2 / batch_get_v2 / batch_exists_v2` | Hybrid-model (KV + sidecar pools) paths. |
| `set / get / batch_set / batch_get / exists` | Single-key/batch zero-copy (`ptr`+`size`) APIs. |
| `clear()` | Drop all locally published entries (pool + directory). |
| `close()` | Tear down discovery, RPC server, and transport. |

## `peercache.config.PeerCacheConfig`

```python
from peercache.config import PeerCacheConfig

cfg = PeerCacheConfig(
    discovery_addr="10.0.0.1:31998",
    protocol="rdma",            # or "tcp"
    device_name="mlx5_0",
    ib_port=1, gid_index=3,
    global_segment_size="8gb",  # accepts int or "4gb"/"512mb"
    vnodes=160,
    directory_replicas=2,
    max_masters=3,              # head + up to 2 more hosts act as discovery masters
    disk_enabled=True,
    disk_path="/data/peercache/",
    disk_size="100gb",
)
# or from SGLang extra_config:
cfg = PeerCacheConfig.from_extra_config({"discovery_addr": "10.0.0.1:31998"})
```

## `peercache.server.NodeRuntime`

Wires the control + data planes for one node: transport, control RPC server +
directory shard, discovery client, hash ring, and directory client.

```python
from peercache.server import NodeRuntime

rt = NodeRuntime(cfg)
rt.start()
print(rt.node_id, rt.local_rdma_endpoint)
rt.directory.put({...})        # peercache.directory.DirectoryClient
rt.transport.batch_read([...]) # peercache.transport.Transport
rt.stop()
```

## `peercache.transport`

Common data-plane interface with two implementations.

```python
from peercache.transport import create_transport, Mr, ReadOp

t = create_transport(cfg)                 # RdmaTransport, or TcpTransport fallback
mr: Mr = t.register_mr(addr, length)      # -> Mr(addr, rkey, lkey)
ok: list[bool] = t.batch_read([
    ReadOp(remote_endpoint="host:port", local_addr=dst,
           remote_addr=src, rkey=mr.rkey, length=n),
])
endpoint = t.local_endpoint()             # "host:port" to advertise
```

- `RdmaTransport` wraps the C++ `_peercache.TransferEngine` (one-sided RDMA READ).
- `TcpTransport` mirrors the same API over TCP for testing without RDMA.

## `peercache.directory`

```python
from peercache.directory import DirectoryServer, DirectoryClient

server = DirectoryServer(); server.attach(rpc_server)   # hosts a shard
client = DirectoryClient(ring, resolve_control, replicas=1)
client.put({key: DataLocation(...)})
locs = client.get([key])          # list[DataLocation | None]
present = client.exists([key])    # list[bool]
client.delete([key])
```

## `peercache.discovery`

```python
from peercache.discovery import DiscoveryServer, DiscoveryClient

meta = DiscoveryServer("0.0.0.0", 31998); meta.start()

client = DiscoveryClient(addr, node_info, on_members=cb, heartbeat_interval=2.0)
client.start()                    # register + heartbeat + refresh membership
client.members()                  # dict[node_id, NodeInfo]
```

## `peercache.hashring.ConsistentHashRing`

```python
from peercache.hashring import ConsistentHashRing

ring = ConsistentHashRing(vnodes=160)
ring.set_nodes(["n1", "n2", "n3"])
ring.get_node("key")              # owner node_id
ring.get_nodes("key", 2)          # replica set, clockwise, distinct
```

## `peercache.pool.PublishedPool`

```python
from peercache.pool import PublishedPool

pool = PublishedPool(base_addr, capacity, rkey, on_evict=cb)
remote_addr = pool.publish(key, src_ptr, length)  # local memcpy; None if too big
pool.address_of(key)              # (remote_addr, length) | None
pool.remove(key)
```

## `peercache.diskstore.DiskStore`

The L4 disk tier: async write-through, LRU-bounded capacity, restart-safe index.

```python
from peercache.diskstore import DiskStore

d = DiskStore("/data/peercache", max_bytes=100 << 30,
              on_evict=lambda keys: None, node_id="node-0")
d.put("key", b"...page bytes...")     # async write-through (idempotent)
d.get("key")                          # bytes | None (moves to MRU)
d.exists("key")                       # bool
used_bytes, num_keys = d.stats()
d.remove("key"); d.close()
```

## `peercache.metrics`

```python
from peercache.metrics import Metrics, MetricsServer

m = Metrics(node_id="node-0")
m.record_read(hit=True, nbytes=4096, seconds=0.0003, source="remote")  # local/remote/disk
m.record_write(nbytes=4096, seconds=0.0002)
m.set_gauge_provider("pool_bytes_used", lambda: pool.bytes_used)
text = m.render_prometheus()          # Prometheus exposition

srv = MetricsServer(m, "0.0.0.0", 31997, dashboard=True)
srv.start()                           # GET /metrics, GET / (dashboard), GET /healthz
srv.stop()
```

`PeerCacheStore` wires these automatically: it spills to a `DiskStore`, registers
pool/disk/membership gauges, and runs a `MetricsServer` (see `extra_config` keys
`disk_*` and `metrics_*`).

## `peercache.types`

- `DataLocation(node_id, rdma_endpoint, remote_addr, rkey, length)` — a directory
  value; `to_dict()` / `from_dict()` for the wire.
- `NodeInfo(node_id, control_host, control_port, rdma_host, rdma_port)` — what a
  node advertises to discovery.

## C++ binding: `_peercache.TransferEngine`

Built from `cpp/` via pybind11 (only on hosts with `libibverbs`/`librdmacm`).

```python
import _peercache
assert _peercache.HAS_RDMA
eng = _peercache.TransferEngine(device_name="mlx5_0", ib_port=1, gid_index=3,
                                bind_host="0.0.0.0", bind_port=0)
mr = eng.register_mr(addr, length)        # -> MrHandle(addr, length, lkey, rkey)
reqs = [_peercache.ReadRequest(remote_node="host:port", local_addr=dst,
                               remote_addr=src, rkey=mr.rkey, length=n)]
ok = eng.batch_read(reqs)                 # list[bool]
endpoint = eng.local_endpoint()
```

When built without RDMA (`PEERCACHE_NO_RDMA=ON`), the module imports with
`HAS_RDMA == False` and `create_transport` automatically selects the TCP fallback.
