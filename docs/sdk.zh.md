# SDK 参考

PeerCache 既可作为 SGLang 的即插即用后端，也可作为库使用。本页记录公开的 Python
API 以及 C++ 数据面绑定。

## `peercache.store.PeerCacheStore`

SGLang 的 `HiCacheStorage` 后端。通常由 SGLang 的 dynamic backend 工厂实例化，也可
直接驱动。

```python
from types import SimpleNamespace
from peercache.store import PeerCacheStore

storage_config = SimpleNamespace(
    tp_rank=0, tp_size=1, pp_rank=0, pp_size=1, is_mla_model=False,
    extra_config={"discovery_addr": "10.0.0.1:31998", "protocol": "rdma",
                  "device_name": "mlx5_0", "global_segment_size": "8gb"},
)
store = PeerCacheStore(storage_config)
store.register_mem_pool_host(mem_pool_host)   # SGLang 会调用它
```

### 主要方法

| 方法 | 说明 |
|---|---|
| `register_mem_pool_host(mem_pool_host)` | 注册接收 MR（主机 KV 缓冲区），并分配 + 注册发布池。 |
| `register_mem_host_pool_v2(host_pool, name)` | 为 v2 传输注册额外的（hybrid）内存池。 |
| `batch_set_v1(keys, host_indices)` | 本地发布页面 + 在目录记录位置。按页返回 `list[bool]`。 |
| `batch_get_v1(keys, host_indices)` | 查询位置 + RDMA READ 进主机缓冲区。按页返回 `list[bool]`。 |
| `batch_exists(keys)` | 从头开始连续存在的页面数量。 |
| `batch_set_v2 / batch_get_v2 / batch_exists_v2` | Hybrid 模型（KV + sidecar 池）路径。 |
| `set / get / batch_set / batch_get / exists` | 单 key / 批量零拷贝（`ptr`+`size`）API。 |
| `clear()` | 清除本地所有已发布条目（池 + 目录）。 |
| `close()` | 拆除服务发现、RPC 服务器与传输。 |

## `peercache.config.PeerCacheConfig`

```python
from peercache.config import PeerCacheConfig

cfg = PeerCacheConfig(
    discovery_addr="10.0.0.1:31998",
    protocol="rdma",            # 或 "tcp"
    device_name="mlx5_0",
    ib_port=1, gid_index=3,
    global_segment_size="8gb",  # 接受 int 或 "4gb"/"512mb"
    vnodes=160,
    directory_replicas=1,
)
# 或从 SGLang extra_config 构造：
cfg = PeerCacheConfig.from_extra_config({"discovery_addr": "10.0.0.1:31998"})
```

## `peercache.server.NodeRuntime`

为单个节点串联控制面 + 数据面：传输、控制 RPC 服务器 + 目录分片、服务发现客户端、
哈希环以及目录客户端。

```python
from peercache.server import NodeRuntime

rt = NodeRuntime(cfg)
rt.start()
print(rt.node_id, rt.local_rdma_endpoint)
print(rt.is_meta)              # 本节点是否承担了内嵌 meta
rt.directory.put({...})        # peercache.directory.DirectoryClient
rt.transport.batch_read([...]) # peercache.transport.Transport
rt.stop()
```

## `peercache.transport`

通用数据面接口，含两种实现。

```python
from peercache.transport import create_transport, Mr, ReadOp

t = create_transport(cfg)                 # RdmaTransport，或 TcpTransport 回退
mr: Mr = t.register_mr(addr, length)      # -> Mr(addr, rkey, lkey)
ok: list[bool] = t.batch_read([
    ReadOp(remote_endpoint="host:port", local_addr=dst,
           remote_addr=src, rkey=mr.rkey, length=n),
])
endpoint = t.local_endpoint()             # 对外公告的 "host:port"
```

- `RdmaTransport` 封装 C++ 的 `_peercache.TransferEngine`（单边 RDMA READ）。
- `TcpTransport` 在 TCP 之上镜像同一套 API，用于无 RDMA 时的测试。

## `peercache.directory`

```python
from peercache.directory import DirectoryServer, DirectoryClient

server = DirectoryServer(); server.attach(rpc_server)   # 承载一个分片
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
client.start()                    # 注册 + 心跳 + 刷新成员列表
client.members()                  # dict[node_id, NodeInfo]
```

> 注意：通常你无需手动创建 `DiscoveryServer` —— `NodeRuntime` 会在 IP 等于
> `discovery_addr` 的节点上自动启动它。

## `peercache.hashring.ConsistentHashRing`

```python
from peercache.hashring import ConsistentHashRing

ring = ConsistentHashRing(vnodes=160)
ring.set_nodes(["n1", "n2", "n3"])
ring.get_node("key")              # 归属节点 node_id
ring.get_nodes("key", 2)          # 副本集合，顺时针，互不相同
```

## `peercache.pool.PublishedPool`

```python
from peercache.pool import PublishedPool

pool = PublishedPool(base_addr, capacity, rkey, on_evict=cb)
remote_addr = pool.publish(key, src_ptr, length)  # 本地 memcpy；过大则返回 None
pool.address_of(key)              # (remote_addr, length) | None
pool.remove(key)
```

## `peercache.diskstore.DiskStore`

L4 磁盘分层：异步写透、按 LRU 约束容量、重启安全的索引。

```python
from peercache.diskstore import DiskStore

d = DiskStore("/data/peercache", max_bytes=100 << 30,
              on_evict=lambda keys: None, node_id="node-0")
d.put("key", b"...page bytes...")     # 异步写透（幂等）
d.get("key")                          # bytes | None（命中后移到 MRU）
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
text = m.render_prometheus()          # Prometheus 文本格式

srv = MetricsServer(m, "0.0.0.0", 31997, dashboard=True)
srv.start()                           # GET /metrics, GET /（可视化页面）, GET /healthz
srv.stop()
```

`PeerCacheStore` 会自动接入：落盘到 `DiskStore`、注册内存池/磁盘/成员数 gauge，并
运行 `MetricsServer`（参见 `extra_config` 中的 `disk_*` 与 `metrics_*` 键）。

## `peercache.types`

- `DataLocation(node_id, rdma_endpoint, remote_addr, rkey, length)` —— 一条目录
  值；`to_dict()` / `from_dict()` 用于线缆序列化。
- `NodeInfo(node_id, control_host, control_port, rdma_host, rdma_port)` —— 节点向
  服务发现公告的信息。

## C++ 绑定：`_peercache.TransferEngine`

通过 pybind11 从 `cpp/` 构建（仅在装有 `libibverbs`/`librdmacm` 的主机上）。

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

在不带 RDMA 构建时（`PEERCACHE_NO_RDMA=ON`），该模块以 `HAS_RDMA == False` 导入，
`create_transport` 会自动选择 TCP 回退。
