# 快速开始

PeerCache 是面向 **PD 分离（prefill/decode 分离）的 SGLang 推理**的跨节点 KV 缓存
传输层：prefill 节点发布 KV 页面，decode 节点通过 RDMA 读回。数据流与拷贝次数详见
[架构](architecture.md)。

## 环境要求

- Python 3.9+
- RDMA 数据面：Linux，安装 `rdma-core` / MLNX_OFED 开发头文件
  （`libibverbs`、`librdmacm`），CMake ≥ 3.18，以及支持 C++17 的编译器。
- 无 RDMA 的功能性测试：无需额外依赖 —— 会自动使用纯 Python 的 TCP 回退传输。

## 安装

```bash
# 带 RDMA 网卡的 Linux
pip install peercache            # 发布到 PyPI 后
# 或从源码安装
pip install git+https://github.com/flymysql/PeerCache.git

# 无 RDMA（仅控制面 + TCP 回退，例如笔记本 / CI）
pip install -e . --config-settings=cmake.define.PEERCACHE_NO_RDMA=ON
```

PeerCache 必须能被 SGLang 进程导入：

```bash
python -c "import peercache; print(peercache.__version__)"
```

## 1. 选定服务发现 head（内嵌多主）

**无需单独启动 meta 进程，也没有单点故障。** 选定一个节点的 IP 作为引导 **head**，
并在*每个*节点上把 `discovery_addr` 配置为它。之后服务发现会自动复制:

- **每个 host** 都在进程内运行服务发现;
- **head 被钉为首席 master**;随着节点加入,再按主机名顺序提升后续 host,凑满
  `max_masters`(默认 **3**)个主;
- 非 head 的 master 挂掉会自动由下一个 host 顶上;节点数不足时全员皆主。注册表是
  软状态,新晋升/重启的 master 一个心跳周期内自动重新填满。

因此这里唯一的决策是：把哪个节点的 IP 作为 head 写进 `discovery_addr`。也可以填
逗号分隔的多个 seed(`"ip1:31998,ip2:31998"`),这样即使 head 挂了,全新节点仍能引导。

> 可选：如果你更希望使用一台不承载 SGLang 的专用发现主机，可在该机器上运行
> `peercache-meta --bind 0.0.0.0:31998` 并把 `discovery_addr` 指向它。

## 2. 用 PeerCache 后端启动 SGLang

PeerCache 通过 SGLang 的 **dynamic backend** 机制接入 —— 无需改动 SGLang 源码。
所有节点使用**相同**的 `discovery_addr`。

```bash
# 在选定的发现节点上，NODE0_IP 即其自身 IP -> 它在进程内承担 meta。
# 在其余每个节点上，相同的 NODE0_IP 只是把它们指向 NODE0。
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
    "disk_enabled": true,
    "disk_path": "/data/peercache/",
    "disk_size": "100gb"
  }'
```

> 磁盘分层(L4)**默认开启**。`disk_path` 必须在每个节点上可写(各自使用一个
> `node_id` 子目录);建议指向一块大而快的本地盘(NVMe)。设 `"disk_enabled": false`
> 可只用内存池。每节点总容量 ≈ `global_segment_size`(内存) + `disk_size`(磁盘)。

## 3. 中心化模式(可选 — 专用 KV 缓存服务器)

默认 PeerCache 为 **P2P**。专用 **storage server** 可与 P2P 节点**同一集群**
共存:`mode=hybrid`(P2P+storage)或 `mode=centralized`(推理节点仅作客户端)。
hybrid 下 **`write_policy`** 默认 `local`(只写本地,与 P2P 相同);可选 `storage`(只写
storage)或 `both`(双写:storage+本地副本)。

1. 启动存储服务器(无需 SGLang):

```bash
peercache-storage-server \
  --discovery-addr NODE0_IP:31998 \
  --global-segment-size 64gb \
  --disk-path /data/peercache/
```

2. SGLang 推理节点增加 `"mode": "centralized", "role": "inference"`。

写入走 `data_ingest` RPC;读取仍为 RDMA READ。中心化模式下推理节点不分配本地 published pool。

## 部署拓扑（PD 分离）

一个典型的 PD 分离集群：

```mermaid
flowchart LR
    subgraph prefill [Prefill 池]
      PF0["node-0（发现 head + master）"]
      PF1[node-1]
    end
    subgraph decode [Decode 池]
      DC0[node-2]
      DC1[node-3]
    end
    PF1 & DC0 & DC1 -. 注册/心跳 .-> PF0
    DC0 & DC1 ==>|RDMA READ KV| PF0 & PF1
```

经验法则：

- 在每个 prefill 和 decode 节点上运行**相同**的 PeerCache 后端配置，且
  **`discovery_addr` 处处一致**。
- 为 `discovery_addr` 选定某个节点的 IP（任意可达节点，通常是某个 prefill 节点）。
  它是被钉住的 head;每个 host 都运行一个发现 master、最多 `max_masters` 个生效,
  因此没有单点 meta 可丢,无需另外启动任何东西。
- 按每个节点应常驻多少已发布 KV 来设置 `global_segment_size`（它会按 `tp_size`
  切分）；池越大命中率越高，但锁定的主机内存也越多。
- 生产用 `protocol: rdma`；`protocol: tcp` 仅用于功能性测试。
- 所有节点之间必须能互相访问 RDMA 端口、控制端口（`rdma_port` / `control_port`，
  默认自动分配）以及发现端口。

## extra_config 参数参考

必填项（dynamic 工厂需要前三项）：

| 键 | 默认值 | 含义 |
|---|---|---|
| `backend_name` | — | 必须为 `peercache`（dynamic 工厂要求） |
| `module_path` | — | `peercache.store`（必填） |
| `class_name` | — | `PeerCacheStore`（必填） |
| `discovery_addr` | — | 引导 head `host:port`(或逗号分隔的 seed 列表)，**所有节点一致**；head 被钉为首席发现 master,每个 host 都运行一个 master（**必填**） |

RDMA / 传输：

| 键 | 默认值 | 含义 |
|---|---|---|
| `protocol` | `rdma` | `rdma`（生产）或 `tcp`（测试用回退传输） |
| `device_name` | `""` | RDMA 设备，如 `mlx5_0`；为空则取第一个激活设备 |
| `ib_port` | `1` | HCA 端口 |
| `gid_index` | `3` | GID 索引（RoCE v2 通常为 3） |
| `max_channels_per_peer` | `16` | 每个对端的最大并发数据面通道数（RDMA 为 QP+CQ；TCP 回退为 socket）。限制对单个对端的并行读取数；超出的线程会短暂等待空闲通道 |

容量 / 放置：

| 键 | 默认值 | 含义 |
|---|---|---|
| `global_segment_size` | `4gb` | 每节点发布池（内存）大小（接受 `int` 或 `"8gb"`/`"512mb"`；按 `tp_size` 切分） |
| `vnodes` | `160` | 一致性哈希环上每节点的虚拟节点数 |
| `directory_replicas` | `2` | 把目录条目复制到 N 个归属者,使单节点丢失时在重分片完成前不丢条目 |
| `directory_read_cache_ttl` | `0` | 把已解析的常驻读位置缓存 N 秒,在热点静态工作集上跳过每批目录查询(`0`=关闭;读 miss 时失效) |
| `max_masters` | `3` | 发现 master 数量;head 加上后续 host(按主机名排序)为主,挂掉自动顶替(小集群则全员皆主) |

磁盘持久化分层（L4）：

| 键 | 默认值 | 含义 |
|---|---|---|
| `disk_enabled` | `true` | 把被淘汰的页面落盘，并在读取时提升回内存（若 `disk_path` 无法创建则优雅降级） |
| `disk_path` | `/data/peercache/` | 数据落盘目录（每个节点使用一个 `node_id` 子目录） |
| `disk_size` | `100gb` | 每节点磁盘容量（按 LRU 约束；接受 `int` 或 `"100gb"`） |

监控（metrics + 可视化页面）：

| 键 | 默认值 | 含义 |
|---|---|---|
| `metrics_enabled` | `true` | 启动 metrics 服务（Prometheus `/metrics` + 可视化页面） |
| `metrics_port` | `31997` | metrics/可视化 HTTP 端口（若已被占用，例如同机多 rank，则自动禁用） |
| `metrics_bind_host` | `0.0.0.0` | metrics 服务绑定接口 |
| `metrics_dashboard` | `true` | 同时在 `/` 提供内置 HTML 可视化页面 |

网络 / 身份（一般无需修改）：

| 键 | 默认值 | 含义 |
|---|---|---|
| `meta_bind_host` | `0.0.0.0` | 内嵌发现 master 绑定的网卡接口（每个 host 都在 meta 端口上运行一个） |
| `local_hostname` | 自动 | 对外公告的 IP；自动解析为能到达 `discovery_addr` 的本机 IP |
| `rdma_bind_host` | `0.0.0.0` | RDMA 数据面绑定接口 |
| `rdma_port` | `0` | RDMA 引导端口；`0` 表示自动分配 |
| `control_bind_host` | `0.0.0.0` | 控制 RPC 服务器绑定接口 |
| `control_port` | `0` | 控制 RPC 端口；`0` 表示自动分配 |
| `node_id` | 自动 | 稳定的节点标识；由 `local_hostname` + 随机后缀自动生成 |
| `heartbeat_interval` | `2.0` | 成员心跳间隔（秒） |
| `member_ttl` | `6.0` | master 将静默节点剔除前的等待秒数 |

## 持久化与监控

开启磁盘分层后（默认开启），每个节点会把发布的页面落盘到
`disk_path/<node_id>/`，并在读取时提升回内存，因此有效容量约等于内存
（`global_segment_size`）+ 磁盘（`disk_size`）。详见[架构](architecture.md)。

每个节点默认还会提供 metrics：

```bash
# Prometheus 抓取目标
curl http://NODE_IP:31997/metrics
# 浏览器打开内置可视化页面
open http://NODE_IP:31997/
```

把 Prometheus 指向 `NODE_IP:31997`（或抓取每个节点）即可绘制命中率、吞吐、时延
p50/p99 以及内存/磁盘用量。详见[架构](architecture.md)。

## TCP 回退（无 RDMA）

设置 `"protocol": "tcp"` 可在没有 RDMA 硬件的情况下验证完整的发现 + 目录 + 发布池
设计。数据仍会被远程读入目标缓冲区，只是走 TCP 而非单边 RDMA。仅用于功能性测试。

## 运行测试

```bash
pip install pytest
pytest -q          # 使用 TCP 回退；无需 RDMA 硬件
```
