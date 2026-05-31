# 架构

## 主要场景：PD 分离的 SGLang 推理

PeerCache 专为 **prefill/decode（PD）分离**的 SGLang 部署而设计：prefill 与 decode
worker 运行在不同节点上。prefill worker 计算出 prompt 的 KV 缓存，decode worker 需要
拿到这份 KV 缓存才能继续生成。PeerCache 就是把这些 KV 页面在节点间搬运的 L3 存储，
采用 **RDMA 零拷贝**，让 decode 直接从远端主机内存里读出 prefill 的 KV —— 没有中心
master，也不对 KV 做额外的网络拷贝。

```mermaid
flowchart LR
    subgraph P [Prefill 节点 - 生产者]
      P0[Prefill worker<br/>set KV 页面]
    end
    subgraph D [Decode 节点 - 消费者]
      D0[Decode worker<br/>get KV 页面]
    end
    P0 -->|"1 PUT 位置（极小 RPC）"| DIR[(一致性哈希<br/>目录分片)]
    D0 -->|"2 GET 位置（极小 RPC）"| DIR
    P0 ==>|"3 单边 RDMA READ（零拷贝）"| D0
```

- **KV 数据留在 prefill 节点**（生产者）上，只把一条极小的位置记录发布到目录。
- **decode 节点主动拉取** KV：单边 RDMA READ 直接落入它自己已注册的主机缓冲区。
- 它同样适用于非分离场景（任何节点都可既做生产者又做消费者）；PD 分离只是它重点
  调优的场景。

## 控制面与数据面

PeerCache 清晰地分为**控制面**（Python）与**数据面**（C++ / RDMA）。

```mermaid
flowchart TB
    subgraph cp [控制面 - Python, TCP]
      DISC[服务发现: 内嵌 meta]
      RING[一致性哈希环]
      DIR[目录分片 + 客户端]
      POOL[发布池 - LRU]
    end
    subgraph dp [数据面 - C++, libibverbs]
      TE[TransferEngine]
      CM[ConnectionManager - RC QP 池]
      MR[MR 注册表]
    end
    STORE[PeerCacheStore - HiCacheStorage] --> cp
    STORE --> dp
```

## 双 MR 模型

SGLang 的主机 KV 缓冲区是 L2 层，会被 HiCache 驱逐/覆盖，因此其地址不能直接发布到
目录里（会成为悬空引用）。为此每个节点注册**两个内存区域（MR）**：

1. **接收 MR** = `mem_pool_host.kv_buffer` —— `get` 时单边 READ 的目标。
2. **发布池 MR** = 后端自有、带 LRU 的主机内存池 —— 远端节点 READ 的来源。`set` 把
   页面 memcpy 进该池（节点本地、不走网络），并把 `addr + rkey + len` 发布到目录。
   从池中驱逐会删除对应的目录条目，因此已发布的地址在被驱逐前始终有效。

## 写入路径

```mermaid
sequenceDiagram
    participant W as 节点 W（生产者）
    participant Dw as 目录归属者 = hash(key)
    W->>W: set(): 本地 memcpy 页面 -> 发布池 MR
    W->>Dw: PUT key -> {node, addr, rkey, len}
    Note over W,Dw: 数据从不离开 W；只发送一条极小的记录
```

写入开销 = 一次本地 memcpy + 一次小的目录 RPC。没有 master，也没有 KV 数据的网络拷贝。

## 读取路径

```mermaid
sequenceDiagram
    participant R as 节点 R（读取方）
    participant Dr as 目录归属者 = hash(key)
    participant W as 节点 W（数据节点）
    R->>Dr: GET key
    Dr-->>R: {node=W, addr, rkey, len}
    R->>W: 单边 RDMA READ (addr, rkey)
    W-->>R: 字节直接落入 R 的主机缓冲区（零拷贝）
```

如果目录显示数据就在读取方自身，读取会退化为一次本地 `memcpy`，完全不走网络。

## 拷贝次数

核心目标就是尽量减少对（庞大的）KV 数据的拷贝。下面只统计 KV **数据**的搬运（目录
RPC 只有几十字节，忽略不计）：

| 操作 | KV 数据拷贝次数 | 发生了什么 |
|---|---|---|
| `set`（写，生产者） | **1 次主机 memcpy** | 把页面从 SGLang 的主机 KV 缓冲区拷进后端发布池 MR（节点本地，不走网络） |
| `get`（远端读） | **0 次 CPU 拷贝** | 单边 `IBV_WR_RDMA_READ`；网卡把字节从远端发布池直接 DMA 进读取方的主机 KV 缓冲区（真正零拷贝） |
| `get`（数据已在本地） | **1 次主机 memcpy** | 发布池 → 主机 KV 缓冲区；不走网络 |

因此一次「生产者→消费者」的 KV 传输代价是 **写端一次主机 memcpy + 读端一次零拷贝
RDMA READ** —— 数据恰好跨网络一次，且传输期间两端 CPU 都不参与（由网卡完成 DMA）。

### 为什么写端这一次 memcpy 是必要的

SGLang 的主机 KV 缓冲区是 L2 层，会被 HiCache **驱逐/覆盖**。如果直接发布它的地址，
远端 READ 可能落到一个已被复用的页面上（悬空引用 / 数据损坏）。后端自有的发布池由
LRU 管理、与 L2 解耦：发布进去要花一次 memcpy，但能保证 `addr + rkey` 在该条目被池
自身驱逐之前一直有效（驱逐同时会删除目录记录）。这是为正确性付出的标准代价；网络
传输本身依然是零拷贝。

## 一致性哈希目录

- 每个节点承载目录的一个**分片**：本地的 `key -> DataLocation` 映射。所有分片的
  并集构成完整目录；不存在中心存储。
- 虚拟节点环（默认每节点 160 个 vnode）决定每个 key 的归属者，从而让写入方与读取方
  独立地就 key 条目所在位置达成一致。
- `directory_replicas > 1` 会把每条条目写入接下来的 N 个归属者以实现高可用；读取在
  副本之间回退。

## 连接管理

- 连接引导使用极小的 TCP 握手（交换 `QpInfo`：qp_num / psn / lid / gid），将设备选择
  与连接建立完全解耦。随后 QP 经历 INIT → RTR → RTS 状态迁移。
- 每个对端一条 RC QP，惰性创建并池化，避免 O(N²) 的全连接网格。
- 共享完成队列按批次轮询；完成项通过 `wr_id` 匹配到对应请求。

## 故障处理与权衡

- **驱逐竞争**：池驱逐会删除目录条目；任何解析到陈旧/缺失条目的读取都会返回 miss，
  让 SGLang 重新计算（安全降级）。
- **内嵌 meta**：没有专用的 meta 机器。IP 等于 `discovery_addr` 的节点在进程内自动
  承担服务发现（其余节点作为客户端连接）。它只是*服务发现*的单点。成员信息在本地
  缓存，因此短暂的 meta 中断不会影响已建立的读写。若发现主机宕机，在相同 IP 上重启
  即可 —— 期间已连接的对端仍可凭缓存的成员信息继续服务。
- **目录持久性**：单副本时，节点故障会丢失该分片的位置记录（以及本就在该节点上的
  数据）—— 这是可接受的缓存 miss。需要冗余时使用 `directory_replicas > 1`。
