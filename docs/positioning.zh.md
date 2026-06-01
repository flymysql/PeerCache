# 定位与对比

这是一份"我该不该用 PeerCache"的指南:它是什么、与其他 KV 缓存有何差异、**主动舍弃**了什么、
以及适用在哪。

## PeerCache 是什么

PeerCache 是一个**去中心化、点对点、RDMA 零拷贝的 SGLang L3(HiCache)存储后端**。它只做一件
事:**跨请求、跨节点复用 KV(前缀)缓存**——生产节点把 KV 页发布进自己的本地池,并把一条很小的
位置记录写进**分片在所有节点上的一致性哈希目录**;任意节点查到 key 后,用**单边 RDMA READ**
直接零拷贝拉进自己的 buffer。

- **没有中心 master,也没有统一托管的数据池**:目录是 DHT,KV 字节**留在产出它的节点**上。
- **以 `--hicache-storage-backend dynamic` 接入 SGLang**——无需 patch SGLang。

## PeerCache 不是什么

- **不是 PD 搬运引擎**。它不负责每个请求的 prefill→decode KV 交接——那条延迟敏感的 GPU→GPU
  路径是 Mooncake / NIXL 通过 `--disaggregation-transfer-backend` 干的。PeerCache 与之正交。
- **不是中心化存储**。没有需要部署/扩容的 master / 元数据服务。

## 两个正交的维度——别混

| | **KV / 前缀复用**(PeerCache) | **PD 的 P→D 交接**(Mooncake/NIXL) |
|---|---|---|
| 范围 | **跨**请求 / 跨节点 | 单个请求**内** |
| 目标 | 省掉重复计算共享前缀 | 把 prefill 的 KV 交给 decode |
| 延迟 | 缓存型,可容忍主机暂存 | 延迟敏感,GPU→GPU 直传 |
| SGLang 参数 | `--hicache-storage-backend` | `--disaggregation-transfer-backend` |

PD 集群通常**两者都用**:PeerCache 在 prefill 层做前缀复用,Mooncake/NIXL 做 P→D 交接。

## 与中心化 KV 缓存的对比

相对 master 协调 / 中心元数据的 KV 存储(如 Mooncake Store、分布式模式的 LMCache):

| 维度 | 中心化存储 | **PeerCache** |
|---|---|---|
| 元数据 | 中心 master / lookup 服务 | **一致性哈希 DHT,分片到所有节点** |
| 单点故障 | master 是 SPOF / 瓶颈 | **没有中心元数据节点** |
| 元数据吞吐 | 受 master 限制 | **随集群规模扩(每节点 ~1/N)** |
| 数据放置 | 常需拷进托管池 | **留在生产节点本地** |
| 写路径 | 入池 + 协调 | **本地 memcpy + 一条小位置记录** |
| 读路径 | 经存储/引擎 | **单边 RDMA READ,零拷贝** |
| 要运行的服务 | master + worker | **仅内嵌发现(无独立 master)** |
| 扩展 | 给协调者扩容 | **加节点 → 环自动 re-shard** |

## 优势

- **元数据无单点故障/瓶颈**:中心化方案里每次 PUT/GET 都打到 master;PeerCache 把目录分片,
  元数据吞吐随集群增长,无中心热点。
- **写路径轻、数据有局部性**:`set()` = 本地 memcpy + 一条小目录记录,**不拷进中心池**。
- **运维组件更少**:发现服务内嵌(`discovery_addr` 指向的节点自动兼任),**没有 master** 要部署、
  扩容、做 HA。
- **无协调者横向扩展**:新节点同时增容量和元数据吞吐;成员变化自动 re-shard 目录。
- **去中心的故障域**:挂一个节点只丢它那份分片,不会整个元数据服务瘫;`directory_replicas`
  (默认 2)保留副本。
- **精简、SGLang 原生**:紧凑的 C++ 数据面 + Python 控制面,经 `dynamic` HiCache 后端直接挂。

## 主动舍弃了什么

诚实说去中心化设计的代价:

- **成熟度与生态**:Mooncake / LMCache 经过大规模打磨,淘汰/分层/可观测性更全、集成更广;
  PeerCache 更精简、更年轻。
- **全局放置决策**:中心 master 能做更聪明的**全局**淘汰/放置/负载均衡;PeerCache 只做
  "本地 + 哈希"决策。
- **生产者热点与数据冗余**:KV 字节留在生产节点,热 key 可能让该节点成读热点;且 **KV 数据本身
  默认不复制**——生产节点宕了那份页就不可用(目录有副本、有 disk 层兜底,但 KV 字节没多副本)。
  中心池更容易摊平负载、做数据冗余。

## 什么时候用(什么时候别用)

**最契合**

- **聚合式(非 PD)+ 高前缀复用**:系统提示词、few-shot、多轮历史、RAG 文档、Agent 上下文。
  这里 PeerCache 就是完整的共享缓存层——不用传输引擎,挂上即可。
- 想要**类似 Mooncake-Store 的复用能力、但不想再养一个中心 master** 的团队。

**互补**

- **PD 分离集群**:在 **prefill 层**加 PeerCache 做跨节点前缀复用(PD 下 SGLang 的 HiCache 正落在
  这层),P→D 交接仍交给 Mooncake/NIXL。

**更适合选其他方案的情况**

- 需要**成熟、功能丰富、带全局调度和强数据冗余**的存储;或
- 已经在用 **Mooncake 做 PD**,顺手用它的 Store 做复用;或
- 负载**几乎没有前缀复用**(每个请求都唯一)——那任何前缀缓存(含 PeerCache)收益都不大。

## 决策表

| 你的情况 | 建议 |
|---|---|
| 想跨节点复用 KV、最少复杂度、不要 master | **PeerCache(聚合模式)** |
| 需要 P/D 物理解耦(扩缩/SLO) | Mooncake/NIXL 做交接 **+ PeerCache 在 prefill** 做复用 |
| 需要全局放置、丰富特性、强数据 HA | 成熟的中心化存储 |
| 提示词都唯一、无共享前缀 | 任何 KV 复用缓存都帮不上多少 |
