# 更新日志

本项目遵循 [语义化版本](https://semver.org/)。

## [0.8.1] - 2026-06-04

### 新增
- Storage 写入走 **RDMA WRITE** 零拷贝(`data_prepare_writes` →
  `batch_write_multi` → `data_commit_writes`);保留 RPC ingest 回退。
- **`mode=hybrid`** — P2P 与 storage server 同一集群共存。

### 变更
- 目录统一分片于所有节点;storage 数据放置用独立 storage ring。

## [0.8.0] - 2026-06-04

### 新增
- **中心化模式(`mode=centralized`)** — 通过 `peercache-storage-server` 运行专用
  KV 缓存服务器;推理节点设 `"mode": "centralized", "role": "inference"`。写入经
  `data_ingest` RPC;读取仍为 RDMA READ。新增配置 `mode`、`role`;`NodeInfo.role`;
  `storage_nodes` 指标。

## [0.7.1] - 2026-06-02

### 变更
- 多主服务发现现在**把配置的 head**(`discovery_addr` 主机)钉为首席 master(只要它
  存活)——一个稳定、众所周知的引导锚点;其余 master 槽位随节点加入按主机名顺序补齐。
  head 挂掉时,存活 host 仍会补满所有槽位。

## [0.7.0] - 2026-06-02

### 新增
- **多主服务发现——消除单点 meta。** 以前只有一个节点(`discovery_addr` 主机)跑发现
  服务,它挂了就无法新加入、也无故障检测。现在**每个 host 都在集群统一的 meta 端口上
  运行发现服务**,当前 master 是按主机名排序最小的 `max_masters`(默认 3)个存活 host,
  由成员表推导:master 挂掉自动顶替,host 数不足时全员皆主。客户端向当前所有 master
  以及配置的引导 seed(`discovery_addr` 可填逗号分隔列表)注册/心跳并合并成员表;注册
  表是软状态,新晋升/重启的 master 一个心跳周期内自动填满。新增 `max_masters` 配置与
  `DiscoveryClient.master_hosts()`。向后兼容单个 `discovery_addr`。

## [0.6.9] - 2026-06-02

### 修复
- **SGLang 通用 `batch_get` 触发的跨节点读现在真正传输了。** SGLang 传入的本地读目标
  缓冲可能不在已注册的 host KV 池内,导致 `lkey_for(addr)` 返回 0、工作请求被静默跳过
  没上网(`read_failures` 上涨却既无完成错误也无超时)。`RdmaContext` 现在对未注册的
  目标区间**惰性注册并缓存 MR**(`LOCAL_WRITE`);SGLang 复用有界的一组 host 页,缓存
  首次触达后即收敛。新增 `rdma_lazy_local_mrs` 指标。

## [0.6.8] - 2026-06-02

### 新增
- **上网前失败计数**,用于区分"上网后失败"与"根本没发出":`rdma_local_reg_misses`、
  `rdma_post_failures`、`rdma_lease_failures`。

## [0.6.7] - 2026-06-02

### 新增
- **RDMA READ 完成错误可见化。** `drain()` 现在记录失败的 `ibv_wc_status` 并(限流)
  打印 `ibv_wc_status_str`;新增 `rdma_read_wc_errors` / `rdma_last_wc_status` 指标,
  可区分远端访问错误(rkey/MR,状态 10)与重试超限(GID/MTU/路径,12/13)。

## [0.6.6] - 2026-06-02

### 变更
- 心跳日志节流到约 10 秒(成员数变化或掉线重注册仍立即打印);心跳频率本身不变。

## [0.6.5] - 2026-06-02

### 修复
- **`batch_exists` 查错了 keyspace,导致读永远不触发。** SGLang 通用路径用 `batch_set`
  按*原始* key 存,但 `batch_exists` 却按零拷贝 v1/v2 用的*带后缀*分量 key 去查,于是
  预取探测每页都 miss(`exists_pages_found` 恒为 0 而写入上涨),SGLang 从不发起 `get`。
  `batch_exists` / `exists` 现在按当前生效的 keyspace 解析 key,只读节点全 miss 时会
  探另一套 namespace 自愈。

## [0.6.4] - 2026-06-02

### 新增
- **`exists` / L3 预取可观测性**:`exists_requests` 与 `exists_pages_found`,让 SGLang
  预取路径端到端可见。

### 变更
- **`exists` → `get` 复用目录查询。** `batch_exists` 把命中前缀的常驻位置塞进一次性、
  短 TTL 的 handoff 缓存,紧接着的 `batch_get` 消费它,省掉重复的第二次目录 RPC。新增
  `directory_lookups_saved` 计数。

## [0.6.3] - 2026-06-02

### 变更
- **服务发现注册改为无限轮询 meta,不再因超时失败**——比 meta 先启动的节点不再崩溃,
  而是周期性打日志等待,meta 起来后继续。
- 大幅增强发现日志(节点身份、启动时的 master、注册/心跳/剔除/成员变更等)。

## [0.6.2] - 2026-06-02

### 修复
- **通用 value 形式的 `set`/`batch_set`/`get`/`batch_get` 现在可用**——SGLang HiCache
  页备份走 `batch_set(hash_values, data)` 并用 `batch_get(keys, dst_tensors)` 读回;
  之前 PeerCache 只实现了零拷贝形式并 `assert` 崩溃。现在接受 tensor 类对象、bytes、
  numpy 数组或原始 int 指针。
- **v2 注册路径(`register_mem_host_pool_v2`)从未创建发布池**(`pool_capacity_bytes`
  恒为 0);v1/v2 现在共用 `_ensure_published_pool()` / `_register_recv()`。

### 新增
- **"多机示例"**与**"定位与对比"**文档页(中英)。

## [0.6.1] - 2026-06-01

### 修复
- **较新 SGLang 上的 dynamic 后端注册**("Backend class PeerCacheStore must inherit
  from HiCacheStorage"):`HiCacheStorage` 现在单独导入、可选名独立降级,确保
  `PeerCacheStore` 始终是 SGLang 真正基类的子类。

### 变更
- 刷新**整机性能基线**:8 网卡多进程聚合 **273 → 413 GB/s(≈ 3.3 Tbps)**;补充
  GPUDirect 结果(49.5 GB/s)与单卡区间(25–89 GB/s)。

## [0.6.0] - 2026-06-01

### 新增
- **GPUDirect RDMA**:接收缓冲可位于 GPU 显存(dmabuf 走 `ibv_reg_dmabuf_mr`,否则用
  设备 VA 的普通 MR + `nvidia-peermem`);`peercache-bench drive --gpu` 可测。
- **配置校验**(可操作的报错);**数据面指标**(`read_failures`、`rdma_rails`、
  `rdma_read_timeouts`、`rdma_channel_discards`);**目录线格式版本**。
- **性能基线文档页**(中英)含图表。

### 变更
- 幂等关停、先注销再拆除。**目录在成员变更后存活**(每个生产者在 ring 变化时重发布
  自己的页);目录复制默认改为 **2**(`directory_replicas`)。新增 `directory_republishes`。

## [0.5.1] - 2026-05-31

### 修复
- **`peercache-bench serve` 在 ring 成员变更时重发布**,使针对同一长驻 `serve` 的连续
  `drive` 无需重启即可工作。

## [0.5.0] - 2026-05-31

### 新增
- **单进程多轨(多网卡)读。** 一个 `PeerCacheStore` 进程为每个设备开一条轨
  (`device_names="mlx5_0,…"`),并在一次释放 GIL 的调用里把每批单边 READ 跨所有轨条带化
  (`TransferEngine::batch_read_multi`),逼近所有网卡的聚合带宽。`DataLocation` 携带
  逐轨 `rail_endpoints[]` / `rail_rkeys[]`(轨 0 保持线兼容)。`--devices` 加入
  `serve` / `drive`。

### 变更
- `TransferEngine` 内部多轨化;`register_mr` 每轨返回一个句柄;新增 `local_endpoints()`
  / `n_rails()`。

## [0.4.0] - 2026-05-31

### 新增
- **双机(分布式)基准**(`peercache-bench serve` / `drive`)做真正的跨机单边 RDMA
  READ;`drive --processes N` 逃逸 GIL。
- **基准日志**(`--log-level` / `--log-file`);**`directory_read_cache_ttl`**(默认关);
  **`max_channels_per_peer`** 配置;**`PEERCACHE_RDMA_OP_TIMEOUT_MS`**。

### 变更
- **读热路径向量化**(`TransferEngine::batch_read_v`,释放 GIL)。
- **RDMA 调优**:RC QP 使用端口协商的 active MTU;`drain()` 每次批量收割 16 个完成。

### 修复
- **大页基准卡顿**:`HostKVPool.fill_slot` 把逐字节 Python 循环换成模板 `memmove`。
- **RDMA 读卡死**:`drain()` 加入超时(默认 5s)并丢弃超时通道;TCP QP 引导 socket 加超时。

## [0.3.0] - 2026-05-31

### 新增
- **并发多线程读写**:按对端的通道池,每条通道是带独立 CQ 的 RC QP(由
  `max_channels_per_peer` 上限约束,默认 16);TCP socket 池与按调用的控制面 RPC 池。
- **基准套件**(`peercache-bench`):完全按 SGLang HiCache 的方式驱动 `HiCacheStorage`
  接口,报告吞吐与时延尾部。新增 `性能基准测试` 文档页。

### 变更
- 共享客户端状态加锁;损坏的池化连接被关闭。
- **默认端口**迁移到 `31997-31999` 段(metrics `31997`、discovery `31998`);
  `rdma_port`/`control_port` 仍自动分配。

## [0.2.0] - 2026-05-31

### 新增
- **磁盘持久化分层（L4）**：发布页面落盘（`disk_path`,默认 `/data/peercache/`,容量
  上限 `disk_size`,默认 `100GB`）。被淘汰页面以非驻留态保留在目录,之后读取时提升回
  内存池(本地,或对远端读取方通过 `data_promote` RPC);`exists` 命中触发尽力预取。
- **Metrics + 监控**：Prometheus `/metrics` 端点和内嵌 HTML 可视化页面(默认端口
  `31997`)。

### 变更
- `DataLocation` 新增 `resident` 标志。

## [0.1.1] - 2026-05-31

### 变更
- **内嵌 meta**:取消单独 meta 进程的依赖(IP 等于 `discovery_addr` 的节点在进程内
  自动承担;已被 0.7.0 的多主服务发现取代)。

### 新增
- 双语（English / 中文）文档,带语言切换器。

## [0.1.0] - 2026-05-31

首个版本。

### 新增
- 去中心化架构:服务发现 + 按节点分片的一致性哈希分布式目录(DHT)——无中心 master
  或 metadata 服务。
- C++ RDMA 数据面（`libibverbs`、RC QP、单边 `IBV_WR_RDMA_READ`)经 `pybind11` 暴露;
  双 MR 模型;`PeerCacheStore`(`HiCacheStorage`,含 v1/v2 与单 key/批量 API);经
  `dynamic` 后端零侵入接入 SGLang;TCP 回退传输;MkDocs 站点与 CI/文档/发布工作流。

[0.7.1]: https://github.com/flymysql/PeerCache/releases/tag/v0.7.1
[0.7.0]: https://github.com/flymysql/PeerCache/releases/tag/v0.7.0
[0.6.9]: https://github.com/flymysql/PeerCache/releases/tag/v0.6.9
[0.6.8]: https://github.com/flymysql/PeerCache/releases/tag/v0.6.8
[0.6.7]: https://github.com/flymysql/PeerCache/releases/tag/v0.6.7
[0.6.6]: https://github.com/flymysql/PeerCache/releases/tag/v0.6.6
[0.6.5]: https://github.com/flymysql/PeerCache/releases/tag/v0.6.5
[0.6.4]: https://github.com/flymysql/PeerCache/releases/tag/v0.6.4
[0.6.3]: https://github.com/flymysql/PeerCache/releases/tag/v0.6.3
[0.6.2]: https://github.com/flymysql/PeerCache/releases/tag/v0.6.2
[0.6.1]: https://github.com/flymysql/PeerCache/releases/tag/v0.6.1
[0.6.0]: https://github.com/flymysql/PeerCache/releases/tag/v0.6.0
[0.5.1]: https://github.com/flymysql/PeerCache/releases/tag/v0.5.1
[0.5.0]: https://github.com/flymysql/PeerCache/releases/tag/v0.5.0
[0.4.0]: https://github.com/flymysql/PeerCache/releases/tag/v0.4.0
[0.3.0]: https://github.com/flymysql/PeerCache/releases/tag/v0.3.0
[0.2.0]: https://github.com/flymysql/PeerCache/releases/tag/v0.2.0
[0.1.1]: https://github.com/flymysql/PeerCache/releases/tag/v0.1.1
[0.1.0]: https://github.com/flymysql/PeerCache/releases/tag/v0.1.0
