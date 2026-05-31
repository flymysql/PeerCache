# 更新日志

本项目遵循 [语义化版本](https://semver.org/)。

## [0.1.1] - 2026-05-31

### 变更
- **内嵌 meta**：取消了对单独 meta 进程的依赖。IP 等于 `discovery_addr` 的节点现在
  会在进程内自动承担服务发现；同机其他无法绑定该端口的节点会自动回退为客户端模式。

### 新增
- 双语（English / 中文）文档，带语言切换器（`mkdocs-static-i18n`）。

## [0.1.0] - 2026-05-31

首个版本。

### 新增
- 去中心化架构：单个节点仅做服务发现，配合按节点分片的一致性哈希分布式目录（DHT）——
  没有中心化的 master 或 metadata 服务。
- C++ RDMA 数据面：原生 `libibverbs` + TCP QP 引导、RC QP、单边
  `IBV_WR_RDMA_READ`、共享 CQ 轮询、按对端惰性连接池化，通过 `pybind11` 暴露给
  Python（`_peercache`）。
- 双 MR 模型：接收 MR（`mem_pool_host.kv_buffer`）+ 后端自有发布池（LRU，驱逐会
  删除目录条目）。
- `PeerCacheStore`：SGLang 的 `HiCacheStorage` 后端，含 v1 零拷贝路径、v2 hybrid
  池路径，以及单 key / 批量 API。与 Mooncake 兼容的 key 后缀（MHA `_k`/`_v`、MLA
  单 key）。
- 通过 `dynamic` 后端机制实现对 SGLang 的零侵入接入。
- 用于无 RDMA 硬件功能性测试的 TCP 回退传输。
- MkDocs SDK 文档站点，以及用于 CI、文档、发布的 GitHub Actions。

[0.1.1]: https://github.com/flymysql/PeerCache/releases/tag/v0.1.1
[0.1.0]: https://github.com/flymysql/PeerCache/releases/tag/v0.1.0
