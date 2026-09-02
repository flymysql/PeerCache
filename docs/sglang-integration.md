# SGLang 官方 Storage Backend 接入差距分析与完善清单

> 目标：推动 PeerCache 进入 sglang 官方支持的 HiCache storage backend 列表
> （与 file / mooncake / hf3fs / nixl / aibrix / eic / simm / mori / shm 并列），
> 使用户可以 `--hicache-storage-backend peercache` 直接启用（而不是 dynamic + extra-config）。

状态：分析基于 sglang main 分支（含 HiCache v1/v2 接口、DSA/MiniMax sidecar pool、DeepSeek-V4 pool 命名）。
日期：2026-08-31

---

## 1. 官方 backend 的准入形态（先搞清楚"官方"意味着什么）

sglang 的 storage backend 分两类：

| 类型 | 方式 | 代表 |
|---|---|---|
| **内置注册** | `StorageBackendFactory.register_backend(name, module_path, class_name)` 在 `backend_factory.py` 里注册，`--hicache-storage-backend <name>` 直接可用 | file, sim, nixl, mooncake, hf3fs, aibrix, eic, simm, mori, shm |
| **动态加载** | `--hicache-storage-backend dynamic` + `--hicache-storage-backend-extra-config` 指定 module_path/class_name | PeerCache 当前用法 |

**"进入官方列表" = 在 `backend_factory.py` 加一行 `register_backend("peercache", "peercache.store", "PeerCacheStore")`，并在 `server_args.py` 的 `choices` 列表里加 `"peercache"`。** 这需要：

1. 合入一个 sglang PR（把 PeerCache 作为可选的第三方依赖，`import` 失败时优雅降级——参考 mooncake/eic 的 `_import_*` 模式）
2. 通过 upstream review：要有单测、契约测试、CI 注册、文档

---

## 2. 差距清单（PeerCache 现状 vs 官方准入要求）

### 2.1 接口完整性 ✅ 基本齐备，需补强

sglang `HiCacheStorage`（main 分支）抽象接口：

| 接口 | PeerCache 现状 | 差距 |
|---|---|---|
| `register_mem_pool_host` | ✅ | — |
| `register_mem_host_pool_v2` | ✅（`registered_pools` dict） | — |
| `batch_set_v1` / `batch_get_v1` | ✅（零拷贝，已实测） | — |
| `batch_exists` | ✅（返回 int 前缀命中） | — |
| `batch_set_v2` / `batch_get_v2` | ✅（PoolTransfer dict 结果） | 需补 v2 的 **TRAILING_PAGES 读路径测试**（Mamba/SWA） |
| `batch_exists_v2` | ✅（PoolTransferResult + hit_policy） | 需补 **DeepSeek-V4 pool 命名**（`DEEPSEEK_V4_C4/C128/...`）的兼容测试 |
| `get` / `batch_get` / `set` / `batch_set`（value 路径） | ✅ | — |
| `exists` / `batch_exists` | ✅ | — |
| `clear` | ✅ | — |
| `get_stats` | ❌ **未实现**（基类有默认） | **需实现**：返回 `StorageMetrics()`（prefetch_pgs 等），mooncake 已实现，review 会要求 |
| `close` | ✅ | — |

### 2.2 注册与配置 ✅ 需一个 PR

- [ ] `backend_factory.py`：`StorageBackendFactory.register_backend("peercache", "peercache.store", "PeerCacheStore")`
- [ ] `server_args.py`：`choices` 加 `"peercache"`
- [ ] 依赖策略：PeerCache 必须是**可选依赖**（sglang 不强制装 peercache），backend 创建时 `try: import peercache` 失败给清晰报错（参考 mooncake 的 `_import_mooncake_store`）
- [ ] 版本适配：PeerCache 的 `store.py` 已做 `try: from sglang.srt.mem_cache.hicache_storage import HiCacheStorage` 降级——**反向兼容**（PeerCache 装在不同 sglang 版本上）也要维护

### 2.3 不同模型架构的接口差异（用户特别关心）

sglang 调度层**按架构选择 v1 / v2 + sidecar pool**，PeerCache 必须逐项验证：

| 架构 | 用的接口 | PeerCache 状态 | 待验证 |
|---|---|---|---|
| 普通 MHA/GQA | `batch_set_v1`/`batch_get_v1`（KV 两组件 `_k`/`_v`） | ✅ 已实测 | — |
| MLA（DeepSeek V3/R1） | v1，单组件 `_k`（`mla_suffix` 空） | ✅ `is_mla_model` 分支已有 | 需 MLA 模型实测 |
| **DSA（DeepSeek Sparse Attention）** | v1 KV + **v2 INDEXER sidecar pool**（`_get_extra_pools` 注入 `PoolTransfer(INDEXER, ALL_PAGES, indices_from_pool=KV)`） | ✅ **main 分支 v2 契约实测**（`test_main_v2_contract.py`：INDEXER roundtrip + ALL_PAGES clamp） | 需 DSA 模型真机 e2e |
| **MiniMax 稀疏 KV** | 同 DSA：INDEXER sidecar | ✅ 同上 | MiniMax 模型实测 |
| **Mamba / SWA** | v2，`TRAILING_PAGES` hit_policy（只查尾部 N 页） | ✅ **main 分支 v2 契约实测**（TRAILING window 语义，含 store 实现 bug 修复） | Mamba 模型实测 |
| **DeepSeek-V4** | 多 pool：`DEEPSEEK_V4_C4`、`C4_INDEXER`、`C128`、`C4_STATE`、`C128_STATE` 等 | ✅ **已实现+实测**：`_pack_multi_buffer`/`_scatter_multi_buffer`（1 key → N buffer，mooncake `_pack_multi_buffer_meta` 等价物），main 契约测试跨节点字节一致（C4=2buf、C128=3buf） | 需 DeepSeek-V4 权重真机 e2e |
| **Draft KV（投机解码）** | `DRAFT` / `DRAFT_INDEXER` / `DRAFT_SWA` pools | ✅ **已实现+实测**：`_v2_component_keys` 按 draft pool 自身类型选 suffix（MHA→`_k`+`_v` 双组件，MLA→`_k` 单组件，镜像 mooncake），main 契约测试 MHA/MLA 双路径跨节点字节一致 | 投机解码 + HiCache 真机 e2e |

**结论**：v1/v2 接口 + sidecar pool + TRAILING + DeepSeek-V4/DRAFT pool 命名
已通过 main 分支真实接口的契约测试（`tests/sglang/test_main_v2_contract.py`，
4/4 过），剩余的是各架构**真机 e2e**（需对应模型权重）。

### 2.4 测试与 CI（upstream review 硬门槛）

| 项 | 现状 | 需要 |
|---|---|---|
| 契约测试 | ✅ `tests/test_hicache_contract.py` 13 项（含 v2 sidecar、TRAILING、prefix 隔离、get_stats/check_server） | — |
| 单元测试 | ✅ 71 项（TCP 传输） | — |
| **sglang release 契约** | ✅ `tests/sglang/test_sglang_contract.py`（真实 0.5.9，15 passed + 1 skip） | — |
| **sglang main v2 契约** | ✅ `tests/sglang/test_main_v2_contract.py`（main 分支 PoolName/PoolTransfer/PoolHitPolicy，4/4 过，含 DSA INDEXER / Mamba TRAILING / DeepSeek-V4+DRAFT 命名） | — |
| **sglang 集成 e2e** | ✅ `tests/sglang/test_sglang_e2e.py`（L20 实测 PASS：write_requests=7/pool_keys=42） | — |
| **CI workflow** | ✅ `ci.yml`：`sglang-contract`（无 GPU）+ `sglang-e2e-gpu`（self-hosted 可选） | — |
| sglang 侧测试注册 | ✅ `sglang-pr/`（registered 测试 + backend_factory/server_args 补丁） | 合入 sglang 时落地 |

### 2.5 运维能力（官方后端都有）

| 能力 | mooncake/nixl 等 | PeerCache | 差距 |
|---|---|---|---|
| 配置加载 | `load_from_extra_config` / env / file | `PeerCacheConfig.from_extra_config` ✅ | — |
| 健康检查 | `check_server()` / `warmup()` | ❌ | **需实现** `check_server()`（启动时探测 discovery/RDMA 可用性，失败给明确报错） |
| 监控 metrics | Prometheus | ✅ `:31997/metrics` | — |
| 运行时挂载/卸载 | `attach_storage_backend`/`detach`（HTTP admin API） | ✅ `close()` 有 | 需验证 detach 后重 attach 的 clean 状态 |
| 多租户/tag | mooncake `_tag_keys`（config_prefix） | ❌ | **需实现** key 前缀隔离（多模型/多实例共用集群时防碰撞） |
| 磁盘 tier | mooncake 有 | ✅ L4 disk（默认 100GB） | — |
| 文档 | 每个后端 README + docs | ✅ `docs/` + `examples/sglang_launch.md` | 需补"官方接入"用法（`--hicache-storage-backend peercache`） |

### 2.6 性能与正确性（review 深水区）

- [ ] **RDMA 数据面正确性**：公共 CI 无 RDMA，需物理 RoCE 机验证（rkey/MR/跨节点 one-sided READ）——PeerCache 仓库已有 `HAS_RDMA` 检测，建议加 `rdma` 标记的集成测试（CI 可跳过）
- [ ] **故障注入**：节点崩溃后 directory 重分片、读失败回退（`read_failures` metrics）、磁盘满
- [ ] **并发/压力**：多线程并发 set/get、prefetch 风暴下的 rate limiting 配合
- [ ] **多节点真实部署**：≥3 节点 sglang + router（`demo-multinode.md` 的 TCP 版自动化）

---

## 3. 建议的实施路线（分 4 步，每步可独立合入/验收）

### Step 1：补齐接口与运维能力（PeerCache 仓库内，不发 PR 也能做）
1. 实现 `get_stats()`（返回 `StorageMetrics`）
2. 实现 `check_server()` / `warmup()`（启动自检）
3. 实现 key 前缀隔离（`_tag_keys` 等价物，config `prefix`）
4. 契约测试扩展到 v2 + sidecar（`batch_exists_v2` PoolTransferResult、hit_policy 语义）
5. CI：加"契约测试 job（装 sglang 最新版跑 contract）"+ "GPU e2e job（self-hosted 可选，TCP protocol）"

### Step 2：架构矩阵验证（PeerCache 仓库内，需要各架构模型）
| 架构 | 模型 | 验证点 |
|---|---|---|
| MLA | Qwen3 系列（MLA） | v1 单组件 key |
| DSA | DeepSeek-V3.2-Sparse（如有内网镜像） | INDEXER sidecar v2 |
| Mamba/SWA | Mamba-2 / MiniMax | TRAILING_PAGES v2 |
| DeepSeek-V4 | 内网权重 | 多 pool 命名对齐 |
| 投机解码 | 任意 + draft | DRAFT pool |

产出：每架构一个 `tests/sglang/test_<arch>_backend.py` + 实测报告。

**真机验证进展（2026-09，RoCE GPU 环境，见 [roce-validation.md](roce-validation.md)）**：
- **DeepSeek-V4**：HostPoolGroup（anchor + `swa`/`c4`/`c4_indexer`/`c128`/`c4_state`/`c4_indexer_state`）
  全部注册，`batch_set_v2` 6 pool publish 全成功（真机）。**关键参数 `"interface_v1": 1`**
  （否则 sglang 不走 `batch_set_v1` 零拷贝，KV 落通用路径）。
- **DSA/Mamba/Draft**：契约级通过（INDEXER/TRAILING/DRAFT pool）；真机需对应权重
  （V3.2 643GB 单机放不下，模型库无 Mamba/Draft）。
- **V4 读回**：读路径完整触发（URT prefetch → `_storage_hit_query` → `batch_exists_v2`），
  跨节点命中确认需 hash 一致场景（torch≥2.13 + 新版 sglang）。

### Step 3：sglang 合入 PR（对 upstream）
1. `backend_factory.py` + `server_args.py` 注册 `peercache`
2. 可选依赖策略（sglang 不强制依赖 peercache）
3. `test/registered/hicache/test_hicache_storage_peercache_backend.py`（仿 mooncake 的 `register_cuda_ci` 注册，跑在 base-b）
4. docs：`docs/docs/advanced_features/hicache.mdx` 加 peercache 段落

### Step 4：长期运营
- 跟进 sglang 接口演进（PoolName 归一化 PR、v2 扩展）
- RDMA 物理机 CI（自建 runner + RoCE）
- 与 Mooncake 的 A/B 对比基准（`bench/` 已有 mooncake 对比行）

---

## 4. 验收标准（Definition of Done）

- [ ] `--hicache-storage-backend peercache` 一行启用（无 extra-config）
- [ ] 契约测试（含 v2/sidecar）在 CI 上绿
- [ ] ≥3 种架构（普通 MLA / DSA / Mamba）的集成测试有实测结果
- [ ] `get_stats` / `check_server` / key 前缀隔离 已实现并有测试
- [ ] sglang PR 合入（backend_factory 注册 + server_args choices + registered 测试）
- [ ] 文档更新（PeerCache README + sglang hicache.mdx）

---

## 附：本次 L20 验证的环境事实（可复用）

- 机器：`21.91.201.69.devcloud.woa.com:36000`（root 免密）
- env：`/data/envs/sgl311`（py3.11 + torch 2.9.1+cu128 + sglang 0.5.9 + peercache 0.8.2）
- 关键 hack：`LD_PRELOAD=/data/peercache-e2e/stub/greenctx_stub.so`（驱动 535 缺 CUDA 12.8+ 符号）
- 启动脚本：`/data/peercache-e2e/start_server.sh`（A:30000）/ `start_server_b.sh`（B:30001）
- 已验证：契约 8/8、双节点 members=2、真实请求 write_requests=8/pool_keys=40、跨节点 batch_set/get_v1 字节一致

