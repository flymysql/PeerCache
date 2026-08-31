# 向 sglang 合入 PeerCache 的 PR 材料

本目录包含把 PeerCache 注册为 sglang **官方** HiCache storage backend 所需的全部
补丁与说明。目标：用户可用 `--hicache-storage-backend peercache` 一行启用。

> 分析依据：`docs/sglang-integration.md`（差距清单与路线）。

---

## 1. 改动清单（对 sglang 仓库）

| 文件 | 改动 | 说明 |
|---|---|---|
| `python/sglang/srt/mem_cache/storage/backend_factory.py` | +1 行 `register_backend("peercache", ...)` **+ `_create_builtin_backend` 加分支** | 与 file/mooncake 并列 |
| `python/sglang/srt/server_args.py` | `choices` 加 `"peercache"` | `--hicache-storage-backend` 直接可选 |
| `test/registered/hicache/test_hicache_storage_peercache_backend.py` | 新文件 | 仿 mooncake，`register_cuda_ci` 注册到 base-b |
| `docs/docs/advanced_features/hicache.mdx` | 加 peercache 段落 | 官方文档 |

### 1.1 backend_factory.py 补丁（两处！）

```python
# A) 在文件末尾的注册区追加：
StorageBackendFactory.register_backend(
    "peercache",
    "peercache.store",
    "PeerCacheStore",
)

# B) 在 _create_builtin_backend 的 elif 链里加分支（否则 create_backend 会
#    落入 else 抛 "Unknown built-in backend"）：
    elif backend_name == "peercache":
        return backend_class(storage_config, mem_pool_host)
```

PeerCacheStore 的构造签名是 `__init__(self, storage_config=None, extra=None)`，
与 mooncake 的 `(storage_config, mem_pool_host)` 位置兼容（mem_pool_host 会作为
`extra` 传入，PeerCache 会把它合并进 extra_config；如需更精确可改成
`return backend_class(storage_config, extra=mem_pool_host)`）。

注意：sglang **不强制依赖 peercache**。`_load_backend_class` 的 ImportError
已带清晰信息；如需更友好报错，可捕获时提示 `pip install peercache`。

### 1.2 server_args.py 补丁

```python
choices=[
    "file", "sim", "nixl", "mooncake", "hf3fs", "aibrix", "eic",
    "simm", "mori", "shm", "peercache",
],
```

---

## 2. registered 测试（test/registered/hicache/）

模板：`test_hicache_storage_mooncake_backend.py`（base mixin + setUpClass 起
server + eval 断言）。PeerCache 不需要外部服务（embedded discovery），实现更简单。
关键点：

- `register_cuda_ci(est_time=..., stage="base-b", runner_config="2-gpu-large")`
- `HiCacheStorageBaseMixin` 复用（file backend 的 base），覆写 server 启动参数：
  ```
  --enable-hierarchical-cache
  --hicache-write-policy write_through
  --hicache-ratio 1.05
  --hicache-storage-backend dynamic
  --hicache-storage-backend-extra-config '{"backend_name":"peercache",
     "module_path":"peercache.store","class_name":"PeerCacheStore",
     "discovery_addr":"127.0.0.1:31998","protocol":"tcp",...}'
  ```
- 无 RDMA：CI runner 用 `protocol=tcp`（控制面完整，数据面走 TCP）
- 断言：server 起后发共享前缀请求，检查 PeerCache `:31997/metrics`
  `write_requests>0` / `pool_keys>0`

> PeerCache 仓库内已提供等价实现：`tests/sglang/test_sglang_e2e.py`（可在
> self-hosted runner 直接跑）；本目录提供 sglang 侧 registered 版本。

---

## 3. 提交说明（PR description 草稿）

```
## Add PeerCache as a built-in HiCache storage backend

PeerCache (github.com/flymysql/PeerCache) is a lightweight P2P L3 KV-cache
backend for HiCache: embedded service discovery (no meta process), a
consistent-hash distributed directory (no central metadata), and one-sided
RDMA READ on get() (zero-copy). TCP fallback validates the full control plane
without RDMA hardware.

This PR:
- registers `peercache` in StorageBackendFactory (--hicache-storage-backend peercache)
- adds the choice to server_args
- adds test/registered/hicache/test_hicache_storage_peercache_backend.py

PeerCache is an optional dependency: users `pip install peercache` separately;
sglang itself does not depend on it. The backend subclasses HiCacheStorage and
implements v1 (batch_set/get_v1) + v2 (batch_set/get/exists_v2 incl. sidecar
pools and PoolHitPolicy) interfaces.

Contract tests (PeerCache repo, tests/sglang/test_sglang_contract.py) run
against a real sglang install on CPU runners; e2e test runs on a GPU runner
with protocol=tcp.
```

---

## 4. 合入前 Checklist

- [ ] PeerCache 契约测试（`tests/sglang/test_sglang_contract.py`）绿
- [ ] PeerCache 全套件（`tests/`）绿
- [ ] sglang 侧 `python -m pytest test/registered/hicache/test_hicache_storage_peercache_backend.py` 在 GPU runner 绿
- [ ] `--hicache-storage-backend peercache` 一行启用验证（不需要 extra-config）
- [ ] docs/hicache.mdx 更新
- [ ] upstream review：接口命名、错误处理、可选依赖策略

