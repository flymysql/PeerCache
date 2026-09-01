# SGLang 集成测试

这些测试验证 PeerCache 作为 SGLang HiCache storage backend 的集成正确性。

| 文件 | 需要 | 验证内容 |
|---|---|---|
| `test_sglang_contract.py` | sglang 已安装（无需 GPU） | PeerCacheStore 继承真实 `HiCacheStorage`、v1/v2 接口用真实 `HiCacheStorageConfig`/`PoolTransfer`/`PoolTransferResult` 跑通、`get_stats` 返回真实 `StorageMetrics` |
| `test_sglang_e2e.py` | GPU + sglang + 模型 | 起真实 sglang server（`--hicache-storage-backend dynamic` 挂 PeerCache），发真实请求，断言 PeerCache metrics。支持 `--mode p2p`（目录，校验 write_requests/pool_keys）与 `--mode slotmap`（目录无关，校验 write_requests/bytes_written）；`--keep-server` 保留 server 供后续脚本 |
| `test_sglang_2node_slotmap_read.py` | GPU + sglang + 模型 + 已运行的 slotmap server | 独立 PeerCache 节点 join 真实 sglang 的 slotmap 集群，解析生产者 slot 布局（无目录），用 one-shot bucket READ 把发布的 KV 页跨节点读回并解码 header |

## 本地运行

```bash
# 契约测试（无 GPU，需 pip install sglang）
pytest tests/sglang/test_sglang_contract.py -v

# e2e（需 GPU 和模型），p2p 目录模式（默认）
PEERCACHE_SGLANG_PY=/path/to/python-with-sglang \
PEERCACHE_E2E_MODEL=Qwen/Qwen2.5-0.5B-Instruct \
python tests/sglang/test_sglang_e2e.py --mode p2p

# e2e，slotmap 目录无关模式
python tests/sglang/test_sglang_e2e.py --mode slotmap

# 2-node slotmap 跨节点读（需先起 slotmap server 并写入前缀）
python tests/sglang/test_sglang_e2e.py --mode slotmap --keep-server
python tests/sglang/test_sglang_2node_slotmap_read.py
```

> slotmap 模式下 `pool_keys` 恒为 0（无 published pool，key 直接哈希到物理槽），
> e2e 脚本按模式区分断言；`write_requests` / `bytes_written` 是两种模式的
> 共同正确性证据。

## CI 集成

`.github/workflows/ci.yml` 新增两个 job：

1. **sglang-contract**（ubuntu-latest，无 GPU）：`pip install sglang` 后跑契约测试
2. **sglang-e2e-gpu**（self-hosted GPU runner，可选）：跑完整 e2e

> 说明：GitHub Actions 公共 runner 没有 RDMA 网卡，e2e 用 `protocol=tcp`
> 验证整条控制面链路（discovery → DHT directory → published pool → 跨节点读取）。
> RDMA 零拷贝性能路径需在带 RoCE/IB 的物理机上单独验证。

