# 性能基准测试

PeerCache 提供一套系统化的基准测试框架（`benchmarks/`），它**完全按照 SGLang HiCache 的方式**
调用 PeerCache 的 `HiCacheStorage` 接口，从而产出可对外发布的性能数据：吞吐（pages/s、
tokens/s、GB/s）与延迟尾部（p50/p95/p99/p999/max），并覆盖一系列**线程模型**（并发度）的扫描，
包括满负载下的**饱和点 / 峰值（PEAK）吞吐**。

!!! danger "RDMA 优先——引用任何数字前必读"
    PeerCache 的价值在于 **RDMA 单边 READ**。标志性数字必须在带 RDMA 网卡的主机上以
    `--protocol rdma` 测得。框架内置的纯 Python TCP 回退**仅用于功能冒烟测试**（CI／笔记本）；
    **TCP 运行不是性能场景，绝不能对外发布。**

## 模拟内容：PD 解耦

```
prefill 节点  --batch_set_v1-->  发布 KV 页     （写 / offload）
decode 节点   --batch_exists-->  探测缓存前缀    （查找）
              --batch_get_v1-->  经 RDMA 加载页  （读 / 预取，零拷贝）
```

框架会拉起一个嵌入式发现服务和两个 `PeerCacheStore` 节点：**producer**（prefill）发布页，
**consumer**（decode）跨 fabric 读回——这正是 SGLang 驱动的完整路径（目录查找 + RDMA READ
直接写入已注册的 host buffer）。页布局忠实于 SGLang：`--layout mla`（每页 1 个对象）或
`--layout mha`（k+v，每页 2 个对象）。

## 模式

| 模式 | 回答的问题 |
|---|---|
| `latency` | 单个在途操作的延迟尾部（并发 1、batch 1）——单页延迟 |
| `throughput` | 单一固定线程模型下的稳态吞吐 + 尾延迟 |
| `saturation` | 并发扫描下的吞吐/延迟曲线 + 峰值 PEAK |
| `suite` | 完整基线：延迟 + get/set/exists 饱和扫描，写入 `results/` |

## 指标

| 指标 | 含义 |
|---|---|
| page | 一个逻辑 KV 页（MLA 1 个对象，MHA 为 k+v） |
| pages/s · tokens/s | 每秒页数；`tokens/s = pages/s × tokens_per_page` |
| GB/s | 实际搬运的组件负载字节/秒（10⁹） |
| p50…p999 / max | 每次 **batch 调用**的延迟（单页延迟请用 `latency` 模式） |
| hit% | 读路径上命中的页比例 |
| PEAK | 稳态吞吐最高的并发行 |

## 在 RDMA 硬件上运行

```bash
pip install .                 # RDMA 构建（需 libibverbs/librdmacm）

PYTHONPATH=python:benchmarks python benchmarks/bench_hicache.py suite \
    --device-name mlx5_0 --layout mla \
    --page-size 131072 --tokens-per-page 64 \
    --batch-size 32 --concurrencies 1,2,4,8,16,32,64 \
    --duration 10 --warmup 2 --tag rdma
```

会写出 `benchmarks/results/hicache-suite-rdma-<ts>.{json,md}`。若要得到真正的**跨双机**结果
（而非单机网卡 loopback），请在一台节点跑 producer `PeerCacheStore`、另一台跑 consumer，
二者指向同一 `discovery_addr`；详见
[`benchmarks/README.md`](https://github.com/flymysql/PeerCache/blob/main/benchmarks/README.md)。

## 基线结果模板

请用你 RDMA 运行的 `results/*.md` 填写下表（此处刻意留空——数字必须来自你的硬件，而非沙箱）：

| op | layout | page | batch | threads | pages/s | tokens/s | GB/s | p50 µs | p99 µs | p999 µs |
|---|---|---|---|---|---|---|---|---|---|---|
| get（延迟） | mla | 128 KB | 1 | 1 | | | | | | |
| get（峰值） | mla | 128 KB | 32 | _N_ | | | | | | |
| set（峰值） | mla | 128 KB | 32 | _N_ | | | | | | |
| exists（峰值） | mla | 128 KB | 32 | _N_ | | | | | | |

发布任何数字时，请同时附上 JSON 产物中的 `host` 与 `meta` 信息（设备、布局、页大小、batch、并发）。

## 可选：与 Mooncake 对比

```bash
PYTHONPATH=python:benchmarks python benchmarks/run_baseline.py \
    --protocol rdma --device-name mlx5_0 \
    --block-sizes 4k,16k,64k,256k,1m --duration 10 --tag rdma
```

在相同 block size 下，将 PeerCache 数据平面微基准与 Mooncake 官方 `transfer_engine_bench` 并列测量。

## 注意事项

1. **TCP ≠ RDMA**，且 TCP 不是性能场景——仅用于验证代码可运行。
2. **loopback ≠ 网络**：单机 RDMA 走网卡 loopback；要测 fabric 行为请跨节点运行。
3. 除非使用 `latency` 模式（batch 1），延迟均为**每次 batch 调用**的延迟。
