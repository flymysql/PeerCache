# 性能基准测试

PeerCache 提供一套可复现的基准测试框架（`benchmarks/`），用于测量其 KV-cache 数据平面，
并在**同一负载**下与 [Mooncake](https://github.com/kvcache-ai/Mooncake) 官方的
`transfer_engine_bench` 进行对比。

!!! danger "引用任何数字前必读"
    PeerCache 与 Mooncake 的核心都是 **RDMA 单边 READ**，其标志性性能只有在真实 RDMA
    硬件（RoCE/InfiniBand）上才能体现。本框架同时支持 **TCP 回退**，因此可在笔记本、CI
    或无 GPU 的虚拟机上运行——但 **TCP 数据仅用于验证正确性/链路，绝不能当作 RDMA 性能
    或用于对外宣传。** 要得到可对外发布的数字，请在 RDMA 硬件上运行（见下方步骤）。

## 测量内容

对每个 block size，在相同 batch size 与时长下产出三行：

| 行 | 系统 | 运行内容 |
|---|---|---|
| `transport-read` | PeerCache | 两个 `Transport`，批量单边 READ（纯数据平面） |
| `store-get` | PeerCache | 双节点 `PeerCacheStore`：先 `batch_set_v1` 再 `batch_get_v1`（完整 HiCache 路径） |
| `transfer-engine` | Mooncake | 官方 `transfer_engine_bench`（initiator 从 target 读取） |

吞吐以 GB/s（10⁹ 字节/秒，与 Mooncake 默认单位一致）报告，另含 ops/s 与 PeerCache 单操作延迟分位数。

## 参考基线（TCP 回退，单机——非 RDMA）

在一台 4 vCPU、15 GiB 的 Linux 虚拟机上采集，**无 RDMA 网卡**（`protocol=tcp`，
`127.0.0.1` 回环）。batch size 64，测量 5 秒，预热 1 秒。PeerCache 单线程提交；
Mooncake 自带 bench 使用 4 线程（其默认模型）。

| block | PeerCache `transport-read` (GB/s) | PeerCache `store-get` (GB/s) | Mooncake `transfer-engine` (GB/s) |
|---|---|---|---|
| 4 KB   | 0.144 | 0.061 | 0.020 |
| 16 KB  | 0.465 | 0.242 | 0.080 |
| 64 KB  | 1.138 | 0.986 | 0.360 |
| 256 KB | 1.890 | 1.290 | 1.230 |
| 1 MB   | 1.677 | 1.386 | 2.840 |

PeerCache 单操作延迟（transport-read）p50/p99：4 KB 时 24.5 / 75 µs，1 MB 时升至
621 / 780 µs。原始产物见 `benchmarks/results/`。

!!! warning "如何解读此表"
    这些是**软件路径、单机回环**的数字。它们只说明：框架可端到端运行，且在同一 fabric 上
    PeerCache 的设计相对 Mooncake 没有病态开销——仅此而已。观察：

    - 小/中等 block 下 PeerCache 轻量的进程内路径领先；
    - 1 MB 时 Mooncake 多线程 C++ 传输反超（2.84 对 1.68）。
    - **以上都不代表 RDMA。** 在 RDMA 上两者都会提升 1–2 个数量级，必须重新测量。

## 复现——沙箱（TCP）

```bash
pip install -e . --config-settings=cmake.define.PEERCACHE_NO_RDMA=ON
pip install mooncake-transfer-engine    # 可选，用于对比行

PYTHONPATH=python:benchmarks python benchmarks/run_baseline.py \
    --protocol tcp --block-sizes 4k,16k,64k,256k,1m \
    --batch-size 64 --duration 5 --warmup 1 --tag sandbox
```

## 复现——RDMA 硬件（可发布的数字）

在带 RoCE/InfiniBand 网卡的主机上（用 `ibv_devices` 查询设备名）：

```bash
pip install .                          # RDMA 构建（需 libibverbs/librdmacm）
pip install mooncake-transfer-engine

PYTHONPATH=python:benchmarks python benchmarks/run_baseline.py \
    --protocol rdma --device-name mlx5_0 \
    --block-sizes 4k,16k,64k,256k,1m,4m \
    --batch-size 64 --threads 1 --mooncake-threads 16 \
    --duration 10 --warmup 2 --tag rdma
```

要得到真正的**跨节点**结果（initiator 与 target 位于不同主机、各自绑定网卡），
请分别在两端直接驱动——双节点命令与全部注意事项见
[`benchmarks/README.md`](https://github.com/flymysql/PeerCache/blob/main/benchmarks/README.md)。

## 注意事项

1. **TCP ≠ RDMA。** PeerCache 的 TCP 传输是纯 Python 验证回退；其快路径是 C++ libibverbs
   单边 READ。Mooncake 的 TCP 后端同样不是其优化路径。
2. **回环 ≠ 网络。** 沙箱运行没有网卡、没有网线。
3. **提交模型不同**（`--threads` 对 `--mooncake-threads`），二者在每一行都有记录。
4. **`store-get` 包含目录与池的额外开销**；`transport-read` 才是与 Mooncake
   `transfer-engine` 最对等的 PeerCache 行。
5. 发布任何数字时，请同时附上 JSON 产物中的 `host` 信息。
