# 快速开始

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

## 1. 选定服务发现主机（内嵌 meta）

**无需单独启动 meta 进程。** 选定一个节点承担服务发现，并在*每个*节点上把
`discovery_addr` 配置为该节点的 IP。IP 与 `discovery_addr` 相符的节点会在启动时
识别到这一点，并在进程内自动承担服务发现；其余节点作为客户端连接到它。

因此这里唯一的决策是：把哪个节点的 IP 写进 `discovery_addr`。

> 可选：如果你更希望使用一台不承载 SGLang 的专用发现主机，可在该机器上运行
> `peercache-meta --bind 0.0.0.0:9100` 并把 `discovery_addr` 指向它。内嵌行为不受
> 影响 —— 谁的 IP 等于 `discovery_addr` 且能成功绑定端口，谁就承担发现服务。

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
    "discovery_addr": "NODE0_IP:9100",
    "protocol": "rdma",
    "device_name": "mlx5_0",
    "ib_port": 1,
    "gid_index": 3,
    "global_segment_size": "8gb"
  }'
```

## extra_config 参数参考

| 键 | 默认值 | 含义 |
|---|---|---|
| `backend_name` | — | 必须为 `peercache`（dynamic 工厂要求） |
| `module_path` | — | `peercache.store`（必填） |
| `class_name` | — | `PeerCacheStore`（必填） |
| `discovery_addr` | — | 发现主机 `host:port`，所有节点一致；IP 相符的节点自动承担 meta（**必填**） |
| `meta_bind_host` | `0.0.0.0` | 本节点作为发现主机时，内嵌 meta 绑定的网卡接口 |
| `protocol` | `rdma` | `rdma` 或 `tcp`（回退传输） |
| `device_name` | `""` | RDMA 设备，如 `mlx5_0`；为空则取第一个激活设备 |
| `ib_port` | `1` | HCA 端口 |
| `gid_index` | `3` | GID 索引（RoCE v2 通常为 3） |
| `global_segment_size` | `4gb` | 每节点发布池大小（按 tp_size 切分） |
| `vnodes` | `160` | 哈希环上每节点的虚拟节点数 |
| `directory_replicas` | `1` | `> 1` 时复制目录条目以实现高可用 |
| `rdma_port` / `control_port` | `0` | 绑定端口；`0` 表示自动分配 |

## TCP 回退（无 RDMA）

设置 `"protocol": "tcp"` 可在没有 RDMA 硬件的情况下验证完整的发现 + 目录 + 发布池
设计。数据仍会被远程读入目标缓冲区，只是走 TCP 而非单边 RDMA。仅用于功能性测试。

## 运行测试

```bash
pip install pytest
pytest -q          # 使用 TCP 回退；无需 RDMA 硬件
```
