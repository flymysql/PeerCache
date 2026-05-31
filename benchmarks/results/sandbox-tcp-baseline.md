# PeerCache vs Mooncake baseline

- created: 2026-05-31T04:57:54Z
- protocol: `tcp`
- host: Linux-6.1.147-x86_64-with-glibc2.39 (cpus=4)

| system | path | proto | block | batch | threads | throughput (GB/s) | ops/s | p50 (us) | p99 (us) | note |
|---|---|---|---|---|---|---|---|---|---|---|
| peercache | transport-read | tcp | 4KB | 64 | 1 | **0.144** | 35,165 | 24.5 | 75.2 | one-sided READ, 2 in-process transports |
| peercache | store-get | tcp | 4KB | 64 | 1 | **0.061** | 14,822 | 52.2 | 293.1 | batch_get_v1: directory GET + remote READ |
| mooncake | transfer-engine | tcp | 4KB | 64 | 4 | **0.020** | 4,883 | - | - | official transfer_engine_bench (read) |
| peercache | transport-read | tcp | 16KB | 64 | 1 | **0.465** | 28,396 | 27.2 | 106.5 | one-sided READ, 2 in-process transports |
| peercache | store-get | tcp | 16KB | 64 | 1 | **0.242** | 14,799 | 53.6 | 230.2 | batch_get_v1: directory GET + remote READ |
| mooncake | transfer-engine | tcp | 16KB | 64 | 4 | **0.080** | 4,883 | - | - | official transfer_engine_bench (read) |
| peercache | transport-read | tcp | 64KB | 64 | 1 | **1.138** | 17,363 | 65.2 | 139.1 | one-sided READ, 2 in-process transports |
| peercache | store-get | tcp | 64KB | 64 | 1 | **0.986** | 15,048 | 58.3 | 150.6 | batch_get_v1: directory GET + remote READ |
| mooncake | transfer-engine | tcp | 64KB | 64 | 4 | **0.360** | 5,493 | - | - | official transfer_engine_bench (read) |
| peercache | transport-read | tcp | 256KB | 64 | 1 | **1.890** | 7,209 | 132.9 | 282.7 | one-sided READ, 2 in-process transports |
| peercache | store-get | tcp | 256KB | 64 | 1 | **1.290** | 4,920 | 182.2 | 571.0 | batch_get_v1: directory GET + remote READ |
| mooncake | transfer-engine | tcp | 256KB | 64 | 4 | **1.230** | 4,692 | - | - | official transfer_engine_bench (read) |
| peercache | transport-read | tcp | 1MB | 64 | 1 | **1.677** | 1,599 | 620.5 | 779.5 | one-sided READ, 2 in-process transports |
| peercache | store-get | tcp | 1MB | 64 | 1 | **1.386** | 1,322 | 722.5 | 1182.3 | batch_get_v1: directory GET + remote READ |
| mooncake | transfer-engine | tcp | 1MB | 64 | 4 | **2.840** | 2,708 | - | - | official transfer_engine_bench (read) |
