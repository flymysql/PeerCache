# PeerCache

**Peer-to-peer RDMA zero-copy L3 KV-cache backend for SGLang HiCache.**

PeerCache gives you Mooncake-style RDMA zero-copy KV-cache sharing across nodes,
but **without** the centralized `master` + `metadata` services. It is built for
**PD-disaggregated (prefill/decode) SGLang inference**: prefill workers publish KV
pages, and decode workers read them back over RDMA with zero CPU copies.

```mermaid
flowchart LR
    subgraph nodeW [Node W - writer / embedded meta]
      D[Embedded discovery:<br/>register / heartbeat / members]
      PW[Published pool MR]
      DW[Directory shard]
    end
    subgraph nodeR [Node R - reader]
      HR[Host KV buffer - recv MR]
      DR[Directory shard]
    end
    nodeR -. register/heartbeat .-> D
    nodeW -->|"PUT key to hash(key) owner"| DR
    nodeR -->|"GET key from hash(key) owner"| DR
    nodeR ==>|one-sided RDMA READ| PW
```

## Why PeerCache?

| | Mooncake | PeerCache |
|---|---|---|
| metadata | central master + metadata service | sharded directory (consistent hash) |
| data placement | dedicated managed pool | stays on the producing node |
| coordination | master allocates / tracks objects | only service discovery, embedded in a node |
| transfer | RDMA zero-copy | RDMA zero-copy (one-sided READ) |

## Core ideas

- **Embedded discovery, no separate meta node** — you set `discovery_addr` to one
  node's IP on every node; that node auto-hosts the discovery service in-process.
  Nodes register, heartbeat, and pull the live membership list. No data and no
  metadata live there.
- **Consistent-hash directory (DHT)** — the mapping
  `key -> {data_node, remote_addr, rkey, length}` is sharded across all nodes by
  hashing the key.
- **Data stays local on write** — `set()` copies the page into a node-local
  *published pool* (a host memcpy, no network, no master) and pushes only a tiny
  location record to the directory.
- **One-sided RDMA READ on read** — `get()` looks up the directory, then issues a
  zero-copy `IBV_WR_RDMA_READ` straight into SGLang's registered host buffer.
- **Disk persistence tier (L4)** — pages evicted from memory spill to disk
  (default `/data/peercache/`, `100GB`) and are promoted back into the pool on a
  later read, locally or by a remote reader.
- **Built-in monitoring** — a Prometheus `/metrics` endpoint plus an embedded HTML
  dashboard (default port `31997`): hit rate, throughput, latency p50/p99,
  memory/disk usage, and more.

## Next steps

- [Getting Started](getting-started.md) — install and run with SGLang.
- [Architecture](architecture.md) — the two-MR model, the directory, and the
  read/write data flows.
- [SDK Reference](sdk.md) — the Python and C++ APIs you can build on.
