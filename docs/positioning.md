# Positioning & comparison

This page is the "should I use PeerCache?" guide: what it is, how it differs
from other KV caches, what it deliberately gives up, and where it fits.

## What PeerCache is

PeerCache is a **decentralized, peer-to-peer, RDMA zero-copy L3 (HiCache)
storage backend for SGLang**. Its one job is **cross-request, cross-node KV
(prefix) cache reuse**: a producing node publishes KV pages into its own local
pool plus a tiny location record into a **consistent-hash directory sharded
across all nodes**; any node looks the key up and pulls the bytes with a
**one-sided RDMA READ** straight into its registered buffer.

- **No central master, no managed data pool.** The directory is a DHT; the KV
  bytes stay on the node that produced them.
- **Plugs into SGLang as `--hicache-storage-backend dynamic`** — no SGLang patch.

## What PeerCache is *not*

- **Not a PD transfer engine.** It does not move the per-request prefill→decode
  KV handoff; that latency-critical GPU→GPU path is what Mooncake / NIXL do via
  `--disaggregation-transfer-backend`. PeerCache is orthogonal to it.
- **Not a centralized store (by default).** P2P mode has no master / managed data
  pool. Set `mode=centralized` to run dedicated storage servers
  (`peercache-storage-server`) that hold KV bytes and directory shards while
  inference nodes are clients — a Mooncake Store–like layout without a separate
  metadata master.

## Two orthogonal axes — don't conflate them

| | **KV / prefix reuse** (PeerCache) | **PD P→D handoff** (Mooncake/NIXL) |
|---|---|---|
| Scope | *Across* requests / nodes | *Within* one request |
| Goal | Skip recomputing shared prefixes | Hand prefill's KV to decode |
| Latency | Cache-style; host staging OK | Latency-critical; GPU→GPU direct |
| SGLang knob | `--hicache-storage-backend` | `--disaggregation-transfer-backend` |

A PD cluster typically uses **both**: PeerCache for prefix reuse on the prefill
tier, and Mooncake/NIXL for the P→D handoff.

## How PeerCache compares to centralized KV caches

Compared with master-coordinated / centralized-metadata KV stores
(e.g. Mooncake Store, LMCache in distributed mode):

| Dimension | Centralized stores | **PeerCache** |
|---|---|---|
| Metadata | Central master / lookup service | **Consistent-hash DHT, sharded over all nodes** |
| Single point of failure | Master is a SPOF / bottleneck | **No central metadata node** |
| Metadata throughput | Bounded by the master | **Scales with cluster size (~1/N per node)** |
| Data placement | Often copied into a managed pool | **Stays on the producing node** |
| Write path | Pool insert + coordination | **Local memcpy + one small location record** |
| Read path | Through the store/engine | **One-sided RDMA READ, zero copy** |
| Services to run | Master + workers | **Embedded discovery only (no separate master)** |
| Scaling | Re-scale the coordinator | **Add a node → ring re-shards automatically** |

## Advantages

- **No metadata single point of failure or bottleneck.** Every PUT/GET in a
  centralized design hits the master; PeerCache shards the directory, so
  metadata throughput grows with the cluster and there is no central hotspot.
- **Light write path with data locality.** `set()` is a local memcpy plus a tiny
  directory record — no copy into a central pool.
- **Fewer moving parts to operate.** Discovery is embedded and multi-master —
  every host runs it, with the `discovery_addr` head pinned and up to `max_masters`
  active — so there is no master to deploy, scale, or keep HA, and no single meta
  to lose.
- **Horizontal scaling without a coordinator.** New nodes grow both capacity and
  metadata throughput; membership changes re-shard the directory automatically.
- **Decentralized failure domain.** Losing a node loses only its shard, not the
  whole metadata service; `directory_replicas` (default 2) keeps a replica.
- **Lean and SGLang-native.** A compact C++ data plane + Python control plane,
  dropped in via the `dynamic` HiCache backend.

## What we deliberately give up

Being honest about the trade-offs of a fully decentralized design:

- **Maturity & ecosystem.** Mooncake / LMCache are battle-tested at scale with
  richer eviction/tiering/observability and broad integrations. PeerCache is
  leaner and younger.
- **Global placement decisions.** A central master can make smarter *global*
  eviction / placement / load-balancing; PeerCache decides locally + by hash.
- **Producer hotspots & data redundancy.** KV bytes stay on the producing node,
  so a hot key can make that node a read hotspot, and **data itself is not
  replicated by default** — if the producer is down, that page is unavailable
  (the directory is replicated and a disk tier exists, but the KV bytes are not).
  A central pool spreads load and replicates data more easily.

## When to use PeerCache (and when not)

**Best fit**

- **Aggregated (non-PD) clusters with high prefix sharing** — system prompts,
  few-shot, multi-turn chat history, RAG documents, agent contexts. PeerCache is
  the complete shared-cache layer here: no transfer engine, just plug it in.
- Teams that want **Mooncake-Store-like reuse without running a central master**
  (P2P mode), or who prefer **explicit storage servers** (`mode=centralized`).

**Complementary**

- **PD-disaggregated clusters**: add PeerCache on the **prefill tier** for
  cross-node prefix reuse (where SGLang's HiCache lives in PD), while
  Mooncake/NIXL still handle the P→D handoff.

**Prefer the alternatives when**

- You need a **mature, feature-rich store** with global scheduling and strong
  data redundancy, or
- You are **already running Mooncake** for PD and want its Store for reuse too,
  or
- Your workload has **little prefix sharing** (every request unique) — then any
  prefix cache, PeerCache included, adds little.

## Decision guide

| Your situation | Recommendation |
|---|---|
| Want cross-node KV reuse, least complexity, no master | **PeerCache P2P (aggregated mode)** |
| Want dedicated KV pool servers, inference stays thin | **PeerCache centralized** (`peercache-storage-server` + `mode=centralized`) |
| Need P/D physical split for scaling / SLO | Mooncake/NIXL for handoff **+ PeerCache on prefill** for reuse |
| Need global placement, rich features, strong data HA | A mature centralized store |
| Unique prompts, no shared prefixes | A KV reuse cache (any) won't help much |
