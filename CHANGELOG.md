# Changelog

All notable changes to PeerCache are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/) and this project adheres to
[Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.5.0] - 2026-05-31

### Added
- **Single-process multi-rail (multi-NIC) reads.** A single `PeerCacheStore`
  process can now drive several RDMA devices at once. Set
  `device_names="mlx5_bond_1,…,mlx5_bond_8"` (or `--devices` on the benchmark)
  and the engine opens one rail (its own `RdmaContext` + `ConnectionManager`)
  per device, registers every buffer on all rails, and **stripes each batch of
  one-sided READs across all rails inside one GIL-released C++ call**
  (`TransferEngine::batch_read_multi`), so a single process can approach the
  aggregate bandwidth of all its NICs instead of being capped by one card / the
  GIL. Rails pair by index across nodes, so `--devices` must list the same
  devices in the same order on both ends.
  - `DataLocation` now carries per-rail `rail_endpoints[]` / `rail_rkeys[]` for
    the published-pool MR (the legacy single `rdma_endpoint`/`rkey` is kept as
    rail 0, so the directory wire format stays backward compatible).
  - `--devices` added to `peercache-bench serve` / `drive`.

### Changed
- `TransferEngine` is now multi-rail internally (a single device behaves exactly
  as before). `register_mr` returns one handle per rail; new `local_endpoints()`
  and `n_rails()`.

## [0.4.0] - 2026-05-31

### Added
- **Two-host (distributed) benchmark** (`peercache-bench serve` / `drive`): runs
  the producer (data node) and consumer (driver) as separate processes on two
  machines so the GET path exercises a genuine cross-host one-sided RDMA READ,
  instead of the in-process `127.0.0.1` loopback used by `suite`/`micro`. The
  producer publishes only after its readers join the ring (stable directory
  sharding); the consumer runs the get/exists concurrency sweep. `--local-host`
  auto-detects the NIC that routes to `--discovery-addr`. `drive --processes N`
  (paired with `serve --readers N`) runs N reader processes to escape the GIL
  for full-load benchmarking.
- **Benchmark logging**: global `--log-level` (default `warning`) and
  `--log-file` on `peercache-bench`, so the `PeerCacheStore up: ... rdma=…`
  transport-selection line and the `using TCP fallback` warning are visible.
- **`directory_read_cache_ttl`** (default `0`/off): caches resolved *resident*
  read locations for N seconds to skip the per-batch directory lookup on hot,
  static working sets; invalidated on a read miss and TTL-bounded. Exposed as
  `drive --dir-cache-ttl`.
- **`max_channels_per_peer`** is now a real `PeerCacheConfig` field (was a
  hardcoded 16); exposed as `drive --max-channels`.
- **`PEERCACHE_RDMA_OP_TIMEOUT_MS`** to tune the data-plane completion timeout.

### Changed
- **Vectorised read hot path**: `store._fetch` now builds parallel primitive
  arrays and calls a new GIL-released `TransferEngine::batch_read_v` (and
  `Transport.batch_read_v`) instead of constructing one Python/pybind object per
  op, shrinking the GIL-held portion of `batch_get_v1` so concurrent readers
  keep more RDMA in flight and can saturate the NIC. `batch_read` is preserved
  (adapts to the vectorised path).
- **RDMA tuning**: RC QPs now use the port's negotiated **active MTU** (e.g.
  4096 on RoCE) instead of a hardcoded 1 KiB, and `drain()` reaps completions in
  batches of 16 to cut per-CQE polling overhead.

### Fixed
- **Benchmark stall on large pages**: `HostKVPool.fill_slot` initialised each
  page with a per-byte Python loop (hundreds of millions of assignments for
  128 KiB pages across thousands of slots), stalling the suite for minutes and
  appearing to hang. Replaced with a single `memmove` from a precomputed
  template (identical bytes); 2048 × 128 KiB slots now fill in ~26 ms.
- **Indefinite hang on a stalled RDMA read**: `RdmaEndpoint::drain` busy-polled
  the completion queue with no deadline, so a READ that never completes (e.g. a
  RoCE GID/loopback misconfiguration) wedged the worker — and the whole process
  — forever. `drain()` now has a timeout (default 5s) and `batch_read` discards
  a timed-out channel; the TCP QP-bootstrap sockets gain send/recv timeouts.
  A broken fabric now fails fast and visibly instead of looking like a deadlock.
- Warn once when host buffers cannot be page-locked (no torch / pin failed),
  since pageable memory materially lowers RDMA throughput.

## [0.3.0] - 2026-05-31

### Added
- **Concurrent multi-threaded reads/writes** on both client and server. The RDMA
  data plane now uses a **per-peer channel pool**, where each channel is an RC QP
  with its own private completion queue, so concurrent reader threads post/poll on
  independent CQs with no shared-CQ contention (capped by the new
  `max_channels_per_peer`, default 16). The TCP fallback gains a matching
  per-endpoint socket pool, and the control-plane RPC pool now leases a connection
  per in-flight call so directory lookups/promotes run in parallel.
- **Benchmark suite** (`peercache.bench`): a systematic, RDMA-first benchmark
  that drives PeerCache's `HiCacheStorage` interface exactly as SGLang HiCache
  does (PD-disaggregated `batch_set_v1` / `batch_exists` / `batch_get_v1`) via a
  faithful `mem_pool_host` simulator (MLA/MHA). Reports throughput (pages/s,
  tokens/s, GB/s) and latency tail (p50/p95/p99/p999/max) across a thread-model
  sweep, including full-load saturation/peak throughput, with `latency`,
  `throughput`, `saturation`, and `suite` modes. Memory-bounded HDR-style latency
  histogram. Shipped inside the package and exposed as a single console command
  `peercache-bench` with subcommands (`latency`, `throughput`, `saturation`,
  `suite`, `micro`, `mooncake`, `compare`) -- run after `pip install` with no
  repo clone or PYTHONPATH. Includes an optional Mooncake `transfer_engine_bench` comparison and
  a low-level data-plane microbench. New `Benchmarks` docs page (EN/中文) and a
  `bench` extra. The TCP fallback is for functional smoke testing only.

### Changed
- Shared client state (`key → length` map) is now lock-guarded; broken pooled
  connections are closed instead of being reused.
- **Default ports** now use the `31997-31999` band: metrics/dashboard stays on
  `31997` and the discovery/meta service default moves from `9100` to `31998`
  (`peercache-meta`, the `DiscoveryServer` default, and the `discovery_addr`
  examples). `rdma_port`/`control_port` remain auto-assigned (`0`) so co-located
  ranks do not collide; `31999` is reserved.

## [0.2.0] - 2026-05-31

### Added
- **Disk persistence tier (L4)**: published pages are written through to disk
  (`disk_path`, default `/data/peercache/`, capped by `disk_size`, default
  `100GB`, LRU-bounded). Pool eviction marks the directory entry non-resident
  instead of deleting it; a later read promotes the page from disk back into the
  pool — locally, or on the owner via a `data_promote` RPC for remote readers.
  `exists` hits also kick a best-effort async prefetch. Degrades gracefully if
  the directory cannot be created.
- **Metrics + monitoring**: a Prometheus `/metrics` endpoint and an embedded,
  dependency-free HTML dashboard (default port `31997`) exposing hit rate,
  read/write throughput and byte counters, eviction/promote counters, pool/disk
  usage, member count, and read/write latency p50/p90/p99 + average.

### Changed
- `DataLocation` gains a `resident` flag (disk-resident pages are kept in the
  directory as non-resident until promoted).

## [0.1.1] - 2026-05-31

### Changed
- **Embedded meta**: removed the requirement for a separate meta process. The
  node whose IP equals `discovery_addr` now auto-hosts the discovery service
  in-process; co-located nodes that cannot bind the port fall back to client
  mode automatically. Adds `PeerCacheConfig.meta_bind_host` and
  `NodeRuntime.is_meta`.

### Added
- Bilingual (English / 中文) documentation with a language switcher
  (`mkdocs-static-i18n`).

## [0.1.0] - 2026-05-31

Initial release.

### Added
- **Decentralized architecture**: a single meta node for service discovery only,
  plus a consistent-hash distributed directory (DHT) sharded across nodes -- no
  centralized master or metadata service.
- **C++ RDMA data plane** (`cpp/`): raw `libibverbs` + TCP QP bootstrap, RC QPs,
  one-sided `IBV_WR_RDMA_READ`, shared CQ polling, lazy per-peer connection
  pooling, exposed to Python via `pybind11` (`_peercache`).
- **Two-MR model**: receive MR (`mem_pool_host.kv_buffer`) as READ destination +
  backend-owned published pool (LRU, eviction deletes the directory entry).
- **`PeerCacheStore`**: a SGLang `HiCacheStorage` backend with the v1 zero-copy
  paths (`batch_set_v1` / `batch_get_v1` / `batch_exists`), the v2 hybrid-pool
  paths, and the single-key/batch APIs. Mooncake-compatible key suffixing
  (MHA `_k`/`_v`, MLA single key).
- **Zero-touch SGLang integration** via the `dynamic` backend mechanism.
- **TCP fallback transport** for functional testing without RDMA hardware.
- Service discovery, consistent-hash ring, directory client/server, and a
  lightweight TCP RPC.
- MkDocs SDK documentation site and GitHub Actions for CI, docs, and release.

[0.5.0]: https://github.com/flymysql/PeerCache/releases/tag/v0.5.0
[0.4.0]: https://github.com/flymysql/PeerCache/releases/tag/v0.4.0
[0.3.0]: https://github.com/flymysql/PeerCache/releases/tag/v0.3.0
[0.2.0]: https://github.com/flymysql/PeerCache/releases/tag/v0.2.0
[0.1.1]: https://github.com/flymysql/PeerCache/releases/tag/v0.1.1
[0.1.0]: https://github.com/flymysql/PeerCache/releases/tag/v0.1.0
