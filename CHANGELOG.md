# Changelog

All notable changes to PeerCache are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/) and this project adheres to
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- **Benchmark harness** (`benchmarks/`): a systematic, RDMA-first benchmark
  that drives PeerCache's `HiCacheStorage` interface exactly as SGLang HiCache
  does (PD-disaggregated `batch_set_v1` / `batch_exists` / `batch_get_v1`) via a
  faithful `mem_pool_host` simulator (MLA/MHA). Reports throughput (pages/s,
  tokens/s, GB/s) and latency tail (p50/p95/p99/p999/max) across a thread-model
  sweep, including full-load saturation/peak throughput, with `latency`,
  `throughput`, `saturation`, and `suite` modes. Memory-bounded HDR-style latency
  histogram. Shipped inside the package and exposed as console commands
  (`peercache-bench`, `peercache-bench-micro`, `peercache-bench-mooncake`,
  `peercache-bench-compare`) -- run after `pip install` with no repo clone or
  PYTHONPATH. Includes an optional Mooncake `transfer_engine_bench` comparison and
  a low-level data-plane microbench. New `Benchmarks` docs page (EN/中文) and a
  `bench` extra. The TCP fallback is for functional smoke testing only.

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

[0.2.0]: https://github.com/flymysql/PeerCache/releases/tag/v0.2.0
[0.1.1]: https://github.com/flymysql/PeerCache/releases/tag/v0.1.1
[0.1.0]: https://github.com/flymysql/PeerCache/releases/tag/v0.1.0
