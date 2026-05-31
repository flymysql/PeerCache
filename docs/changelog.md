# Changelog

This project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- **Benchmark harness** (`benchmarks/`): a reproducible PeerCache-vs-Mooncake
  comparison driving PeerCache's data plane, its full store path, and Mooncake's
  official `transfer_engine_bench` under one workload. Runs over RDMA or the TCP
  fallback; emits JSON + Markdown reports. See the [Benchmarks](benchmarks.md)
  page.

## [0.2.0] - 2026-05-31

### Added
- **Disk persistence tier (L4)**: published pages spill to disk (`disk_path`,
  default `/data/peercache/`, capped by `disk_size`, default `100GB`). Evicted
  pages stay in the directory as non-resident and are promoted back into the pool
  on a later read (locally, or on the owner via a `data_promote` RPC for remote
  readers); `exists` hits trigger a best-effort prefetch.
- **Metrics + monitoring**: Prometheus `/metrics` endpoint and an embedded HTML
  dashboard (default port `31997`) for hit rate, throughput, latency p50/p99,
  and memory/disk usage.

### Changed
- `DataLocation` gains a `resident` flag for disk-resident pages.

## [0.1.1] - 2026-05-31

### Changed
- **Embedded meta**: removed the requirement for a separate meta process. The
  node whose IP equals `discovery_addr` now auto-hosts the discovery service
  in-process; co-located nodes that cannot bind the port fall back to client
  mode automatically.

### Added
- Bilingual (English / 中文) documentation with a language switcher
  (`mkdocs-static-i18n`).

## [0.1.0] - 2026-05-31

Initial release.

### Added
- Decentralized architecture: a single meta node for service discovery only, plus
  a consistent-hash distributed directory (DHT) sharded across nodes — no
  centralized master or metadata service.
- C++ RDMA data plane: raw `libibverbs` + TCP QP bootstrap, RC QPs, one-sided
  `IBV_WR_RDMA_READ`, shared CQ polling, lazy per-peer connection pooling, exposed
  to Python via `pybind11` (`_peercache`).
- Two-MR model: receive MR (`mem_pool_host.kv_buffer`) + backend-owned published
  pool (LRU, eviction deletes the directory entry).
- `PeerCacheStore`: a SGLang `HiCacheStorage` backend with v1 zero-copy paths, v2
  hybrid-pool paths, and the single-key/batch APIs. Mooncake-compatible key
  suffixing (MHA `_k`/`_v`, MLA single key).
- Zero-touch SGLang integration via the `dynamic` backend mechanism.
- TCP fallback transport for functional testing without RDMA hardware.
- MkDocs SDK documentation site and GitHub Actions for CI, docs, and release.

[0.2.0]: https://github.com/flymysql/PeerCache/releases/tag/v0.2.0
[0.1.1]: https://github.com/flymysql/PeerCache/releases/tag/v0.1.1
[0.1.0]: https://github.com/flymysql/PeerCache/releases/tag/v0.1.0
