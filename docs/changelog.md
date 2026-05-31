# Changelog

This project adheres to [Semantic Versioning](https://semver.org/).

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

[0.1.0]: https://github.com/flymysql/PeerCache/releases/tag/v0.1.0
