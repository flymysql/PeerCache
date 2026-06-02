# Changelog

All notable changes to PeerCache are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/) and this project adheres to
[Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.6.3] - 2026-06-02

### Changed
- **Discovery registration now polls the meta indefinitely instead of failing on
  a timeout.** A node started before the meta no longer crashes the host process
  (`TimeoutError: timed out` out of `register()`); it waits, logging periodically
  (`waiting for meta … attempt N … retrying`), and proceeds once the meta is up.
- **Much more discovery logging** for operability: node identity + which node is
  the meta at startup; the meta logs every register / re-register / deregister /
  dead-node prune with the current member count and list; each client logs
  successful registration, every heartbeat (`known=`, member count), and any
  membership change (joined / left).

## [0.6.2] - 2026-06-02

### Fixed
- **Generic value-based `set`/`batch_set`/`get`/`batch_get` now work** — SGLang's
  HiCache page-backup path calls `batch_set(hash_values, data)` (a list of host
  KV page tensors) and reads back via `batch_get(keys, dst_tensors)`. PeerCache
  previously only implemented the zero-copy (`target_location`/`target_sizes`)
  form and `assert`ed, crashing the controller's backup thread with an
  `AssertionError`. These methods now accept tensor-like objects (`data_ptr()` /
  `numel()` / `element_size()`), bytes, numpy arrays, or raw int ptrs, for both
  the value form and the fill-target form; `batch_get` returns a list aligned
  with `keys` (the destination on a hit, else `None`).
- **PeerCache also did nothing under SGLang versions that register the KV pool
  via `register_mem_host_pool_v2` (the v2 path)** — that handler never created
  the published pool / recv MR / set `mem_pool_host`. Registration is now shared
  between the v1 and v2 paths via `_ensure_published_pool()` / `_register_recv()`.

### Added
- **"Multi-node Demo" docs page** (EN/中文): a step-by-step walkthrough that
  brings up 4 aggregated (non-PD) SGLang nodes sharing one prefix/KV cache via
  PeerCache and proves cross-node hits with the metrics (`read_remote_hits`),
  including the `--hicache-write-policy write_through` / `--hicache-ratio` knobs,
  a shared-prefix workload, round-robin routing, an A/B benefit check, and a
  troubleshooting table.

### Added
- **"Positioning & comparison" docs page** (EN/中文): what PeerCache is (a
  decentralized P2P prefix/KV-reuse cache) and is not (not a PD transfer engine),
  the two orthogonal axes (reuse vs P→D handoff), a comparison vs centralized KV
  stores (Mooncake Store / LMCache), the advantages, the trade-offs deliberately
  accepted, and a when-to-use decision guide.

## [0.6.1] - 2026-06-01

### Fixed
- **SGLang dynamic-backend registration on newer SGLang** ("Backend class
  PeerCacheStore must inherit from HiCacheStorage"). `peercache.store` imported
  `HiCacheStorage` together with optional names (`HiCacheStorageConfig`,
  `HiCacheStorageExtraInfo`, `PoolName`) in one statement; if any optional name
  was absent in the installed SGLang, the whole import fell back to a stand-in
  base class and `PeerCacheStore` was no longer a subclass of SGLang's real
  `HiCacheStorage`, so SGLang rejected it. The base class is now imported on its
  own and the optional names degrade independently.

### Changed
- Refreshed the **full-machine performance baseline** (docs + README + overview
  + charts): 8-NIC multi-process aggregate **273 → 413 GB/s (≈ 3.3 Tbps)** with
  8 reader processes/NIC at 128 KiB pages; added the measured GPUDirect result
  (49.5 GB/s, single-GPU PCIe-bound) and noted the per-NIC range (25–89 GB/s).

## [0.6.0] - 2026-06-01

### Added
- **GPUDirect RDMA**: the receive buffer (read destination) may live in GPU
  memory so pages land straight in VRAM with no host bounce. Buffers that expose
  a dmabuf fd register via `ibv_reg_dmabuf_mr` (`TransferEngine.register_mr_dmabuf`);
  otherwise a plain MR of the device VA is used (works with `nvidia-peermem`).
  Registration failures raise a clear error pointing at the GPUDirect
  prerequisites. New `peercache-bench drive --gpu` allocates the recv MR in GPU
  memory to measure the GPUDirect path.
- **Config validation**: `PeerCacheConfig` now fails fast with actionable errors
  on a bad `protocol`/`discovery_addr`/`ib_port`/`gid_index`/`global_segment_size`
  and on duplicate `device_names` (rails must be distinct NICs).
- **Data-plane observability**: new metrics `read_failures` (entry found but the
  RDMA READ failed) and transport gauges `rdma_rails` / `rdma_read_timeouts` /
  `rdma_channel_discards` (surfaced from the C++ engine's `stats()`).
- **Directory wire-format version** (`DataLocation` carries a schema `v`), kept
  forward/backward compatible (`from_dict` ignores unknown keys, missing newer
  fields fall back to single-rail/legacy values).

- **Performance baseline docs page** (EN/中文) with charts: single-NIC PeerCache
  vs bare `ib_read_bw`, single-process multi-rail scaling, and the full-machine
  8-NIC multi-process aggregate. Figures are regenerated by
  `docs/assets/perf/make_charts.py`.

### Changed
- Node shutdown is now idempotent and deregisters from discovery before tearing
  down the RPC/RDMA endpoints, so peers drop a cleanly-stopped node faster.
- **Directory survives membership changes.** When the ring changes (a node joins
  or leaves) the consistent-hash owner of a key can move, and entries are not
  migrated automatically. Each producer now **re-publishes the locations of the
  pages it owns** onto the new owners on every membership change (off the
  discovery thread), so a node that joins after a publish still finds every key.
  Directory replication now defaults to **2** (`directory_replicas`) so a single
  node loss doesn't drop entries before the re-shard completes. New
  `directory_republishes` metric.

## [0.5.1] - 2026-05-31

### Fixed
- **`peercache-bench serve` now re-publishes when ring membership changes**, so
  back-to-back `drive` runs against one long-lived `serve` work without a
  restart. The directory is consistent-hash sharded across the live ring, and a
  `drive` reader hosts part of it; when one run's reader exits and the next
  starts (a fresh process, reusing the same node_id but with an empty directory
  shard), the previously published entries for keys owned by that shard were
  lost and the new run timed out in "waiting for the producer's working set".
  `serve` now watches membership (keyed on each member's control endpoint, so a
  re-run is detected even though the node_id is reused) and re-shards the
  directory for the current members. Departed readers are also pruned faster
  (`member_ttl` 30s -> 5s in the bench).

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

[0.6.3]: https://github.com/flymysql/PeerCache/releases/tag/v0.6.3
[0.6.2]: https://github.com/flymysql/PeerCache/releases/tag/v0.6.2
[0.6.1]: https://github.com/flymysql/PeerCache/releases/tag/v0.6.1
[0.6.0]: https://github.com/flymysql/PeerCache/releases/tag/v0.6.0
[0.5.1]: https://github.com/flymysql/PeerCache/releases/tag/v0.5.1
[0.5.0]: https://github.com/flymysql/PeerCache/releases/tag/v0.5.0
[0.4.0]: https://github.com/flymysql/PeerCache/releases/tag/v0.4.0
[0.3.0]: https://github.com/flymysql/PeerCache/releases/tag/v0.3.0
[0.2.0]: https://github.com/flymysql/PeerCache/releases/tag/v0.2.0
[0.1.1]: https://github.com/flymysql/PeerCache/releases/tag/v0.1.1
[0.1.0]: https://github.com/flymysql/PeerCache/releases/tag/v0.1.0
