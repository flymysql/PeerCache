# Changelog

This project adheres to [Semantic Versioning](https://semver.org/).

## [0.7.1] - 2026-06-02

### Changed
- Multi-master discovery now **pins the configured head** (the `discovery_addr`
  host) as the primary master whenever it is alive — a stable, well-known
  bootstrap anchor — and fills the remaining master slots in hostname order as
  nodes join. If the head is down, the live hosts still fill all slots.

## [0.7.0] - 2026-06-02

### Added
- **Multi-master discovery — no single meta SPOF.** Every host runs a discovery
  server on the cluster-wide meta port; the active masters are the `max_masters`
  (default 3) lowest-hostname live hosts, derived from membership. A dead master
  is replaced automatically, and a cluster with fewer than `max_masters` hosts has
  all of them as masters. Clients register/heartbeat to all current masters plus
  the configured bootstrap seeds (`discovery_addr` may be a comma-separated list)
  and merge the membership; the soft-state registry repopulates within one
  heartbeat. New `max_masters` config and `DiscoveryClient.master_hosts()`.
  Backward compatible with a single `discovery_addr`.

## [0.6.9] - 2026-06-02

### Fixed
- **Cross-node reads from SGLang's generic `batch_get` now transfer.** The local
  READ destination SGLang passes can sit outside the registered host KV pool, so
  `lkey_for(addr)` returned 0 and the work request was silently never posted
  (`read_failures` climbed with no completion error and no timeout). `RdmaContext`
  now lazily registers and caches an MR (`LOCAL_WRITE`) for an unregistered
  destination range; SGLang reuses a bounded set of host pages, so the cache
  converges after first touch. New `rdma_lazy_local_mrs` gauge.

## [0.6.8] - 2026-06-02

### Added
- **Pre-wire read-failure counters** to tell "failed on the wire" from "never
  posted": `rdma_local_reg_misses`, `rdma_post_failures`, `rdma_lease_failures`.

## [0.6.7] - 2026-06-02

### Added
- **RDMA READ completion-error visibility.** `drain()` records the failing
  `ibv_wc_status` and logs `ibv_wc_status_str` (rate-limited); new
  `rdma_read_wc_errors` / `rdma_last_wc_status` gauges distinguish a remote-access
  error (bad rkey/MR, status 10) from retry-exceeded (GID/MTU/path, 12/13).

## [0.6.6] - 2026-06-02

### Changed
- Heartbeat logging throttled to ~10s (membership/known-state changes still log
  immediately); the heartbeat cadence itself is unchanged.

## [0.6.5] - 2026-06-02

### Fixed
- **`batch_exists` probed the wrong keyspace, so reads never fired.** SGLang's
  generic path writes one blob per *raw* key via `batch_set`, but `batch_exists`
  looked keys up as the *suffixed* K/V component keys used by the zero-copy v1/v2
  path, so the prefetch probe missed every page (`exists_pages_found` stayed 0
  while writes climbed) and SGLang never issued a `get`. `batch_exists` / `exists`
  now resolve keys through the active keyspace and self-heal on read-only nodes.

## [0.6.4] - 2026-06-02

### Added
- **`exists` / L3-prefetch observability**: `exists_requests` and
  `exists_pages_found` make the SGLang prefetch path visible end to end.

### Changed
- **Directory lookup reused across `exists` → `get`.** `batch_exists` primes the
  resident hit locations into a one-shot, short-TTL handoff cache that the imminent
  `batch_get` consumes, skipping the redundant second directory RPC. New
  `directory_lookups_saved` counter.

## [0.6.3] - 2026-06-02

### Changed
- **Discovery registration polls the meta indefinitely instead of failing on a
  timeout** — a node started before the meta no longer crashes the host process;
  it waits, logging periodically, and proceeds once the meta is up.
- **Much more discovery logging** for operability (node identity, master at
  startup, register/heartbeat/prune/membership-change events).

## [0.6.2] - 2026-06-02

### Fixed
- **Generic value-based `set`/`batch_set`/`get`/`batch_get` now work** — SGLang's
  HiCache page-backup calls `batch_set(hash_values, data)` and reads back via
  `batch_get(keys, dst_tensors)`; PeerCache previously only implemented the
  zero-copy form and crashed with an `AssertionError`. These now accept
  tensor-like objects, bytes, numpy arrays, or raw int ptrs.
- **The v2 registration path (`register_mem_host_pool_v2`) never created the
  published pool** (so `pool_capacity_bytes` stayed 0). v1 and v2 now share
  `_ensure_published_pool()` / `_register_recv()`.

### Added
- **"Multi-node Demo"** and **"Positioning & comparison"** docs pages (EN/中文).

## [0.6.1] - 2026-06-01

### Fixed
- **SGLang dynamic-backend registration on newer SGLang** ("Backend class
  PeerCacheStore must inherit from HiCacheStorage"): `HiCacheStorage` is now
  imported on its own and the optional names degrade independently, so
  `PeerCacheStore` always subclasses SGLang's real base.

### Changed
- Refreshed the **full-machine performance baseline**: 8-NIC multi-process
  aggregate **273 → 413 GB/s (≈ 3.3 Tbps)**; added the GPUDirect result
  (49.5 GB/s) and the per-NIC range (25–89 GB/s).

## [0.6.0] - 2026-06-01

### Added
- **GPUDirect RDMA**: the receive buffer may live in GPU memory (dmabuf via
  `ibv_reg_dmabuf_mr`, else a plain MR of the device VA with `nvidia-peermem`);
  `peercache-bench drive --gpu` measures it.
- **Config validation** with actionable errors; **data-plane gauges**
  (`read_failures`, `rdma_rails`, `rdma_read_timeouts`, `rdma_channel_discards`);
  **directory wire-format version** on `DataLocation`.
- **Performance baseline docs page** (EN/中文) with charts.

### Changed
- Idempotent shutdown that deregisters first. **Directory survives membership
  changes** (each producer re-publishes its pages on a ring change); directory
  replication now defaults to **2** (`directory_replicas`). New
  `directory_republishes` metric.

## [0.5.1] - 2026-05-31

### Fixed
- **`peercache-bench serve` re-publishes on ring-membership changes**, so
  back-to-back `drive` runs against one long-lived `serve` work without a restart.

## [0.5.0] - 2026-05-31

### Added
- **Single-process multi-rail (multi-NIC) reads.** One `PeerCacheStore` process
  opens one rail per device (`device_names="mlx5_0,…"`) and stripes each batch of
  one-sided READs across all rails in one GIL-released call
  (`TransferEngine::batch_read_multi`), approaching the aggregate bandwidth of all
  NICs. `DataLocation` carries per-rail `rail_endpoints[]` / `rail_rkeys[]` (rail 0
  stays wire-compatible). `--devices` added to `peercache-bench serve` / `drive`.

### Changed
- `TransferEngine` is multi-rail internally; `register_mr` returns one handle per
  rail; new `local_endpoints()` / `n_rails()`.

## [0.4.0] - 2026-05-31

### Added
- **Two-host (distributed) benchmark** (`peercache-bench serve` / `drive`) for a
  genuine cross-host one-sided RDMA READ; `drive --processes N` to escape the GIL.
- **Benchmark logging** (`--log-level` / `--log-file`); **`directory_read_cache_ttl`**
  (default off); **`max_channels_per_peer`** config; **`PEERCACHE_RDMA_OP_TIMEOUT_MS`**.

### Changed
- **Vectorised read hot path** (`TransferEngine::batch_read_v`, GIL-released).
- **RDMA tuning**: RC QPs use the port's negotiated active MTU; `drain()` reaps
  completions in batches of 16.

### Fixed
- **Benchmark stall on large pages**: `HostKVPool.fill_slot` replaced a per-byte
  Python loop with a single `memmove` from a template.
- **Indefinite hang on a stalled RDMA read**: `drain()` gains a timeout (default
  5s) and discards timed-out channels; TCP QP-bootstrap sockets gain timeouts.

## [0.3.0] - 2026-05-31

### Added
- **Concurrent multi-threaded reads/writes**: a per-peer channel pool, each
  channel an RC QP with its own CQ (capped by `max_channels_per_peer`, default 16);
  matching TCP socket pool and per-call control-plane RPC pool.
- **Benchmark suite** (`peercache-bench`): drives the `HiCacheStorage` interface
  exactly as SGLang HiCache does, reporting throughput and latency tails across a
  thread-model sweep. New `Benchmarks` docs page.

### Changed
- Shared client state is lock-guarded; broken pooled connections are closed.
- **Default ports** moved to the `31997-31999` band (metrics `31997`, discovery
  `31998`); `rdma_port`/`control_port` stay auto-assigned.

## [0.2.0] - 2026-05-31

### Added
- **Disk persistence tier (L4)**: published pages spill to disk (`disk_path`,
  default `/data/peercache/`, capped by `disk_size`, default `100GB`). Evicted
  pages stay in the directory as non-resident and are promoted back on a later
  read (locally, or on the owner via a `data_promote` RPC); `exists` hits kick a
  best-effort prefetch.
- **Metrics + monitoring**: Prometheus `/metrics` endpoint and an embedded HTML
  dashboard (default port `31997`).

### Changed
- `DataLocation` gains a `resident` flag for disk-resident pages.

## [0.1.1] - 2026-05-31

### Changed
- **Embedded meta**: removed the requirement for a separate meta process (the
  node whose IP equals `discovery_addr` auto-hosted it in-process; superseded by
  multi-master discovery in 0.7.0).

### Added
- Bilingual (English / 中文) documentation with a language switcher.

## [0.1.0] - 2026-05-31

Initial release.

### Added
- Decentralized architecture: service discovery plus a consistent-hash
  distributed directory (DHT) sharded across nodes — no centralized master or
  metadata service.
- C++ RDMA data plane (`libibverbs`, RC QPs, one-sided `IBV_WR_RDMA_READ`) via
  `pybind11`; two-MR model; `PeerCacheStore` (`HiCacheStorage`) with v1/v2 and
  single-key/batch APIs; zero-touch SGLang `dynamic` backend integration; TCP
  fallback transport; MkDocs site and CI/docs/release workflows.

[0.7.1]: https://github.com/flymysql/PeerCache/releases/tag/v0.7.1
[0.7.0]: https://github.com/flymysql/PeerCache/releases/tag/v0.7.0
[0.6.9]: https://github.com/flymysql/PeerCache/releases/tag/v0.6.9
[0.6.8]: https://github.com/flymysql/PeerCache/releases/tag/v0.6.8
[0.6.7]: https://github.com/flymysql/PeerCache/releases/tag/v0.6.7
[0.6.6]: https://github.com/flymysql/PeerCache/releases/tag/v0.6.6
[0.6.5]: https://github.com/flymysql/PeerCache/releases/tag/v0.6.5
[0.6.4]: https://github.com/flymysql/PeerCache/releases/tag/v0.6.4
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
