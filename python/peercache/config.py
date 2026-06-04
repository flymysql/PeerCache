"""Configuration for the PeerCache backend."""

from __future__ import annotations

import socket
import uuid
from dataclasses import dataclass
from typing import Optional


def _parse_size(value) -> int:
    """Parse sizes like '4gb', '512mb', 1048576 into bytes."""
    if isinstance(value, int):
        return value
    s = str(value).strip().lower()
    units = {"kb": 1 << 10, "mb": 1 << 20, "gb": 1 << 30, "tb": 1 << 40}
    for suffix, mult in units.items():
        if s.endswith(suffix):
            return int(float(s[: -len(suffix)].strip()) * mult)
    return int(s)


def _as_bool(value) -> bool:
    """Parse JSON/string/int booleans (extra_config values may be strings)."""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in ("1", "true", "yes", "on")


@dataclass
class PeerCacheConfig:
    # Service discovery (meta node) "host:port".
    discovery_addr: str

    # Deployment mode (per inference node; storage servers use role=storage):
    #   p2p          - default: KV stays on the producing node (decentralized)
    #   hybrid       - P2P and storage servers coexist; use write_policy to choose
    #                    where hybrid inference nodes publish (default: local only)
    #   centralized  - inference nodes are clients; KV only on storage servers
    mode: str = "p2p"
    # Hybrid write policy (ignored in p2p / centralized):
    #   local   - default: publish locally only (P2P path), even if storage servers exist
    #   storage - RDMA WRITE to storage servers only (directory points at storage)
    #   both    - dual write: storage (for cross-node sharing) + local pool copy
    write_policy: str = "local"
    # Node role (centralized mode only):
    #   auto       - infer from the process (PeerCacheStore -> inference,
    #                peercache-storage-server -> storage)
    #   inference  - SGLang worker / cache client
    #   storage    - dedicated KV cache server
    role: str = "auto"

    # RDMA / transport.
    protocol: str = "rdma"  # "rdma" or "tcp" (fallback)
    device_name: str = ""  # e.g. "mlx5_0"; empty -> first active device
    # Multi-rail (multi-NIC): comma-separated device list, e.g.
    # "mlx5_bond_1,mlx5_bond_2,...". When set (>1 device) a single process opens
    # one RDMA rail per device and stripes READs across all of them, so it can
    # drive several NICs without the GIL capping it. Overrides device_name.
    device_names: str = ""
    ib_port: int = 1
    gid_index: int = 3
    # Number of RC QP "channels" pooled per peer. Each channel has its own CQ,
    # so more channels let more reader threads post/poll a given peer fully in
    # parallel (raise for high-concurrency reads against one data node).
    max_channels_per_peer: int = 16

    # Backend-owned published pool size (host memory registered as MR).
    global_segment_size: int = 4 << 30

    # Consistent-hash directory.
    vnodes: int = 160  # virtual nodes per physical node
    directory_replicas: int = 2  # replicate directory entries for HA on node loss
    # Cache resolved *resident* read locations for this many seconds to skip the
    # per-batch directory lookup (a cross-node RPC) on hot, static working sets.
    # 0 disables the cache. Entries are invalidated immediately on a read miss
    # and are TTL-bounded, so a stale (evicted) location self-heals.
    directory_read_cache_ttl: float = 0.0

    # Default fixed ports use the 31997-31999 band:
    #   31997 -> metrics/dashboard HTTP (metrics_port)
    #   31998 -> discovery/meta service (the port in discovery_addr; see docs)
    #   31999 -> reserved
    # rdma_port/control_port stay 0 (auto-assign) so co-located ranks on one host
    # do not collide.
    local_hostname: str = ""
    rdma_bind_host: str = "0.0.0.0"
    rdma_port: int = 0
    control_bind_host: str = "0.0.0.0"
    control_port: int = 0

    # Embedded meta: the node whose IP equals discovery_addr auto-hosts the
    # discovery service, bound on this interface.
    meta_bind_host: str = "0.0.0.0"

    # Disk persistence tier (L4): published pages also spill to disk so they can
    # be promoted back into the pool (and read remotely) after LRU eviction.
    disk_enabled: bool = True
    disk_path: str = "/data/peercache/"
    disk_size: int = 100 << 30  # accepts int or "100gb"/"512mb"

    # Metrics / monitoring. Exposes Prometheus /metrics and an embedded dashboard.
    metrics_enabled: bool = True
    metrics_bind_host: str = "0.0.0.0"
    metrics_port: int = 31997
    metrics_dashboard: bool = True  # serve the built-in HTML dashboard at "/"

    # Node identity; auto-generated if not provided.
    node_id: str = ""

    # Heartbeat / membership refresh interval (seconds).
    heartbeat_interval: float = 2.0
    member_ttl: float = 6.0

    # Multi-master discovery: every host runs a discovery server on the meta
    # port; the active masters are the `max_masters` lowest-hostname live hosts.
    # A dead master is replaced automatically; a cluster with fewer hosts than
    # this has all of them as masters. `discovery_addr` may also be a
    # comma-separated list of bootstrap seeds.
    max_masters: int = 3

    def __post_init__(self):
        if not self.local_hostname:
            self.local_hostname = _resolve_local_ip(self.discovery_addr)
        if not self.node_id:
            self.node_id = f"{self.local_hostname}-{uuid.uuid4().hex[:8]}"
        self.global_segment_size = _parse_size(self.global_segment_size)
        # Normalise the rail device list: prefer device_names, else device_name,
        # else [""] (single rail, auto-pick first device).
        if self.device_names:
            self._rails = [d.strip() for d in str(self.device_names).split(",") if d.strip()]
        elif self.device_name:
            self._rails = [self.device_name]
        else:
            self._rails = [""]
        self.disk_size = _parse_size(self.disk_size)
        self.disk_enabled = _as_bool(self.disk_enabled)
        self.metrics_enabled = _as_bool(self.metrics_enabled)
        self.metrics_dashboard = _as_bool(self.metrics_dashboard)
        self._validate()

    def is_centralized(self) -> bool:
        return str(self.mode).strip().lower() == "centralized"

    def is_hybrid(self) -> bool:
        return str(self.mode).strip().lower() == "hybrid"

    def uses_storage_writes(self) -> bool:
        """Whether this node should push KV pages to storage servers when available."""
        if self.is_centralized():
            return True
        if self.is_hybrid():
            return self.write_policy in ("storage", "both")
        return False

    def writes_local_pool(self) -> bool:
        """Whether this node publishes into its local published pool."""
        if self.is_inference_client_only():
            return False
        if self.is_hybrid():
            return self.write_policy in ("local", "both")
        return True

    def is_inference_client_only(self) -> bool:
        """True when this inference node has no local published pool."""
        return self.is_centralized() and self.effective_role() == "inference"

    def effective_role(self, *, for_storage_server: bool = False) -> str:
        """Resolve ``role=auto`` to ``inference`` or ``storage``."""
        r = str(self.role).strip().lower()
        if r != "auto":
            return r
        return "storage" if for_storage_server else "inference"

    def _validate(self) -> None:
        """Fail fast on misconfiguration with an actionable message."""
        mode = str(self.mode).strip().lower()
        if mode not in ("p2p", "hybrid", "centralized"):
            raise ValueError(
                f"peercache: mode must be 'p2p', 'hybrid', or 'centralized', "
                f"got {self.mode!r}"
            )
        self.mode = mode
        role = str(self.role).strip().lower()
        if role not in ("auto", "inference", "storage"):
            raise ValueError(
                f"peercache: role must be 'auto', 'inference', or 'storage', "
                f"got {self.role!r}"
            )
        self.role = role
        wp = str(getattr(self, "write_policy", "local")).strip().lower()
        if wp not in ("local", "storage", "both"):
            raise ValueError(
                f"peercache: write_policy must be 'local', 'storage', or 'both', "
                f"got {self.write_policy!r}"
            )
        self.write_policy = wp
        if role == "storage" and mode == "p2p":
            pass  # storage servers may coexist with P2P inference nodes (hybrid cluster)
        if self.protocol not in ("rdma", "tcp"):
            raise ValueError(
                f"peercache: protocol must be 'rdma' or 'tcp', got {self.protocol!r}"
            )
        seeds = self.discovery_seeds()
        if not seeds or any(":" not in s for s in seeds):
            raise ValueError(
                "peercache: discovery_addr must be 'host:port' (or a "
                f"comma-separated list of them), got {self.discovery_addr!r}"
            )
        ports = {s.rsplit(":", 1)[1] for s in seeds}
        if len(ports) > 1:
            raise ValueError(
                "peercache: all discovery_addr seeds must share the same meta "
                f"port (every host listens on it), got ports {sorted(ports)}"
            )
        if int(self.max_masters) < 1:
            raise ValueError(f"peercache: max_masters must be >= 1, got {self.max_masters}")
        if not (1 <= int(self.ib_port) <= 255):
            raise ValueError(f"peercache: ib_port must be 1..255, got {self.ib_port}")
        if int(self.gid_index) < -1:
            raise ValueError(
                f"peercache: gid_index must be >= -1 (-1 disables GRH), got "
                f"{self.gid_index}"
            )
        if int(self.global_segment_size) <= 0:
            raise ValueError(
                "peercache: global_segment_size must be > 0, got "
                f"{self.global_segment_size}"
            )
        if int(getattr(self, "max_channels_per_peer", 1)) < 1:
            raise ValueError("peercache: max_channels_per_peer must be >= 1")
        # Multi-rail: duplicate devices would make rails pair to the same NIC and
        # waste channels; flag it (named devices only -- "" means auto-pick).
        named = [d for d in self._rails if d]
        dupes = {d for d in named if named.count(d) > 1}
        if dupes:
            raise ValueError(
                "peercache: device_names has duplicate device(s) "
                f"{sorted(dupes)}; each rail must be a distinct NIC. Rails pair "
                "by index across nodes, so list the same distinct devices in the "
                "same order on every node."
            )

    def device_rails(self) -> list:
        """Ordered list of RDMA device names, one per rail (>=1)."""
        return list(getattr(self, "_rails", None) or [self.device_name or ""])

    def discovery_seeds(self) -> list:
        """Bootstrap seed endpoints parsed from discovery_addr (host:port list)."""
        return [s.strip() for s in str(self.discovery_addr).split(",") if s.strip()]

    def meta_port(self) -> int:
        """Cluster-wide discovery/meta port (every host listens on it). Taken
        from the first seed; all seeds must share the same meta port."""
        return int(self.discovery_seeds()[0].rsplit(":", 1)[1])

    @classmethod
    def from_extra_config(cls, extra: dict) -> "PeerCacheConfig":
        """Build from SGLang's --hicache-storage-backend-extra-config dict."""
        if "discovery_addr" not in extra:
            raise ValueError(
                "peercache: 'discovery_addr' is required in extra_config "
                "(the meta/discovery node 'host:port')"
            )
        known = {
            "discovery_addr",
            "mode",
            "role",
            "write_policy",
            "protocol",
            "device_name",
            "device_names",
            "ib_port",
            "gid_index",
            "max_channels_per_peer",
            "global_segment_size",
            "vnodes",
            "directory_replicas",
            "directory_read_cache_ttl",
            "local_hostname",
            "rdma_bind_host",
            "rdma_port",
            "control_bind_host",
            "control_port",
            "meta_bind_host",
            "disk_enabled",
            "disk_path",
            "disk_size",
            "metrics_enabled",
            "metrics_bind_host",
            "metrics_port",
            "metrics_dashboard",
            "node_id",
            "heartbeat_interval",
            "member_ttl",
            "max_masters",
        }
        kwargs = {k: v for k, v in extra.items() if k in known}
        return cls(**kwargs)


def _resolve_local_ip(peer_addr: str) -> str:
    """Best-effort local IP that can reach the discovery node."""
    host = peer_addr.split(":")[0] if peer_addr else "8.8.8.8"
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect((host, 9))
            return s.getsockname()[0]
        finally:
            s.close()
    except Exception:
        return socket.gethostbyname(socket.gethostname())
