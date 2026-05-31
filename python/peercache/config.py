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

    # RDMA / transport.
    protocol: str = "rdma"  # "rdma" or "tcp" (fallback)
    device_name: str = ""  # e.g. "mlx5_0"; empty -> first active device
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
    directory_replicas: int = 1  # >1 replicates directory entries for HA
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

    def __post_init__(self):
        if not self.local_hostname:
            self.local_hostname = _resolve_local_ip(self.discovery_addr)
        if not self.node_id:
            self.node_id = f"{self.local_hostname}-{uuid.uuid4().hex[:8]}"
        self.global_segment_size = _parse_size(self.global_segment_size)
        self.disk_size = _parse_size(self.disk_size)
        self.disk_enabled = _as_bool(self.disk_enabled)
        self.metrics_enabled = _as_bool(self.metrics_enabled)
        self.metrics_dashboard = _as_bool(self.metrics_dashboard)

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
            "protocol",
            "device_name",
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
