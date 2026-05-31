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


@dataclass
class PeerCacheConfig:
    # Service discovery (meta node) "host:port".
    discovery_addr: str

    # RDMA / transport.
    protocol: str = "rdma"  # "rdma" or "tcp" (fallback)
    device_name: str = ""  # e.g. "mlx5_0"; empty -> first active device
    ib_port: int = 1
    gid_index: int = 3

    # Backend-owned published pool size (host memory registered as MR).
    global_segment_size: int = 4 << 30

    # Consistent-hash directory.
    vnodes: int = 160  # virtual nodes per physical node
    directory_replicas: int = 1  # >1 replicates directory entries for HA

    # Local bind addresses. rdma_port/control_port 0 -> auto-assign.
    local_hostname: str = ""
    rdma_bind_host: str = "0.0.0.0"
    rdma_port: int = 0
    control_bind_host: str = "0.0.0.0"
    control_port: int = 0

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
            "global_segment_size",
            "vnodes",
            "directory_replicas",
            "local_hostname",
            "rdma_bind_host",
            "rdma_port",
            "control_bind_host",
            "control_port",
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
