"""Shared data types for the PeerCache control plane."""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import List, Optional

# Bumped when the on-the-wire directory entry (DataLocation) layout changes in a
# way readers must be aware of. Old fields are only ever added, never removed or
# repurposed, so from_dict stays tolerant across versions. Current additions:
#   v1: base (node_id, rdma_endpoint, remote_addr, rkey, length, resident)
#   v2: + rail_endpoints[]/rail_rkeys[] (multi-NIC)
DIRECTORY_SCHEMA_VERSION = 2


@dataclass
class NodeInfo:
    """Identity + endpoints advertised by a node to the discovery service."""

    node_id: str
    # Control-plane RPC endpoint (directory shard lives here).
    control_host: str
    control_port: int
    # RDMA QP-bootstrap endpoint (or TCP data-server endpoint in fallback mode).
    rdma_host: str
    rdma_port: int

    def rdma_endpoint(self) -> str:
        return f"{self.rdma_host}:{self.rdma_port}"

    def control_endpoint(self) -> str:
        return f"{self.control_host}:{self.control_port}"

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "NodeInfo":
        return cls(
            node_id=d["node_id"],
            control_host=d["control_host"],
            control_port=int(d["control_port"]),
            rdma_host=d["rdma_host"],
            rdma_port=int(d["rdma_port"]),
        )


@dataclass
class DataLocation:
    """Where a published KV page physically lives, stored in the directory.

    The data sits in the producing node's published-pool MR; readers use
    (rdma_endpoint, remote_addr, rkey, length) to issue a one-sided RDMA READ.
    """

    node_id: str
    rdma_endpoint: str  # rail-0 "host:port" of the data node's transfer engine
    remote_addr: int  # virtual address inside that node's published-pool MR
    rkey: int  # rail-0 remote key for that MR
    length: int  # bytes
    # When False the page has been evicted from the pool and only lives on the
    # owner's disk tier; remote_addr/rkey are invalid until the owner promotes
    # it back into the pool (see PeerCacheStore._ensure_resident).
    resident: bool = True
    # Multi-rail (multi-NIC) reachability: the same pool MR registered on every
    # rail of the owner. rail_endpoints[r] is the rail-r QP-bootstrap endpoint
    # and rail_rkeys[r] the rail-r remote key for the SAME remote_addr. A reader
    # with matching rails can stripe one-sided READs across all of them to use
    # several NICs from a single process. Empty/length-1 == single-rail (the
    # legacy rdma_endpoint/rkey). Defaults keep old producers wire-compatible.
    rail_endpoints: List[str] = field(default_factory=list)
    rail_rkeys: List[int] = field(default_factory=list)

    def endpoints(self) -> List[str]:
        return self.rail_endpoints or [self.rdma_endpoint]

    def rkeys(self) -> List[int]:
        return self.rail_rkeys or [self.rkey]

    def to_dict(self) -> dict:
        d = asdict(self)
        d["v"] = DIRECTORY_SCHEMA_VERSION  # schema version for forward/back compat
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "DataLocation":
        # Tolerant of any schema version: unknown keys are ignored and missing
        # newer fields fall back to single-rail / legacy values.
        return cls(
            node_id=d["node_id"],
            rdma_endpoint=d["rdma_endpoint"],
            remote_addr=int(d["remote_addr"]),
            rkey=int(d["rkey"]),
            length=int(d["length"]),
            resident=bool(d.get("resident", True)),
            rail_endpoints=list(d.get("rail_endpoints", []) or []),
            rail_rkeys=[int(x) for x in (d.get("rail_rkeys", []) or [])],
        )
