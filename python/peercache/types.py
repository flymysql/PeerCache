"""Shared data types for the PeerCache control plane."""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Optional


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
    rdma_endpoint: str  # "host:port" of the data node's transfer engine
    remote_addr: int  # virtual address inside that node's published-pool MR
    rkey: int  # remote key for that MR
    length: int  # bytes
    # When False the page has been evicted from the pool and only lives on the
    # owner's disk tier; remote_addr/rkey are invalid until the owner promotes
    # it back into the pool (see PeerCacheStore._ensure_resident).
    resident: bool = True

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "DataLocation":
        return cls(
            node_id=d["node_id"],
            rdma_endpoint=d["rdma_endpoint"],
            remote_addr=int(d["remote_addr"]),
            rkey=int(d["rkey"]),
            length=int(d["length"]),
            resident=bool(d.get("resident", True)),
        )
