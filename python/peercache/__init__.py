"""PeerCache: peer-to-peer RDMA zero-copy L3 KV-cache backend for SGLang HiCache."""

from peercache.config import PeerCacheConfig
from peercache.hashring import ConsistentHashRing
from peercache.types import DataLocation, NodeInfo

__all__ = [
    "PeerCacheConfig",
    "ConsistentHashRing",
    "DataLocation",
    "NodeInfo",
]

__version__ = "0.2.0"
