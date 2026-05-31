"""Node-side runtime that wires the control + data planes together.

A ``NodeRuntime`` owns, for one node:
- the data-plane transport (RDMA or TCP fallback),
- a control-plane RpcServer hosting this node's directory shard,
- a discovery client (register / heartbeat / membership refresh),
- the consistent-hash ring (rebuilt on every membership change),
- a directory client that routes ops to the owning shard.

The PeerCacheStore uses this runtime; it is also usable standalone in tests.
"""

from __future__ import annotations

import logging
from typing import List, Optional

from peercache.config import PeerCacheConfig
from peercache.directory import DirectoryClient, DirectoryServer
from peercache.discovery import DiscoveryClient
from peercache.hashring import ConsistentHashRing
from peercache.rpc import RpcServer
from peercache.transport import Transport, create_transport
from peercache.types import NodeInfo

logger = logging.getLogger(__name__)


class NodeRuntime:
    def __init__(self, config: PeerCacheConfig, transport: Optional[Transport] = None):
        self.config = config
        self.transport: Transport = transport or create_transport(config)

        # Control-plane RPC server (hosts the directory shard).
        self._rpc = RpcServer(config.control_bind_host, config.control_port)
        self.directory_server = DirectoryServer()
        self.directory_server.attach(self._rpc)
        control_port = self._rpc.start()

        rdma_host, rdma_port = self.transport.local_endpoint().rsplit(":", 1)
        self.info = NodeInfo(
            node_id=config.node_id,
            control_host=config.local_hostname,
            control_port=control_port,
            rdma_host=rdma_host,
            rdma_port=int(rdma_port),
        )

        self.ring = ConsistentHashRing(config.vnodes)
        self.discovery = DiscoveryClient(
            config.discovery_addr,
            self.info,
            on_members=self._on_members,
            heartbeat_interval=config.heartbeat_interval,
        )
        self.directory = DirectoryClient(
            self.ring,
            resolve_control=self.discovery.control_of,
            replicas=config.directory_replicas,
        )

    def _on_members(self, members: List[NodeInfo]) -> None:
        self.ring.set_nodes([m.node_id for m in members])
        logger.debug("peercache membership: %s", [m.node_id for m in members])

    def start(self) -> None:
        self.discovery.start()

    def stop(self) -> None:
        self.discovery.stop()
        self._rpc.stop()
        self.transport.close()

    @property
    def node_id(self) -> str:
        return self.config.node_id

    @property
    def local_rdma_endpoint(self) -> str:
        return self.transport.local_endpoint()
