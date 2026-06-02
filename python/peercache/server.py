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
import socket
from typing import List, Optional

from peercache.config import PeerCacheConfig
from peercache.directory import DirectoryClient, DirectoryServer
from peercache.discovery import DiscoveryClient, DiscoveryServer
from peercache.hashring import ConsistentHashRing
from peercache.rpc import RpcServer
from peercache.transport import Transport, create_transport
from peercache.types import NodeInfo

logger = logging.getLogger(__name__)


def _local_ip_set(local_hostname: str) -> set:
    """Best-effort set of addresses that identify this host."""
    ips = {"127.0.0.1", "0.0.0.0", "::1", "localhost", local_hostname}
    try:
        hostname = socket.gethostname()
        ips.add(hostname)
        ips.add(socket.gethostbyname(hostname))
        for info in socket.getaddrinfo(hostname, None):
            ips.add(info[4][0])
    except Exception:
        pass
    return ips


def host_is_self(host: str, local_hostname: str) -> bool:
    """True if `host` refers to this node (so it should host the meta service)."""
    if host in ("127.0.0.1", "0.0.0.0", "::1", "localhost"):
        return True
    local = _local_ip_set(local_hostname)
    if host in local:
        return True
    try:
        if socket.gethostbyname(host) in local:
            return True
    except Exception:
        pass
    return False


class NodeRuntime:
    def __init__(self, config: PeerCacheConfig, transport: Optional[Transport] = None):
        self.config = config
        self._member_listeners: List = []

        # Embedded meta: there is no separate meta node. The node whose IP matches
        # `discovery_addr` automatically hosts the discovery service. Start it
        # before anything else so this node (and peers) can register against it.
        self._meta_server: Optional[DiscoveryServer] = None
        disc_host, disc_port = config.discovery_addr.rsplit(":", 1)
        if host_is_self(disc_host, config.local_hostname):
            # This node is the designated meta. If the port is already bound,
            # another local node already hosts it (e.g. several SGLang ranks on
            # one box, or an externally-launched meta) -> just act as a client.
            try:
                self._meta_server = DiscoveryServer(
                    config.meta_bind_host, int(disc_port), member_ttl=config.member_ttl
                )
                self._meta_server.start()
                logger.info(
                    "This node hosts the embedded PeerCache meta/discovery service "
                    "on %s:%s (discovery_addr=%s resolves to self)",
                    config.meta_bind_host,
                    disc_port,
                    config.discovery_addr,
                )
            except OSError:
                self._meta_server = None
                logger.info(
                    "Embedded meta port %s already bound; another local node hosts "
                    "the discovery service. Acting as a client.",
                    disc_port,
                )
        else:
            logger.info(
                "Discovery meta runs on %s (this node %s is a client, not the meta).",
                config.discovery_addr, config.node_id,
            )

        self.transport: Transport = transport or create_transport(config)

        # Control-plane RPC server (hosts the directory shard). Exposed as
        # `control_rpc` so the store can register data-plane handlers (promote).
        self._rpc = RpcServer(config.control_bind_host, config.control_port)
        self.control_rpc = self._rpc
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

    def add_member_listener(self, fn) -> None:
        """Register a callback invoked (after the ring is updated) whenever the
        live membership changes. Used by the store to re-shard the directory."""
        self._member_listeners.append(fn)

    def _on_members(self, members: List[NodeInfo]) -> None:
        node_ids = [m.node_id for m in members]
        changed = set(node_ids) != set(self.ring.nodes)
        self.ring.set_nodes(node_ids)
        logger.debug("peercache membership: %s", node_ids)
        if changed:
            for fn in self._member_listeners:
                try:
                    fn(members)
                except Exception as e:  # a listener must never break discovery
                    logger.debug("peercache: member listener failed: %s", e)

    def start(self) -> None:
        self.discovery.start()

    def stop(self) -> None:
        # Idempotent. Deregister from discovery FIRST so peers drop us from the
        # ring (stop routing here) before we tear down the RPC/RDMA endpoints.
        if getattr(self, "_stopped", False):
            return
        self._stopped = True
        self.discovery.stop()
        self._rpc.stop()
        self.transport.close()
        if self._meta_server is not None:
            self._meta_server.stop()

    @property
    def is_meta(self) -> bool:
        return self._meta_server is not None

    @property
    def node_id(self) -> str:
        return self.config.node_id

    @property
    def local_rdma_endpoint(self) -> str:
        return self.transport.local_endpoint()
