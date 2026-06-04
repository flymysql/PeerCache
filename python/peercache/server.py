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

        # Multi-master embedded discovery: there is no dedicated meta node. EVERY
        # host runs a DiscoveryServer on the cluster-wide meta port; the active
        # masters are the `max_masters` lowest-hostname live hosts (derived by
        # the client), so a dead master is replaced automatically and a small
        # cluster has all hosts as masters. Start it before anything else so this
        # node (and peers) can register. If the port is already bound, a
        # co-located rank on this host already hosts it -> act as client only.
        seeds = config.discovery_seeds()
        meta_port = config.meta_port()
        self._meta_server: Optional[DiscoveryServer] = None
        try:
            self._meta_server = DiscoveryServer(
                config.meta_bind_host, meta_port, member_ttl=config.member_ttl
            )
            self._meta_server.start()
            logger.info(
                "This host runs an embedded PeerCache discovery master on %s:%d "
                "(seeds=%s, max_masters=%d).",
                config.meta_bind_host, meta_port, seeds, config.max_masters,
            )
        except OSError:
            self._meta_server = None
            logger.info(
                "Discovery meta port %d already bound on this host; a co-located "
                "rank hosts it. Acting as a client.", meta_port,
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
            role=config.effective_role(for_storage_server=False),
        )

        self.ring = ConsistentHashRing(config.vnodes)
        # In centralized mode the directory (and KV placement) is sharded across
        # storage nodes only; inference nodes are clients.
        self._data_ring = ConsistentHashRing(config.vnodes)
        self.discovery = DiscoveryClient(
            config.discovery_addr,
            self.info,
            on_members=self._on_members,
            heartbeat_interval=config.heartbeat_interval,
            meta_port=config.meta_port(),
            max_masters=config.max_masters,
        )
        dir_ring = self.ring
        self.directory = DirectoryClient(
            dir_ring,
            resolve_control=self.discovery.control_of,
            replicas=config.directory_replicas,
        )

    def add_member_listener(self, fn) -> None:
        """Register a callback invoked (after the ring is updated) whenever the
        live membership changes. Used by the store to re-shard the directory."""
        self._member_listeners.append(fn)

    def _on_members(self, members: List[NodeInfo]) -> None:
        all_ids = [m.node_id for m in members]
        storage_ids = [
            m.node_id for m in members
            if getattr(m, "role", "inference") == "storage"
        ]
        changed = (
            set(all_ids) != set(self.ring.nodes)
            or set(storage_ids) != set(self._data_ring.nodes)
        )
        self.ring.set_nodes(all_ids)
        self._data_ring.set_nodes(storage_ids)
        # Directory is always sharded across all live nodes so P2P and storage
        # locations share one lookup namespace in hybrid clusters.
        self.directory.set_ring(self.ring)
        logger.debug(
            "peercache membership: all=%s storage=%s mode=%s",
            all_ids, storage_ids, self.config.mode,
        )
        if changed:
            for fn in self._member_listeners:
                try:
                    fn(members)
                except Exception as e:  # a listener must never break discovery
                    logger.debug("peercache: member listener failed: %s", e)

    def data_owner(self, key: str) -> Optional[str]:
        """Return the storage node that should hold `key`, or None if no storage tier."""
        if not self._data_ring.nodes:
            return None
        return self._data_ring.get_node(key)

    def storage_nodes(self) -> List[str]:
        """Live dedicated storage node ids (empty when none registered)."""
        return list(self._data_ring.nodes)

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
