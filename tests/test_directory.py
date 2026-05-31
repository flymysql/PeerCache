import pytest

from peercache.directory import DirectoryClient, DirectoryServer
from peercache.hashring import ConsistentHashRing
from peercache.rpc import RpcServer
from peercache.types import DataLocation


@pytest.fixture
def two_shard_cluster():
    """Two directory shards on localhost + a client that routes by hash ring."""
    servers = {}
    endpoints = {}
    shards = {}
    for node_id in ("nodeA", "nodeB"):
        rpc = RpcServer("127.0.0.1", 0)
        shard = DirectoryServer()
        shard.attach(rpc)
        port = rpc.start()
        servers[node_id] = rpc
        shards[node_id] = shard
        endpoints[node_id] = f"127.0.0.1:{port}"

    ring = ConsistentHashRing(vnodes=128)
    ring.set_nodes(list(endpoints.keys()))
    client = DirectoryClient(ring, resolve_control=endpoints.get, replicas=1)
    yield client, ring, endpoints, shards
    for rpc in servers.values():
        rpc.stop()


def _loc(node_id, addr):
    return DataLocation(
        node_id=node_id,
        rdma_endpoint=f"{node_id}:1234",
        remote_addr=addr,
        rkey=7,
        length=4096,
    )


def test_put_get_roundtrip(two_shard_cluster):
    client, _, _, _ = two_shard_cluster
    entries = {f"k{i}": _loc("nodeA", 1000 + i) for i in range(50)}
    client.put(entries)

    got = client.get(list(entries.keys()))
    assert all(g is not None for g in got)
    for key, loc in zip(entries.keys(), got):
        assert loc.remote_addr == entries[key].remote_addr
        assert loc.node_id == "nodeA"


def test_entries_land_on_owning_shard(two_shard_cluster):
    client, ring, _, shards = two_shard_cluster
    client.put({f"k{i}": _loc("nodeA", i) for i in range(100)})
    # Each entry must live exactly on the shard the ring assigns.
    total = sum(len(s) for s in shards.values())
    assert total == 100
    for i in range(100):
        owner = ring.get_node(f"k{i}")
        assert shards[owner]._on_exists({"keys": [f"k{i}"]})["exists"] == [True]


def test_exists_and_delete(two_shard_cluster):
    client, _, _, _ = two_shard_cluster
    keys = [f"k{i}" for i in range(30)]
    client.put({k: _loc("nodeB", j) for j, k in enumerate(keys)})

    assert client.exists(keys) == [True] * 30
    assert client.get(["missing"]) == [None]

    client.delete(keys[:10])
    ex = client.exists(keys)
    assert ex[:10] == [False] * 10
    assert ex[10:] == [True] * 20
