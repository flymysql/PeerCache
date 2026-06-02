"""Discovery client polls for the meta instead of failing on a timeout."""

import socket
import threading
import time

import pytest

from peercache.discovery import DiscoveryClient, DiscoveryServer
from peercache.types import NodeInfo


def _free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _info(nid: str) -> NodeInfo:
    return NodeInfo(node_id=nid, control_host="127.0.0.1", control_port=1,
                    rdma_host="127.0.0.1", rdma_port=2)


def _host_info(host: str) -> NodeInfo:
    return NodeInfo(node_id=f"{host}-n", control_host=host, control_port=1,
                    rdma_host=host, rdma_port=2)


def _loopback_aliases_usable(hosts) -> bool:
    """127.0.0.0/8 is fully routable to lo on Linux but not on macOS."""
    for h in hosts:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind((h, 0))
            s.close()
        except OSError:
            return False
    return True


def test_register_polls_until_meta_is_up():
    port = _free_port()
    addr = f"127.0.0.1:{port}"
    client = DiscoveryClient(addr, _info("n1"), heartbeat_interval=0.2,
                             register_retry_interval=0.2)

    # start() blocks in the (polling) register until the meta exists -> run it
    # in a thread. It must NOT raise even though nothing is listening yet.
    t = threading.Thread(target=client.start, daemon=True)
    t.start()
    time.sleep(0.6)
    assert "n1" not in client.members()  # still polling, meta not up

    server = DiscoveryServer("127.0.0.1", port)
    server.start()
    try:
        deadline = time.time() + 5
        while time.time() < deadline and "n1" not in client.members():
            time.sleep(0.1)
        assert "n1" in client.members()  # registered once the meta came up
    finally:
        client.stop()
        server.stop()


def _wait(cond, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return True
        time.sleep(0.1)
    return cond()


def test_multi_master_three_masters_and_failover():
    hosts = ["127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4"]
    if not _loopback_aliases_usable(hosts):
        pytest.skip("loopback aliases (127.0.0.2+) not bindable on this OS")
    port = _free_port()
    seeds = ",".join(f"{h}:{port}" for h in hosts[:2])  # bootstrap via 2 seeds

    servers = {h: DiscoveryServer(h, port, member_ttl=2.0) for h in hosts}
    for s in servers.values():
        s.start()
    clients = {
        h: DiscoveryClient(seeds, _host_info(h), heartbeat_interval=0.2,
                           register_retry_interval=0.2, max_masters=3)
        for h in hosts
    }
    threads = [threading.Thread(target=c.start, daemon=True) for c in clients.values()]
    for t in threads:
        t.start()
    try:
        c1 = clients["127.0.0.1"]
        # All 4 nodes converge, and exactly the 3 lowest hosts are masters.
        assert _wait(lambda: len(c1.members()) == 4)
        assert _wait(lambda: c1.master_hosts() == hosts[:3])

        # Kill the two lowest masters (incl. a seed) + their nodes.
        for h in ("127.0.0.1", "127.0.0.2"):
            clients[h].stop()
            servers[h].stop()
        c3 = clients["127.0.0.3"]
        # Survivors prune the dead nodes and re-derive masters from who's left;
        # discovery keeps working through the remaining master(s)/seed.
        assert _wait(lambda: set(c3.members().keys()) == {"127.0.0.3-n", "127.0.0.4-n"})
        assert _wait(lambda: c3.master_hosts() == ["127.0.0.3", "127.0.0.4"])
    finally:
        for h in ("127.0.0.3", "127.0.0.4"):
            clients[h].stop()
            servers[h].stop()


def test_multi_master_small_cluster_all_masters():
    hosts = ["127.0.0.1", "127.0.0.2"]
    if not _loopback_aliases_usable(hosts):
        pytest.skip("loopback aliases not bindable on this OS")
    port = _free_port()
    seeds = f"127.0.0.1:{port}"
    servers = {h: DiscoveryServer(h, port, member_ttl=3.0) for h in hosts}
    for s in servers.values():
        s.start()
    clients = {
        h: DiscoveryClient(seeds, _host_info(h), heartbeat_interval=0.2,
                           register_retry_interval=0.2, max_masters=3)
        for h in hosts
    }
    for c in clients.values():
        threading.Thread(target=c.start, daemon=True).start()
    try:
        c1 = clients["127.0.0.1"]
        assert _wait(lambda: len(c1.members()) == 2)
        # Fewer hosts than max_masters -> every host is a master.
        assert _wait(lambda: c1.master_hosts() == hosts)
    finally:
        for c in clients.values():
            c.stop()
        for s in servers.values():
            s.stop()
