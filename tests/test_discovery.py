"""Discovery client polls for the meta instead of failing on a timeout."""

import socket
import threading
import time

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
