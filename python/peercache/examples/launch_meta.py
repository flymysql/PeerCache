"""Launch a standalone PeerCache meta (discovery) node.

This is OPTIONAL. Normally the meta service is embedded: the PeerCache node whose
IP matches `discovery_addr` auto-hosts discovery in-process, so you do not need a
separate process. Use this only if you want a dedicated discovery host that does
not serve SGLang. It does service discovery only -- no metadata and no data.

    python -m peercache.examples.launch_meta --bind 0.0.0.0:31998
"""

from __future__ import annotations

import argparse
import logging
import signal
import time

from peercache.discovery import DiscoveryServer


def main() -> None:
    parser = argparse.ArgumentParser(description="PeerCache meta/discovery node")
    parser.add_argument(
        "--bind", default="0.0.0.0:31998", help="host:port to bind (default 0.0.0.0:31998)"
    )
    parser.add_argument(
        "--member-ttl", type=float, default=6.0,
        help="seconds before a silent node is pruned (default 6.0)",
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log = logging.getLogger("peercache.meta")

    host, port = args.bind.rsplit(":", 1)
    server = DiscoveryServer(host, int(port), member_ttl=args.member_ttl)
    bound = server.start()
    log.info("PeerCache meta node listening on %s:%d", host, bound)

    stop = {"flag": False}

    def _handle(signum, frame):
        stop["flag"] = True

    signal.signal(signal.SIGINT, _handle)
    signal.signal(signal.SIGTERM, _handle)

    try:
        while not stop["flag"]:
            time.sleep(0.5)
    finally:
        server.stop()
        log.info("PeerCache meta node stopped")


if __name__ == "__main__":
    main()
