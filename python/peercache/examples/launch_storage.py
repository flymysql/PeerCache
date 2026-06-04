"""Launch a dedicated PeerCache storage server (centralized mode).

Storage nodes hold the KV pool and directory shards; SGLang inference workers
connect with ``mode=centralized`` and ``role=inference`` (the default for
``PeerCacheStore``).

Example::

    peercache-storage-server \\
        --discovery-addr 10.0.0.1:31998 \\
        --global-segment-size 64gb \\
        --disk-path /data/peercache/ \\
        --protocol rdma \\
        --device-names mlx5_bond_1,mlx5_bond_2
"""

from __future__ import annotations

import argparse
import logging

from peercache.config import PeerCacheConfig
from peercache.storage_server import StorageServer


def _build_config(args: argparse.Namespace) -> PeerCacheConfig:
    return PeerCacheConfig(
        discovery_addr=args.discovery_addr,
        role="storage",
        protocol=args.protocol,
        device_name=args.device_name,
        device_names=args.device_names,
        global_segment_size=args.global_segment_size,
        disk_enabled=not args.no_disk,
        disk_path=args.disk_path,
        disk_size=args.disk_size,
        local_hostname=args.local_hostname,
        metrics_port=args.metrics_port,
        node_id=args.node_id,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="PeerCache centralized storage server (KV pool + directory shard)"
    )
    parser.add_argument(
        "--discovery-addr", required=True,
        help="Meta/discovery seed host:port (same as inference nodes)",
    )
    parser.add_argument(
        "--global-segment-size", default="4gb",
        help="Published pool size (default 4gb)",
    )
    parser.add_argument(
        "--disk-path", default="/data/peercache/",
        help="Disk tier directory (default /data/peercache/)",
    )
    parser.add_argument(
        "--disk-size", default="100gb",
        help="Disk tier capacity (default 100gb)",
    )
    parser.add_argument("--no-disk", action="store_true", help="Disable disk tier")
    parser.add_argument(
        "--protocol", default="rdma", choices=("rdma", "tcp"),
        help="Data-plane transport (default rdma)",
    )
    parser.add_argument("--device-name", default="", help="Single RDMA device")
    parser.add_argument(
        "--device-names", default="",
        help="Comma-separated multi-rail RDMA devices",
    )
    parser.add_argument(
        "--local-hostname", default="",
        help="Advertised control/RDMA host (auto-detected if empty)",
    )
    parser.add_argument(
        "--metrics-port", type=int, default=31997,
        help="Metrics HTTP port (default 31997)",
    )
    parser.add_argument("--node-id", default="", help="Optional fixed node id")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    server = StorageServer(_build_config(args))
    server.run_forever()


if __name__ == "__main__":
    main()
