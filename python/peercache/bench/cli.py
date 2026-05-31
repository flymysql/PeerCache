"""Unified ``peercache-bench`` command line.

A single entry point that dispatches to every benchmark via subcommands:

    peercache-bench latency      ...   # single in-flight op latency baseline
    peercache-bench throughput   ...   # fixed concurrency throughput
    peercache-bench saturation   ...   # concurrency sweep -> peak throughput
    peercache-bench suite        ...   # full systematic SGLang-HiCache baseline
    peercache-bench micro        ...   # low-level data-plane microbench
    peercache-bench mooncake     ...   # Mooncake transfer_engine_bench wrapper
    peercache-bench compare      ...   # PeerCache vs Mooncake sweep
    peercache-bench serve        ...   # two-host: producer/data node
    peercache-bench drive        ...   # two-host: consumer/driver sweep

Run ``peercache-bench <subcommand> --help`` for that subcommand's options.
"""

from __future__ import annotations

import argparse
import logging

from peercache.bench import compare, dist, hicache, microbench, mooncake_bench


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog="peercache-bench",
        description="PeerCache benchmark suite (SGLang-HiCache, RDMA-first). "
        "TCP is for functional smoke testing only and must not be published.",
    )
    # Logging is off by default (only WARNING+ reaches stderr). Use --log-level
    # info to confirm the transport actually selected RDMA (look for the
    # "PeerCacheStore up: ... rdma=..." line) and to surface the TCP-fallback
    # warning; --log-file additionally tees everything to a file.
    ap.add_argument(
        "--log-level", default="warning",
        choices=["debug", "info", "warning", "error"],
        help="console/file log verbosity (default: warning)",
    )
    ap.add_argument(
        "--log-file", default="",
        help="also append logs to this file (default: none)",
    )
    sub = ap.add_subparsers(dest="command", required=True, metavar="<subcommand>")
    hicache.add_subparsers(sub)        # latency / throughput / saturation / suite
    microbench.add_subparser(sub)      # micro
    mooncake_bench.add_subparser(sub)  # mooncake
    compare.add_subparser(sub)         # compare
    dist.add_subparsers(sub)           # serve / drive (two-host)
    return ap


def _setup_logging(level: str, log_file: str) -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if log_file:
        handlers.append(logging.FileHandler(log_file))
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.WARNING),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=handlers,
    )


def main() -> None:
    args = build_parser().parse_args()
    _setup_logging(getattr(args, "log_level", "warning"), getattr(args, "log_file", ""))
    args._handler(args)


if __name__ == "__main__":
    main()
