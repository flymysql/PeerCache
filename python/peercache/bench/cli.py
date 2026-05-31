"""Unified ``peercache-bench`` command line.

A single entry point that dispatches to every benchmark via subcommands:

    peercache-bench latency      ...   # single in-flight op latency baseline
    peercache-bench throughput   ...   # fixed concurrency throughput
    peercache-bench saturation   ...   # concurrency sweep -> peak throughput
    peercache-bench suite        ...   # full systematic SGLang-HiCache baseline
    peercache-bench micro        ...   # low-level data-plane microbench
    peercache-bench mooncake     ...   # Mooncake transfer_engine_bench wrapper
    peercache-bench compare      ...   # PeerCache vs Mooncake sweep

Run ``peercache-bench <subcommand> --help`` for that subcommand's options.
"""

from __future__ import annotations

import argparse

from peercache.bench import compare, hicache, microbench, mooncake_bench


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog="peercache-bench",
        description="PeerCache benchmark suite (SGLang-HiCache, RDMA-first). "
        "TCP is for functional smoke testing only and must not be published.",
    )
    sub = ap.add_subparsers(dest="command", required=True, metavar="<subcommand>")
    hicache.add_subparsers(sub)        # latency / throughput / saturation / suite
    microbench.add_subparser(sub)      # micro
    mooncake_bench.add_subparser(sub)  # mooncake
    compare.add_subparser(sub)         # compare
    return ap


def main() -> None:
    args = build_parser().parse_args()
    args._handler(args)


if __name__ == "__main__":
    main()
