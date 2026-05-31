"""Run the PeerCache vs Mooncake baseline sweep and emit a report.

Runs, for each block size in the sweep:
  * PeerCache ``transport-read`` (data-plane one-sided READ),
  * PeerCache ``store-get``      (full HiCache store path),
  * Mooncake  ``transfer-engine`` (official transfer_engine_bench), if available.

Writes a JSON artifact and a Markdown table to ``benchmarks/results/``.

Examples
--------
Sandbox (no RDMA, TCP fallback -- validation only, NOT a marketing number):
    PYTHONPATH=python:benchmarks python benchmarks/run_baseline.py \
        --protocol tcp --block-sizes 4096,65536,1048576 --duration 5

RDMA hardware (the configuration that produces publishable numbers):
    # On the target/producer node and initiator/consumer node, see README.
    PYTHONPATH=python:benchmarks python benchmarks/run_baseline.py \
        --protocol rdma --device-name mlx5_0 \
        --block-sizes 4096,16384,65536,262144,1048576 --duration 10
"""

from __future__ import annotations

import argparse
import os
import time

from peercache.bench.common import BaselineReport, Workload, render_console

from peercache.bench import microbench as bench_peercache
from peercache.bench import mooncake_bench as bench_mooncake


def parse_sizes(s: str):
    out = []
    for tok in s.split(","):
        tok = tok.strip().lower()
        if not tok:
            continue
        mult = 1
        if tok.endswith("kb"):
            mult, tok = 1024, tok[:-2]
        elif tok.endswith("mb"):
            mult, tok = 1024 * 1024, tok[:-2]
        elif tok.endswith("k"):
            mult, tok = 1024, tok[:-1]
        elif tok.endswith("m"):
            mult, tok = 1024 * 1024, tok[:-1]
        out.append(int(float(tok) * mult))
    return out


def _add_args(p) -> None:
    p.add_argument("--protocol", default="rdma", choices=["rdma", "tcp"])
    p.add_argument("--device-name", default="", help="RDMA device for both systems, e.g. mlx5_0")
    p.add_argument("--block-sizes", default="4096,65536,1048576",
                   help="comma list, e.g. 4k,64k,1m")
    p.add_argument("--batch-size", type=int, default=64)
    p.add_argument("--threads", type=int, default=1, help="PeerCache submit threads")
    p.add_argument("--mooncake-threads", type=int, default=4,
                   help="Mooncake submit threads (its bench is multi-threaded by design)")
    p.add_argument("--duration", type=float, default=5.0)
    p.add_argument("--warmup", type=float, default=1.0)
    p.add_argument("--skip-mooncake", action="store_true")
    p.add_argument("--skip-store", action="store_true")
    p.add_argument("--out-dir", default=os.path.join(os.getcwd(), "peercache-bench-results"))
    p.add_argument("--tag", default="", help="optional label suffix for output files")


def run(args) -> None:
    report = BaselineReport()

    # Start one shared Mooncake metadata server for the whole sweep (avoids
    # per-point startup churn and orphaned processes).
    meta_proc = None
    meta_url = None
    if not args.skip_mooncake:
        try:
            meta_proc, meta_url = bench_mooncake.start_metadata_server()
            print(f"[mooncake] metadata server: {meta_url}")
        except Exception as e:  # noqa: BLE001
            print(f"[mooncake] metadata server unavailable ({e}); Mooncake rows skipped")

    try:
        for bs in parse_sizes(args.block_sizes):
            wl = Workload(
                block_size=bs,
                batch_size=args.batch_size,
                threads=args.threads,
                duration=args.duration,
                warmup=args.warmup,
            )
            print(f"[run] PeerCache transport-read  block={bs}")
            report.add(bench_peercache.bench_transport_read(wl, args.protocol, args.device_name))

            if not args.skip_store:
                print(f"[run] PeerCache store-get       block={bs}")
                report.add(bench_peercache.bench_store_get(wl, args.protocol, args.device_name))

            if not args.skip_mooncake and meta_url is not None:
                print(f"[run] Mooncake transfer-engine  block={bs}")
                mwl = Workload(
                    block_size=bs,
                    batch_size=args.batch_size,
                    threads=args.mooncake_threads,
                    duration=args.duration,
                    warmup=args.warmup,
                )
                report.add(
                    bench_mooncake.bench_transfer_engine(
                        mwl, args.protocol, args.device_name, metadata_url=meta_url
                    )
                )
    finally:
        if meta_proc is not None:
            bench_mooncake._kill(meta_proc)

    print()
    print(render_console(report))

    os.makedirs(args.out_dir, exist_ok=True)
    ts = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
    suffix = f"-{args.tag}" if args.tag else ""
    base = f"compare-{args.protocol}{suffix}-{ts}"
    json_path = os.path.join(args.out_dir, base + ".json")
    md_path = os.path.join(args.out_dir, base + ".md")
    with open(json_path, "w") as f:
        f.write(report.to_json())
    with open(md_path, "w") as f:
        f.write("# PeerCache vs Mooncake baseline\n\n")
        f.write(f"- created: {report.created_at}\n")
        f.write(f"- protocol: `{args.protocol}`")
        if args.device_name:
            f.write(f"  device: `{args.device_name}`")
        f.write(f"\n- host: {report.host.get('platform')} (cpus={report.host.get('cpu_count')})\n\n")
        f.write(render_console(report).split("\n\n", 1)[-1])
        f.write("\n")
    print(f"\nwrote {json_path}\nwrote {md_path}")


def add_subparser(sub) -> None:
    p = sub.add_parser("compare", help="PeerCache vs Mooncake sweep (matched block sizes)")
    _add_args(p)
    p.set_defaults(_handler=run)


def main() -> None:
    ap = argparse.ArgumentParser(description="PeerCache vs Mooncake baseline")
    _add_args(ap)
    run(ap.parse_args())


if __name__ == "__main__":
    main()
