"""Systematic SGLang-HiCache benchmark for PeerCache.

Drives PeerCache's ``HiCacheStorage`` interface exactly as SGLang HiCache does,
modelling **PD-disaggregated** inference:

    prefill node  --batch_set_v1-->  publish KV pages   (write / offload)
    decode node   --batch_exists-->  probe cached prefix (lookup)
                  --batch_get_v1-->  load pages over RDMA (read / prefetch)

It produces the systematic baseline you can publish: throughput (pages/s,
tokens/s, GB/s) and latency tail (p50/p95/p99/p999/max) under a sweep of
**thread models** (concurrency), including the full-load saturation point
(maximum sustained throughput).

RDMA-first. Run on a host with an RDMA NIC:

    PYTHONPATH=python:benchmarks python benchmarks/bench_hicache.py suite \
        --device-name mlx5_0 --layout mla \
        --page-size 131072 --tokens-per-page 64 \
        --batch-size 32 --concurrencies 1,2,4,8,16,32 \
        --duration 10 --warmup 2

Modes
-----
  latency     single in-flight op (concurrency 1, batch 1) -> per-op tail
  throughput  fixed concurrency -> sustained throughput + tail
  saturation  concurrency sweep -> throughput/latency curve + peak
  suite       latency + get/set saturation + exists, written to results/

The TCP fallback works for functional smoke testing only; it is not a
performance scenario and its numbers must not be published.
"""

from __future__ import annotations

import argparse
import os
import threading
import time
from typing import Callable, List, Optional, Tuple

from peercache.bench.common import (
    BaselineReport,
    Histogram,
    Workload,
    human_bytes,
    make_result,
    render_console,
)
from peercache.bench.sglang_sim import Cluster


# --------------------------------------------------------------------------- #
# Concurrency driver
# --------------------------------------------------------------------------- #
# A worker returns (ops, bytes, pages, hits, calls, Histogram of call latency).
WorkerResult = Tuple[int, int, int, int, int, Histogram]
WorkerFn = Callable[[int, float, float], WorkerResult]


def _drive(n_threads: int, worker: WorkerFn, warmup: float, duration: float):
    results: List[Optional[WorkerResult]] = [None] * n_threads
    barrier = threading.Barrier(n_threads)

    def run(tid: int) -> None:
        barrier.wait()
        start = time.perf_counter()
        warm_end = start + warmup
        end = warm_end + duration
        results[tid] = worker(tid, warm_end, end)

    threads = [threading.Thread(target=run, args=(i,), daemon=True) for i in range(n_threads)]
    t0 = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    wall = time.perf_counter() - t0

    ops = sum(r[0] for r in results)
    by = sum(r[1] for r in results)
    pages = sum(r[2] for r in results)
    hits = sum(r[3] for r in results)
    calls = sum(r[4] for r in results)
    hist = Histogram()
    for r in results:
        hist.merge(r[5])
    elapsed = min(duration, wall) if wall > 0 else duration
    return ops, by, pages, hits, calls, hist, elapsed


# --------------------------------------------------------------------------- #
# Sizing
# --------------------------------------------------------------------------- #
def _page_total(page_size: int, layout: str) -> int:
    return page_size * (1 if layout == "mla" else 2)


def _budget_ok(total_bytes: int, max_bytes: int, what: str) -> None:
    if total_bytes > max_bytes:
        raise SystemExit(
            f"[bench_hicache] {what} needs {human_bytes(total_bytes)} which exceeds "
            f"--max-bytes {human_bytes(max_bytes)}. Lower --batch-size / max "
            f"--concurrencies / --working-set / --page-size, or raise --max-bytes."
        )


# --------------------------------------------------------------------------- #
# GET (read / prefetch) sweep
# --------------------------------------------------------------------------- #
def run_get(args, concurrencies: List[int]) -> List:
    layout = args.layout
    pt = _page_total(args.page_size, layout)
    comps = 1 if layout == "mla" else 2
    bs = args.batch_size
    max_conc = max(concurrencies)

    consumer_slots = max_conc * bs
    working_set = max(args.working_set, bs * max_conc * 2)
    # Bound producer working set + both pools by the memory budget.
    seg_bytes = int(working_set * pt * 1.3)
    total = 2 * seg_bytes + consumer_slots * pt + working_set * pt
    if total > args.max_bytes:
        working_set = max(bs * max_conc, args.max_bytes // (4 * pt))
        seg_bytes = int(working_set * pt * 1.3)
        total = 2 * seg_bytes + consumer_slots * pt + working_set * pt
    _budget_ok(total, args.max_bytes, "GET sweep")

    cluster = Cluster(
        n_nodes=2, protocol=args.protocol, device_name=args.device_name,
        seg_bytes=seg_bytes, layout=layout, disk=args.disk, metrics=False,
        ib_port=args.ib_port, gid_index=args.gid_index,
    )
    rows = []
    try:
        producer = cluster.producer()
        consumer = cluster.consumer()
        # Producer needs `working_set` source slots; consumer needs read slots.
        from peercache.bench.sglang_sim import HostKVPool

        prod_pool = HostKVPool(args.page_size, working_set, layout=layout)
        producer.register_mem_pool_host(prod_pool)
        cons_pool = HostKVPool(args.page_size, consumer_slots, layout=layout)
        consumer.register_mem_pool_host(cons_pool)
        for i in range(working_set):
            prod_pool.fill_slot(i, i)

        keys = [f"pcbench/get/{i}" for i in range(working_set)]
        # Publish the working set in chunks (prefill offload).
        for lo in range(0, working_set, 256):
            chunk = list(range(lo, min(lo + 256, working_set)))
            producer.batch_set_v1([keys[i] for i in chunk], chunk)

        for conc in concurrencies:
            def worker(tid, warm_end, end):
                base = tid * bs
                idxs = list(range(base, base + bs))
                hist = Histogram()
                ops = by = pages = hits = calls = 0
                r = 0
                measuring = False
                while True:
                    now = time.perf_counter()
                    if now >= end:
                        break
                    if not measuring and now >= warm_end:
                        measuring = True
                    kstart = (tid * 7919 + r * bs) % working_set
                    kk = [keys[(kstart + j) % working_set] for j in range(bs)]
                    t0 = time.perf_counter()
                    oks = consumer.batch_get_v1(kk, idxs)
                    t1 = time.perf_counter()
                    n = sum(1 for o in oks if o)
                    r += 1
                    if measuring:
                        calls += 1
                        hist.record(t1 - t0)
                        hits += n
                        ops += n * comps
                        pages += n
                        by += n * pt
                return ops, by, pages, hits, calls, hist

            ops, by, pages, hits, calls, hist, elapsed = _drive(
                conc, worker, args.warmup, args.duration
            )
            wl = Workload(block_size=args.page_size, batch_size=bs, threads=conc,
                          duration=args.duration, operation="read")
            expected = calls * bs
            hit_rate = (hits / expected) if expected else float("nan")
            rows.append(make_result(
                "peercache", "hicache-get", args.protocol, wl, ops, by, elapsed,
                hist=hist, op="get", pages=pages, tokens_per_page=args.tokens_per_page,
                hit_rate=hit_rate,
                note=f"batch_get_v1 {layout} ws={working_set}",
            ))
    finally:
        cluster.close()
    return rows


# --------------------------------------------------------------------------- #
# SET (write / offload) sweep
# --------------------------------------------------------------------------- #
def run_set(args, concurrencies: List[int]) -> List:
    layout = args.layout
    pt = _page_total(args.page_size, layout)
    comps = 1 if layout == "mla" else 2
    bs = args.batch_size
    max_conc = max(concurrencies)

    prod_slots = max_conc * bs
    # Pool capacity: hold a few thousand fresh pages before LRU eviction kicks in.
    resident_pages = max(prod_slots * 8, 2048)
    seg_bytes = int(resident_pages * pt * 1.3)
    total = 2 * seg_bytes + prod_slots * pt
    if total > args.max_bytes:
        resident_pages = max(prod_slots, args.max_bytes // (3 * pt))
        seg_bytes = int(resident_pages * pt * 1.3)
        total = 2 * seg_bytes + prod_slots * pt
    _budget_ok(total, args.max_bytes, "SET sweep")

    cluster = Cluster(
        n_nodes=2, protocol=args.protocol, device_name=args.device_name,
        seg_bytes=seg_bytes, layout=layout, disk=args.disk, metrics=False,
        ib_port=args.ib_port, gid_index=args.gid_index,
    )
    rows = []
    try:
        from peercache.bench.sglang_sim import HostKVPool

        producer = cluster.producer()
        prod_pool = HostKVPool(args.page_size, prod_slots, layout=layout)
        producer.register_mem_pool_host(prod_pool)
        for i in range(prod_slots):
            prod_pool.fill_slot(i, i)

        for conc in concurrencies:
            def worker(tid, warm_end, end):
                base = tid * bs
                idxs = list(range(base, base + bs))
                hist = Histogram()
                ops = by = pages = hits = calls = 0
                r = 0
                measuring = False
                while True:
                    now = time.perf_counter()
                    if now >= end:
                        break
                    if not measuring and now >= warm_end:
                        measuring = True
                    # Fresh unique keys each call -> a real publish (not skipped).
                    kk = [f"pcbench/set/{tid}/{r}/{j}" for j in range(bs)]
                    t0 = time.perf_counter()
                    oks = producer.batch_set_v1(kk, idxs)
                    t1 = time.perf_counter()
                    n = sum(1 for o in oks if o)
                    r += 1
                    if measuring:
                        calls += 1
                        hist.record(t1 - t0)
                        hits += n
                        ops += n * comps
                        pages += n
                        by += n * pt
                return ops, by, pages, hits, calls, hist

            ops, by, pages, hits, calls, hist, elapsed = _drive(
                conc, worker, args.warmup, args.duration
            )
            wl = Workload(block_size=args.page_size, batch_size=bs, threads=conc,
                          duration=args.duration, operation="write")
            rows.append(make_result(
                "peercache", "hicache-set", args.protocol, wl, ops, by, elapsed,
                hist=hist, op="set", pages=pages, tokens_per_page=args.tokens_per_page,
                note=f"batch_set_v1 {layout}",
            ))
    finally:
        cluster.close()
    return rows


# --------------------------------------------------------------------------- #
# EXISTS (prefix lookup) sweep
# --------------------------------------------------------------------------- #
def run_exists(args, concurrencies: List[int]) -> List:
    layout = args.layout
    pt = _page_total(args.page_size, layout)
    bs = args.batch_size
    max_conc = max(concurrencies)
    working_set = max(args.working_set, bs * 4)
    seg_bytes = int(working_set * pt * 1.3)
    total = 2 * seg_bytes + working_set * pt
    _budget_ok(total, args.max_bytes, "EXISTS sweep")

    cluster = Cluster(
        n_nodes=2, protocol=args.protocol, device_name=args.device_name,
        seg_bytes=seg_bytes, layout=layout, disk=args.disk, metrics=False,
        ib_port=args.ib_port, gid_index=args.gid_index,
    )
    rows = []
    try:
        from peercache.bench.sglang_sim import HostKVPool

        producer = cluster.producer()
        consumer = cluster.consumer()
        prod_pool = HostKVPool(args.page_size, working_set, layout=layout)
        producer.register_mem_pool_host(prod_pool)
        consumer.register_mem_pool_host(HostKVPool(args.page_size, bs, layout=layout))
        for i in range(working_set):
            prod_pool.fill_slot(i, i)
        keys = [f"pcbench/ex/{i}" for i in range(working_set)]
        for lo in range(0, working_set, 256):
            chunk = list(range(lo, min(lo + 256, working_set)))
            producer.batch_set_v1([keys[i] for i in chunk], chunk)

        for conc in concurrencies:
            def worker(tid, warm_end, end):
                hist = Histogram()
                pages = hits = calls = 0
                r = 0
                measuring = False
                while True:
                    now = time.perf_counter()
                    if now >= end:
                        break
                    if not measuring and now >= warm_end:
                        measuring = True
                    kstart = (tid * 7919 + r * bs) % working_set
                    kk = [keys[(kstart + j) % working_set] for j in range(bs)]
                    t0 = time.perf_counter()
                    n = consumer.batch_exists(kk)
                    t1 = time.perf_counter()
                    r += 1
                    if measuring:
                        calls += 1
                        hist.record(t1 - t0)
                        hits += n
                        pages += bs
                return 0, 0, pages, hits, calls, hist

            _, _, pages, hits, calls, hist, elapsed = _drive(
                conc, worker, args.warmup, args.duration
            )
            wl = Workload(block_size=args.page_size, batch_size=bs, threads=conc,
                          duration=args.duration, operation="exists")
            rows.append(make_result(
                "peercache", "hicache-exists", args.protocol, wl, pages, 0, elapsed,
                hist=hist, op="exists", pages=pages, tokens_per_page=args.tokens_per_page,
                note="batch_exists (directory only)",
            ))
    finally:
        cluster.close()
    return rows


# --------------------------------------------------------------------------- #
# Reporting
# --------------------------------------------------------------------------- #
def _peak_note(rows: List) -> None:
    ok = [r for r in rows if r.ok and r.pages_per_s > 0]
    if not ok:
        return
    peak = max(ok, key=lambda r: r.pages_per_s)
    peak.note = (peak.note + " [PEAK]").strip()


def _write(report: BaselineReport, out_dir: str, base: str) -> None:
    os.makedirs(out_dir, exist_ok=True)
    jp = os.path.join(out_dir, base + ".json")
    mp = os.path.join(out_dir, base + ".md")
    with open(jp, "w") as f:
        f.write(report.to_json())
    with open(mp, "w") as f:
        f.write("# PeerCache HiCache benchmark\n\n")
        f.write(f"- created: {report.created_at}\n")
        f.write("- meta: " + ", ".join(f"{k}={v}" for k, v in report.meta.items()) + "\n")
        f.write(f"- host: {report.host.get('platform')} (cpus={report.host.get('cpu_count')})\n\n")
        f.write(render_console(report, hicache=True).split("\n\n", 1)[-1])
        f.write("\n")
    print(f"\nwrote {jp}\nwrote {mp}")


def _common_args(ap: argparse.ArgumentParser) -> None:
    ap.add_argument("--protocol", default="rdma", choices=["rdma", "tcp"])
    ap.add_argument("--device-name", default="", help="RDMA device, e.g. mlx5_0")
    ap.add_argument("--ib-port", type=int, default=1)
    ap.add_argument("--gid-index", type=int, default=3)
    ap.add_argument("--layout", default="mla", choices=["mla", "mha"],
                    help="mla: 1 object/page; mha: k+v (2 objects/page)")
    ap.add_argument("--page-size", type=int, default=131072,
                    help="bytes per KV component object (k or v)")
    ap.add_argument("--tokens-per-page", type=int, default=64,
                    help="tokens represented by one page (for tokens/s)")
    ap.add_argument("--batch-size", type=int, default=32, help="pages per batch call")
    ap.add_argument("--duration", type=float, default=10.0)
    ap.add_argument("--warmup", type=float, default=2.0)
    ap.add_argument("--working-set", type=int, default=4096, help="distinct pages for get/exists")
    ap.add_argument("--disk", action="store_true", help="enable disk write-through tier")
    ap.add_argument("--max-bytes", type=int, default=8 * (1 << 30),
                    help="host-memory budget guard for buffers/pools")
    ap.add_argument("--out-dir", default=os.path.join(os.getcwd(), "peercache-bench-results"))
    ap.add_argument("--tag", default="")


def _csv_ints(s: str) -> List[int]:
    return [int(x) for x in s.split(",") if x.strip()]


def add_subparsers(sub) -> None:
    """Attach the HiCache subcommands (latency/throughput/saturation/suite)."""
    p_lat = sub.add_parser("latency", help="single in-flight op latency baseline")
    _common_args(p_lat)
    p_lat.set_defaults(_handler=run)

    p_thr = sub.add_parser("throughput", help="fixed concurrency throughput")
    _common_args(p_thr)
    p_thr.add_argument("--concurrency", type=int, default=8)
    p_thr.add_argument("--op", default="get", choices=["get", "set", "exists"])
    p_thr.set_defaults(_handler=run)

    p_sat = sub.add_parser("saturation", help="concurrency sweep -> peak throughput")
    _common_args(p_sat)
    p_sat.add_argument("--concurrencies", default="1,2,4,8,16,32")
    p_sat.add_argument("--op", default="get", choices=["get", "set", "exists"])
    p_sat.set_defaults(_handler=run)

    p_suite = sub.add_parser("suite", help="full systematic baseline")
    _common_args(p_suite)
    p_suite.add_argument("--concurrencies", default="1,2,4,8,16,32")
    p_suite.set_defaults(_handler=run)


def run(args) -> None:
    mode = args.command
    report = BaselineReport()
    report.meta = {
        "mode": mode, "protocol": args.protocol,
        "device": args.device_name or "-", "layout": args.layout,
        "page_size": args.page_size, "batch_size": args.batch_size,
        "tokens_per_page": args.tokens_per_page,
    }
    op_fn = {"get": run_get, "set": run_set, "exists": run_exists}

    if mode == "latency":
        args.batch_size = 1
        for fn in (run_get, run_set, run_exists):
            for r in fn(args, [1]):
                report.add(r)
    elif mode == "throughput":
        for r in op_fn[args.op](args, [args.concurrency]):
            report.add(r)
    elif mode == "saturation":
        rows = op_fn[args.op](args, _csv_ints(args.concurrencies))
        _peak_note(rows)
        for r in rows:
            report.add(r)
    elif mode == "suite":
        concs = _csv_ints(args.concurrencies)
        lat_args = argparse.Namespace(**vars(args))
        lat_args.batch_size = 1
        for fn in (run_get, run_set, run_exists):
            for r in fn(lat_args, [1]):
                r.note = (r.note + " [latency-baseline]").strip()
                report.add(r)
        g = run_get(args, concs); _peak_note(g)
        for r in g:
            report.add(r)
        s = run_set(args, concs); _peak_note(s)
        for r in s:
            report.add(r)
        e = run_exists(args, concs); _peak_note(e)
        for r in e:
            report.add(r)

    print(render_console(report, hicache=True))
    ts = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
    suffix = f"-{args.tag}" if args.tag else ""
    _write(report, args.out_dir, f"hicache-{mode}-{args.protocol}{suffix}-{ts}")


def main() -> None:
    ap = argparse.ArgumentParser(description="PeerCache HiCache benchmark")
    sub = ap.add_subparsers(dest="command", required=True)
    add_subparsers(sub)
    args = ap.parse_args()
    args._handler(args)


if __name__ == "__main__":
    main()
