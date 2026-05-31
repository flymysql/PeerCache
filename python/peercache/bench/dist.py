"""Two-host (distributed) PeerCache benchmark.

The in-process ``suite``/``micro`` benchmarks bring up *both* nodes in one
process over RDMA loopback (``127.0.0.1``). That is software-bound (GIL +
control-plane RPC) and unrepresentative of real PD-disaggregated inference. This
module splits the two roles into two processes on two machines so the GET path
exercises a genuine cross-host one-sided RDMA READ over the fabric:

    prefill / data node  (serve)  --batch_set_v1--> publish KV pages, then idle
    decode / driver node (drive)  --batch_get_v1--> read pages over RDMA + sweep

Host A -- producer / data node (publishes a working set, then serves reads):

    peercache-bench serve \
        --discovery-addr 0.0.0.0:31998 \
        --device-name mlx5_bond_1 --gid-index <N> \
        --layout mla --page-size 131072 --working-set 4096

Host B -- consumer / driver (runs the concurrency sweep against host A):

    peercache-bench drive \
        --discovery-addr <A_IP>:31998 \
        --device-name mlx5_bond_1 --gid-index <N> \
        --layout mla --page-size 131072 --working-set 4096 \
        --batch-size 32 --concurrencies 1,2,4,8,16,32,64 \
        --duration 10 --warmup 2 --op get --tag rdma

Notes
-----
* ``--local-host`` is optional: when omitted each node advertises the local IP
  that routes to ``--discovery-addr`` (so peers can reach its control + RDMA
  endpoints). Set it explicitly to pin a specific NIC/interface.
* The producer publishes only *after* the consumer has joined the ring, so the
  sharded directory is built against the final 2-node membership (entries are
  not migrated on a late join).
* ``serve`` keeps the published pages resident and idles until Ctrl-C; run it
  first and leave it running while you launch one or more ``drive`` clients.
"""

from __future__ import annotations

import argparse
import os
import signal
import time
from types import SimpleNamespace
from typing import List

from peercache.bench.common import (
    BaselineReport,
    Histogram,
    Workload,
    human_bytes,
    make_result,
    render_console,
)
from peercache.bench.hicache import _csv_ints, _drive, _peak_note, _write
from peercache.bench.sglang_sim import HostKVPool

# Producer and consumer must agree on this key namespace and the working set.
KEY_PREFIX = "pcbench/dist/get"


def _page_total(page_size: int, layout: str) -> int:
    return page_size * (1 if layout == "mla" else 2)


def _make_store(args, node_id: str, seg_bytes: int):
    from peercache.store import PeerCacheStore

    extra = {
        "discovery_addr": args.discovery_addr,
        "protocol": args.protocol,
        "device_name": args.device_name,
        "node_id": node_id,
        "heartbeat_interval": 0.5,
        "member_ttl": 30.0,
        "global_segment_size": seg_bytes,
        "metrics_enabled": False,
        "disk_enabled": False,
        "ib_port": args.ib_port,
        "gid_index": args.gid_index,
    }
    if getattr(args, "local_host", ""):
        extra["local_hostname"] = args.local_host
    cfg = SimpleNamespace(
        tp_rank=0, tp_size=1, pp_rank=0, pp_size=1,
        is_mla_model=(args.layout == "mla"),
        extra_config=extra,
    )
    return PeerCacheStore(cfg)


def _wait_ring(store, n: int, timeout: float) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if len(store.runtime.ring) >= n:
            return
        time.sleep(0.2)
    raise SystemExit(
        f"[dist] timed out after {timeout:.0f}s waiting for {n} nodes in the ring "
        f"(have {len(store.runtime.ring)}). Check that --discovery-addr is reachable "
        f"from this host and that the control port is not firewalled."
    )


def _wait_published(store, keys: List[str], timeout: float) -> None:
    """Block until the producer's working set is visible in the directory."""
    deadline = time.time() + timeout
    probe = [keys[0], keys[len(keys) // 2], keys[-1]]
    while time.time() < deadline:
        if store.batch_exists(probe) == len(probe):
            return
        time.sleep(0.2)
    raise SystemExit(
        f"[dist] timed out after {timeout:.0f}s waiting for the producer to publish "
        f"{len(keys)} pages under '{KEY_PREFIX}/*'. Is `serve` running with the same "
        f"--working-set / --layout / --page-size?"
    )


# --------------------------------------------------------------------------- #
# serve: producer / data node
# --------------------------------------------------------------------------- #
def run_serve(args) -> None:
    layout = args.layout
    pt = _page_total(args.page_size, layout)
    ws = args.working_set
    seg_bytes = args.global_segment_size or int(ws * pt * 1.3)

    store = _make_store(args, args.node_id or "pcbench-producer", seg_bytes)
    print(
        f"[serve] node up: rdma={store.runtime.local_rdma_endpoint} "
        f"discovery={args.discovery_addr} layout={layout} "
        f"page={human_bytes(args.page_size)} working_set={ws}",
        flush=True,
    )
    print("[serve] waiting for a reader to join the ring ...", flush=True)
    _wait_ring(store, 2, args.connect_timeout)

    pool = HostKVPool(args.page_size, ws, layout=layout)
    store.register_mem_pool_host(pool)
    for i in range(ws):
        pool.fill_slot(i, i)
    keys = [f"{KEY_PREFIX}/{i}" for i in range(ws)]
    for lo in range(0, ws, 256):
        chunk = list(range(lo, min(lo + 256, ws)))
        store.batch_set_v1([keys[i] for i in chunk], chunk)
    print(
        f"[serve] published {ws} pages ({human_bytes(ws * pt)}); "
        f"readers may now run `drive`. Ctrl-C to stop.",
        flush=True,
    )

    stop = {"flag": False}

    def _handler(*_a):
        stop["flag"] = True

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)
    while not stop["flag"]:
        time.sleep(0.5)
    print("[serve] shutting down.", flush=True)
    store.close()


# --------------------------------------------------------------------------- #
# drive: consumer / driver node (the actual benchmark)
# --------------------------------------------------------------------------- #
def run_drive(args) -> None:
    layout = args.layout
    pt = _page_total(args.page_size, layout)
    comps = 1 if layout == "mla" else 2
    bs = args.batch_size
    ws = args.working_set
    concs = _csv_ints(args.concurrencies)
    max_conc = max(concs)
    consumer_slots = max_conc * bs
    seg_bytes = args.global_segment_size or int(max(ws, consumer_slots) * pt * 1.3)

    store = _make_store(args, args.node_id or "pcbench-consumer", seg_bytes)
    print(
        f"[drive] node up: rdma={store.runtime.local_rdma_endpoint} "
        f"discovery={args.discovery_addr} op={args.op}",
        flush=True,
    )
    _wait_ring(store, 2, args.connect_timeout)

    pool = HostKVPool(args.page_size, consumer_slots, layout=layout)
    store.register_mem_pool_host(pool)
    keys = [f"{KEY_PREFIX}/{i}" for i in range(ws)]
    print("[drive] waiting for the producer's working set ...", flush=True)
    _wait_published(store, keys, args.connect_timeout)
    print("[drive] producer ready; starting sweep.", flush=True)

    report = BaselineReport()
    report.meta = {
        "mode": "dist", "role": "drive", "op": args.op,
        "protocol": args.protocol, "device": args.device_name or "-",
        "layout": layout, "page_size": args.page_size, "batch_size": bs,
        "tokens_per_page": args.tokens_per_page, "working_set": ws,
        "remote": args.discovery_addr,
    }

    rows = []
    for conc in concs:
        if args.op == "get":
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
                    kstart = (tid * 7919 + r * bs) % ws
                    kk = [keys[(kstart + j) % ws] for j in range(bs)]
                    t0 = time.perf_counter()
                    oks = store.batch_get_v1(kk, idxs)
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
        else:  # exists
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
                    kstart = (tid * 7919 + r * bs) % ws
                    kk = [keys[(kstart + j) % ws] for j in range(bs)]
                    t0 = time.perf_counter()
                    n = store.batch_exists(kk)
                    t1 = time.perf_counter()
                    r += 1
                    if measuring:
                        calls += 1
                        hist.record(t1 - t0)
                        hits += n
                        pages += bs
                return 0, 0, pages, hits, calls, hist

        ops, by, pages, hits, calls, hist, elapsed = _drive(
            conc, worker, args.warmup, args.duration
        )
        wl = Workload(block_size=args.page_size, batch_size=bs, threads=conc,
                      duration=args.duration,
                      operation="read" if args.op == "get" else "exists")
        expected = calls * bs
        hit_rate = (hits / expected) if expected else float("nan")
        rows.append(make_result(
            "peercache", f"hicache-{args.op}", args.protocol, wl,
            ops if args.op == "get" else pages, by, elapsed,
            hist=hist, op=args.op, pages=pages,
            tokens_per_page=args.tokens_per_page,
            hit_rate=hit_rate if args.op == "get" else float("nan"),
            note=f"dist batch_{args.op} {layout} ws={ws} remote={args.discovery_addr}",
        ))

    _peak_note(rows)
    for r in rows:
        report.add(r)

    print(render_console(report, hicache=True))
    ts = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
    suffix = f"-{args.tag}" if args.tag else ""
    _write(report, args.out_dir, f"hicache-dist-{args.op}-{args.protocol}{suffix}-{ts}")
    store.close()


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def _common(p: argparse.ArgumentParser) -> None:
    p.add_argument("--discovery-addr", required=True,
                   help="meta/discovery 'host:port'. On the producer use a local "
                        "bind (e.g. 0.0.0.0:31998); on the consumer use the "
                        "producer's IP:port.")
    p.add_argument("--local-host", default="",
                   help="IP this node advertises to peers (default: auto-detect "
                        "the NIC that routes to --discovery-addr)")
    p.add_argument("--protocol", default="rdma", choices=["rdma", "tcp"])
    p.add_argument("--device-name", default="", help="RDMA device, e.g. mlx5_bond_1")
    p.add_argument("--ib-port", type=int, default=1)
    p.add_argument("--gid-index", type=int, default=3)
    p.add_argument("--layout", default="mla", choices=["mla", "mha"])
    p.add_argument("--page-size", type=int, default=131072)
    p.add_argument("--tokens-per-page", type=int, default=64)
    p.add_argument("--working-set", type=int, default=4096,
                   help="distinct pages the producer publishes (must match on both)")
    p.add_argument("--global-segment-size", type=int, default=0,
                   help="published-pool bytes (0 = auto-size to the working set)")
    p.add_argument("--node-id", default="")
    p.add_argument("--connect-timeout", type=float, default=60.0)


def add_subparsers(sub) -> None:
    p_serve = sub.add_parser(
        "serve", help="producer/data node: publish a working set and serve reads")
    _common(p_serve)
    p_serve.set_defaults(_handler=run_serve)

    p_drive = sub.add_parser(
        "drive", help="consumer/driver: run a cross-host concurrency sweep")
    _common(p_drive)
    p_drive.add_argument("--op", default="get", choices=["get", "exists"])
    p_drive.add_argument("--batch-size", type=int, default=32)
    p_drive.add_argument("--concurrencies", default="1,2,4,8,16,32,64")
    p_drive.add_argument("--duration", type=float, default=10.0)
    p_drive.add_argument("--warmup", type=float, default=2.0)
    p_drive.add_argument("--out-dir",
                         default=os.path.join(os.getcwd(), "peercache-bench-results"))
    p_drive.add_argument("--tag", default="")
    p_drive.set_defaults(_handler=run_drive)
