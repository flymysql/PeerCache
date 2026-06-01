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
import multiprocessing as mp
import os
import signal
import time
from types import SimpleNamespace
from typing import List, Tuple

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
        "device_names": getattr(args, "devices", "") or "",
        "node_id": node_id,
        "heartbeat_interval": 0.5,
        # Prune departed readers quickly so a re-run isn't polluted by a stale
        # node still owning directory shards.
        "member_ttl": 5.0,
        "global_segment_size": seg_bytes,
        "metrics_enabled": False,
        "disk_enabled": False,
        "ib_port": args.ib_port,
        "gid_index": args.gid_index,
        "max_channels_per_peer": getattr(args, "max_channels", 16),
        "directory_read_cache_ttl": getattr(args, "dir_cache_ttl", 0.0),
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
    readers = max(1, int(getattr(args, "readers", 1)))
    print(f"[serve] waiting for {readers} reader(s) to join the ring ...", flush=True)
    _wait_ring(store, 1 + readers, args.connect_timeout)

    pool = HostKVPool(args.page_size, ws, layout=layout)
    store.register_mem_pool_host(pool)
    for i in range(ws):
        pool.fill_slot(i, i)
    keys = [f"{KEY_PREFIX}/{i}" for i in range(ws)]

    def _publish_all():
        for lo in range(0, ws, 256):
            chunk = list(range(lo, min(lo + 256, ws)))
            store.batch_set_v1([keys[i] for i in chunk], chunk)

    def _members():
        # Key on (node_id, control_endpoint): a re-run reuses the same node_id
        # but a fresh process (new control port) with an empty directory shard,
        # so we must re-publish to repopulate that shard.
        try:
            return frozenset(
                (nid, ni.control_endpoint())
                for nid, ni in store.runtime.discovery.members().items()
            )
        except Exception:
            return frozenset()

    _publish_all()
    print(
        f"[serve] published {ws} pages ({human_bytes(ws * pt)}); "
        f"readers may now run `drive`. Ctrl-C to stop.",
        flush=True,
    )

    # The directory is consistent-hash sharded across the live ring, so when the
    # membership changes (a reader joins or a departed one is pruned) some keys
    # change owner. Re-publish on every change to re-shard the directory for the
    # current members; this makes back-to-back `drive` runs against a long-lived
    # `serve` work without a restart.
    last_members = _members()
    stop = {"flag": False}

    def _handler(*_a):
        stop["flag"] = True

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)
    while not stop["flag"]:
        time.sleep(0.5)
        cur = _members()
        if cur != last_members:
            last_members = cur
            _publish_all()
            print(f"[serve] membership changed -> re-published for "
                  f"{len(cur)} node(s).", flush=True)
    print("[serve] shutting down.", flush=True)
    store.close()


# --------------------------------------------------------------------------- #
# drive: consumer / driver node (the actual benchmark)
# --------------------------------------------------------------------------- #
# One sweep point: (conc, ops, bytes, pages, hits, calls, Histogram, elapsed).
SweepPoint = Tuple[int, int, int, int, int, int, Histogram, float]


def _make_worker(store, args, keys):
    """Build the per-thread worker closure for the configured op."""
    bs = args.batch_size
    ws = args.working_set
    pt = _page_total(args.page_size, args.layout)
    comps = 1 if args.layout == "mla" else 2

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
    return worker


def _sweep(store, args, keys, concs) -> List[SweepPoint]:
    points: List[SweepPoint] = []
    for conc in concs:
        worker = _make_worker(store, args, keys)
        ops, by, pages, hits, calls, hist, elapsed = _drive(
            conc, worker, args.warmup, args.duration
        )
        points.append((conc, ops, by, pages, hits, calls, hist, elapsed))
    return points


def _hist_to_wire(h: Histogram):
    return (dict(h.counts), h.n, h._sum_ns, h._max_ns, h._min_ns)


def _hist_from_wire(w) -> Histogram:
    h = Histogram()
    counts, n, sum_ns, max_ns, min_ns = w
    h.counts = counts
    h.n = n
    h._sum_ns = sum_ns
    h._max_ns = max_ns
    h._min_ns = min_ns
    return h


def _rows_from_points(args, points: List[SweepPoint]) -> list:
    bs = args.batch_size
    pt = _page_total(args.page_size, args.layout)
    rows = []
    for (conc, ops, by, pages, hits, calls, hist, elapsed) in points:
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
            note=(f"dist batch_{args.op} {args.layout} ws={args.working_set} "
                  f"remote={args.discovery_addr}"
                  + (f" x{args.processes}proc" if args.processes > 1 else "")),
        ))
    _peak_note(rows)
    return rows


def _setup_consumer(args, node_id: str, expect_nodes: int):
    """Bring up a consumer store, join the ring, register a pool, wait for data."""
    pt = _page_total(args.page_size, args.layout)
    consumer_slots = max(_csv_ints(args.concurrencies)) * args.batch_size
    # The consumer never publishes (it only READs into its recv pool), so its
    # backend published pool is unused -- size it to the read slots only, NOT to
    # the working set. Otherwise each reader process would allocate
    # working_set*page_size bytes of idle host memory, which OOMs at scale with
    # large pages (e.g. 1 MiB pages x big working set x many processes).
    seg_bytes = args.global_segment_size or int(max(consumer_slots, 64) * pt * 1.3)
    store = _make_store(args, node_id, seg_bytes)
    device = "cuda" if getattr(args, "gpu", False) else "cpu"
    print(f"[drive] {node_id} up: rdma={store.runtime.local_rdma_endpoint} "
          f"discovery={args.discovery_addr} op={args.op} recv={device}", flush=True)
    _wait_ring(store, expect_nodes, args.connect_timeout)
    # GPUDirect: with --gpu the read destination (recv MR) is GPU memory, so
    # pages land straight in VRAM (needs nvidia-peermem / a dmabuf-capable stack).
    pool = HostKVPool(args.page_size, consumer_slots, layout=args.layout, device=device)
    store.register_mem_pool_host(pool)
    keys = [f"{KEY_PREFIX}/{i}" for i in range(args.working_set)]
    print(f"[drive] {node_id} waiting for the producer's working set ...", flush=True)
    _wait_published(store, keys, args.connect_timeout)
    return store, keys


def _merge_points(per_proc: List[List[SweepPoint]]) -> List[SweepPoint]:
    """Sum throughput counters and merge histograms across processes per conc."""
    merged: List[SweepPoint] = []
    n_points = len(per_proc[0])
    for i in range(n_points):
        conc = per_proc[0][i][0]
        ops = by = pages = hits = calls = 0
        hist = Histogram()
        elapsed = 0.0
        total_threads = 0
        for proc in per_proc:
            c, o, b, pg, h, cl, hi, el = proc[i]
            ops += o; by += b; pages += pg; hits += h; calls += cl
            hist.merge(hi)
            elapsed = max(elapsed, el)
            total_threads += c
        merged.append((total_threads, ops, by, pages, hits, calls, hist, elapsed))
    return merged


def _drive_child(args, proc_idx: int, total: int, q) -> None:
    try:
        node_id = f"{args.node_id or 'pcbench-consumer'}-{proc_idx}"
        store, keys = _setup_consumer(args, node_id, 1 + total)
        points = _sweep(store, args, keys, _csv_ints(args.concurrencies))
        wire = [(c, o, b, pg, h, cl, _hist_to_wire(hi), el)
                for (c, o, b, pg, h, cl, hi, el) in points]
        q.put((proc_idx, wire))
        store.close()
    except BaseException as e:  # report the failure rather than hanging the parent
        q.put((proc_idx, {"error": repr(e)}))


def run_drive(args) -> None:
    procs = max(1, int(getattr(args, "processes", 1)))
    report = BaselineReport()
    report.meta = {
        "mode": "dist", "role": "drive", "op": args.op,
        "protocol": args.protocol, "device": args.device_name or "-",
        "layout": args.layout, "page_size": args.page_size,
        "batch_size": args.batch_size, "tokens_per_page": args.tokens_per_page,
        "working_set": args.working_set, "processes": procs,
        "remote": args.discovery_addr,
    }

    if procs == 1:
        store, keys = _setup_consumer(args, args.node_id or "pcbench-consumer", 2)
        print("[drive] producer ready; starting sweep.", flush=True)
        points = _sweep(store, args, keys, _csv_ints(args.concurrencies))
        rows = _rows_from_points(args, points)
        store.close()
    else:
        # Escape the GIL: each process is its own consumer node and runs the
        # full sweep in parallel; results are summed per concurrency point. The
        # producer must be started with `serve --readers {procs}` so it only
        # publishes once all readers have joined the ring (stable sharding).
        print(f"[drive] launching {procs} reader processes "
              f"(ensure `serve --readers {procs}`) ...", flush=True)
        ctx = mp.get_context("spawn")
        q = ctx.Queue()
        children = [ctx.Process(target=_drive_child, args=(args, i, procs, q))
                    for i in range(procs)]
        for c in children:
            c.start()
        collected = {}
        for _ in range(procs):
            idx, payload = q.get()
            if isinstance(payload, dict) and "error" in payload:
                for c in children:
                    c.terminate()
                raise SystemExit(f"[drive] reader process {idx} failed: {payload['error']}")
            collected[idx] = [(c, o, b, pg, h, cl, _hist_from_wire(hw), el)
                              for (c, o, b, pg, h, cl, hw, el) in payload]
        for c in children:
            c.join()
        per_proc = [collected[i] for i in sorted(collected)]
        rows = _rows_from_points(args, _merge_points(per_proc))

    for r in rows:
        report.add(r)
    print(render_console(report, hicache=True))
    ts = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
    suffix = f"-{args.tag}" if args.tag else ""
    _write(report, args.out_dir, f"hicache-dist-{args.op}-{args.protocol}{suffix}-{ts}")


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
    p.add_argument("--devices", default="",
                   help="multi-rail: comma-separated device list, e.g. "
                        "mlx5_bond_1,...,mlx5_bond_8. One process opens a rail "
                        "per device and stripes READs across all of them. Must "
                        "match in count/order on serve and drive. Overrides "
                        "--device-name.")
    p.add_argument("--ib-port", type=int, default=1)
    p.add_argument("--gid-index", type=int, default=3)
    p.add_argument("--layout", default="mla", choices=["mla", "mha"])
    p.add_argument("--page-size", type=int, default=131072)
    p.add_argument("--tokens-per-page", type=int, default=64)
    p.add_argument("--working-set", type=int, default=4096,
                   help="distinct pages the producer publishes (must match on both)")
    p.add_argument("--global-segment-size", type=int, default=0,
                   help="published-pool bytes (0 = auto-size to the working set)")
    p.add_argument("--max-channels", type=int, default=16,
                   help="RC QP channels pooled per peer (raise for high concurrency)")
    p.add_argument("--node-id", default="")
    p.add_argument("--connect-timeout", type=float, default=60.0)


def add_subparsers(sub) -> None:
    p_serve = sub.add_parser(
        "serve", help="producer/data node: publish a working set and serve reads")
    _common(p_serve)
    p_serve.add_argument("--readers", type=int, default=1,
                         help="publish only after this many reader nodes join "
                              "(match the drive --processes count)")
    p_serve.set_defaults(_handler=run_serve)

    p_drive = sub.add_parser(
        "drive", help="consumer/driver: run a cross-host concurrency sweep")
    _common(p_drive)
    p_drive.add_argument("--op", default="get", choices=["get", "exists"])
    p_drive.add_argument("--gpu", action="store_true",
                         help="GPUDirect: allocate the read destination (recv MR) "
                              "in GPU memory so pages land in VRAM (needs torch+CUDA "
                              "and nvidia-peermem / a dmabuf-capable RDMA stack)")
    p_drive.add_argument("--batch-size", type=int, default=32)
    p_drive.add_argument("--concurrencies", default="1,2,4,8,16,32,64")
    p_drive.add_argument("--duration", type=float, default=10.0)
    p_drive.add_argument("--warmup", type=float, default=2.0)
    p_drive.add_argument("--processes", type=int, default=1,
                         help="reader processes to run in parallel (escapes the "
                              "GIL; start `serve --readers N` to match)")
    p_drive.add_argument("--dir-cache-ttl", type=float, default=0.0,
                         help="cache resident read locations for N seconds to "
                              "skip the per-batch directory RPC (0 = off)")
    p_drive.add_argument("--out-dir",
                         default=os.path.join(os.getcwd(), "peercache-bench-results"))
    p_drive.add_argument("--tag", default="")
    p_drive.set_defaults(_handler=run_drive)
