"""PeerCache data-plane microbenchmarks (transport + store path).

These are low-level microbenchmarks kept for completeness; the systematic,
SGLang-faithful benchmark lives in ``bench_hicache.py``. RDMA-first: pass
``--protocol rdma --device-name mlx5_0`` on real hardware. The TCP path exists
only for functional smoke testing and is not a performance scenario.

1. ``transport-read`` -- two ``Transport``s; batched one-sided READ.
2. ``store-get``      -- 2-node ``PeerCacheStore``: ``batch_set_v1`` then
   ``batch_get_v1``.
"""

from __future__ import annotations

import argparse
import ctypes
import time
from types import SimpleNamespace
from typing import List

from common import Histogram, Workload, make_result, render_console, BaselineReport

from peercache.config import PeerCacheConfig
from peercache.transport import ReadOp, create_transport


def _make_transport(protocol: str, device_name: str, port: int):
    cfg = PeerCacheConfig(
        discovery_addr="127.0.0.1:0",
        protocol=protocol,
        device_name=device_name,
        local_hostname="127.0.0.1",
        rdma_bind_host="127.0.0.1",
        rdma_port=port,
        metrics_enabled=False,
        disk_enabled=False,
    )
    return create_transport(cfg)


def bench_transport_read(wl: Workload, protocol: str, device_name: str = ""):
    target = _make_transport(protocol, device_name, 0)
    initiator = _make_transport(protocol, device_name, 0)

    buf_pages = max(wl.batch_size * 4, 256)
    buf_bytes = wl.block_size * buf_pages
    src = (ctypes.c_byte * buf_bytes)()
    dst = (ctypes.c_byte * buf_bytes)()
    src_addr = ctypes.addressof(src)
    dst_addr = ctypes.addressof(dst)
    for i in range(0, buf_bytes, 4096):
        src[i] = (i // 4096) % 251

    src_mr = target.register_mr(src_addr, buf_bytes)
    initiator.register_mr(dst_addr, buf_bytes)
    remote = target.local_endpoint()

    def build_batch(round_idx: int) -> List[ReadOp]:
        ops = []
        for j in range(wl.batch_size):
            slot = (round_idx * wl.batch_size + j) % buf_pages
            ops.append(ReadOp(
                remote_endpoint=remote,
                local_addr=dst_addr + slot * wl.block_size,
                remote_addr=src_addr + slot * wl.block_size,
                rkey=src_mr.rkey,
                length=wl.block_size,
            ))
        return ops

    warm_end = time.perf_counter() + wl.warmup
    r = 0
    while time.perf_counter() < warm_end:
        initiator.batch_read(build_batch(r)); r += 1

    hist = Histogram()
    ops_done = bytes_done = 0
    start = time.perf_counter(); end = start + wl.duration; r = 0
    while time.perf_counter() < end:
        batch = build_batch(r)
        t0 = time.perf_counter()
        oks = initiator.batch_read(batch)
        t1 = time.perf_counter()
        n_ok = sum(1 for o in oks if o)
        ops_done += n_ok; bytes_done += n_ok * wl.block_size
        if n_ok:
            hist.record((t1 - t0) / n_ok)
        r += 1
    elapsed = time.perf_counter() - start
    target.close(); initiator.close()
    return make_result("peercache", "transport-read", protocol, wl, ops_done,
                       bytes_done, elapsed, hist=hist, op="read",
                       note="one-sided READ, 2 in-process transports")


class _FakeKVBuffer:
    def __init__(self, nbytes):
        self._b = (ctypes.c_byte * nbytes)()

    def data_ptr(self):
        return ctypes.addressof(self._b)

    def numel(self):
        return len(self._b)

    def element_size(self):
        return 1


class _FakeMemPoolHost:
    def __init__(self, page_bytes, num_pages):
        self.page_bytes = page_bytes
        self.kv_buffer = _FakeKVBuffer(page_bytes * num_pages)

    def get_page_buffer_meta(self, host_indices):
        base = self.kv_buffer.data_ptr()
        ptrs = [base + i * self.page_bytes for i in host_indices]
        sizes = [self.page_bytes] * len(host_indices)
        return ptrs, sizes


def _store_cfg(discovery_addr, node_id, protocol, device_name, seg_bytes):
    return SimpleNamespace(
        tp_rank=0, tp_size=1, pp_rank=0, pp_size=1, is_mla_model=True,
        extra_config={
            "discovery_addr": discovery_addr, "protocol": protocol,
            "device_name": device_name, "local_hostname": "127.0.0.1",
            "node_id": node_id, "heartbeat_interval": 0.2, "member_ttl": 30.0,
            "global_segment_size": seg_bytes, "metrics_enabled": False,
            "disk_enabled": False,
        },
    )


def bench_store_get(wl: Workload, protocol: str, device_name: str = ""):
    from peercache.discovery import DiscoveryServer
    from peercache.store import PeerCacheStore

    page = wl.block_size
    npages = max(wl.batch_size * 8, 512)
    seg_bytes = max(page * npages * 2, 1 << 20)

    meta = DiscoveryServer("127.0.0.1", 0)
    port = meta.start()
    addr = f"127.0.0.1:{port}"
    a = PeerCacheStore(_store_cfg(addr, "A", protocol, device_name, seg_bytes))
    b = PeerCacheStore(_store_cfg(addr, "B", protocol, device_name, seg_bytes))

    deadline = time.time() + 10
    while time.time() < deadline and (len(a.runtime.ring) < 2 or len(b.runtime.ring) < 2):
        time.sleep(0.05)

    host_a = _FakeMemPoolHost(page, npages); a.register_mem_pool_host(host_a)
    host_b = _FakeMemPoolHost(page, npages); b.register_mem_pool_host(host_b)
    keys = [f"k{i}" for i in range(npages)]
    a.batch_set_v1(keys, list(range(npages)))

    def get_round(round_idx: int) -> int:
        lo = (round_idx * wl.batch_size) % npages
        idxs = [(lo + j) % npages for j in range(wl.batch_size)]
        return sum(1 for o in b.batch_get_v1([keys[i] for i in idxs], idxs) if o)

    warm_end = time.perf_counter() + wl.warmup
    r = 0
    while time.perf_counter() < warm_end:
        get_round(r); r += 1

    hist = Histogram()
    ops_done = bytes_done = 0
    start = time.perf_counter(); end = start + wl.duration; r = 0
    while time.perf_counter() < end:
        t0 = time.perf_counter()
        n_ok = get_round(r)
        t1 = time.perf_counter()
        ops_done += n_ok; bytes_done += n_ok * page
        if n_ok:
            hist.record((t1 - t0) / n_ok)
        r += 1
    elapsed = time.perf_counter() - start
    a.close(); b.close(); meta.stop()
    return make_result("peercache", "store-get", protocol, wl, ops_done,
                       bytes_done, elapsed, hist=hist, op="get",
                       note="batch_get_v1: directory GET + remote READ")


def main() -> None:
    ap = argparse.ArgumentParser(description="PeerCache data-plane microbench")
    ap.add_argument("--protocol", default="rdma", choices=["tcp", "rdma"])
    ap.add_argument("--device-name", default="", help="RDMA device (e.g. mlx5_0)")
    ap.add_argument("--block-size", type=int, default=65536)
    ap.add_argument("--batch-size", type=int, default=64)
    ap.add_argument("--threads", type=int, default=1)
    ap.add_argument("--duration", type=float, default=5.0)
    ap.add_argument("--warmup", type=float, default=1.0)
    ap.add_argument("--path", default="transport", choices=["transport", "store", "both"])
    args = ap.parse_args()

    wl = Workload(block_size=args.block_size, batch_size=args.batch_size,
                  threads=args.threads, duration=args.duration, warmup=args.warmup)
    report = BaselineReport()
    report.meta = {"protocol": args.protocol, "device": args.device_name or "-"}
    if args.path in ("transport", "both"):
        report.add(bench_transport_read(wl, args.protocol, args.device_name))
    if args.path in ("store", "both"):
        report.add(bench_store_get(wl, args.protocol, args.device_name))
    print(render_console(report, hicache=True))


if __name__ == "__main__":
    main()
