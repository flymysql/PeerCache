"""Shared utilities for the PeerCache benchmark harness.

Provides workload configuration, a memory-bounded latency histogram with
percentile reporting (p50/p95/p99/p999/max), a uniform result schema
(JSON-serialisable), and Markdown/console renderers.

The harness is RDMA-first: the headline numbers are produced with
``--protocol rdma`` on real RDMA hardware. A pure-Python TCP fallback exists in
the transport for functional smoke testing on machines without a NIC, but TCP
runs are not a performance scenario and must not be quoted as RDMA numbers.
"""

from __future__ import annotations

import json
import math
import os
import platform
import time
from dataclasses import asdict, dataclass, field
from typing import Dict, List, Optional


# --------------------------------------------------------------------------- #
# Helpers (defined first so dataclass factories can use them)
# --------------------------------------------------------------------------- #
def host_info() -> Dict[str, object]:
    return {
        "platform": platform.platform(),
        "python": platform.python_version(),
        "processor": platform.processor() or platform.machine(),
        "cpu_count": os.cpu_count(),
    }


def human_bytes(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            if unit == "B":
                return f"{int(n)}{unit}"
            return f"{n:.0f}{unit}" if float(n).is_integer() else f"{n:.1f}{unit}"
        n /= 1024
    return f"{n}B"


def human_count(n: float) -> str:
    for unit, div in (("", 1), ("K", 1e3), ("M", 1e6), ("G", 1e9)):
        if n < 1000 * div or unit == "G":
            v = n / div
            return f"{v:.0f}{unit}" if float(v).is_integer() else f"{v:.2f}{unit}"
    return str(n)


# --------------------------------------------------------------------------- #
# Workload
# --------------------------------------------------------------------------- #
@dataclass
class Workload:
    """A single comparable workload point.

    block_size : bytes per individual read/transfer (one KV component object).
    batch_size : objects submitted together per batch call.
    duration   : measurement window in seconds (steady state).
    warmup     : seconds discarded before measurement starts.
    threads    : concurrent submitting threads (the "thread model").
    operation  : "read" / "write" / "exists".
    """

    block_size: int = 64 * 1024
    batch_size: int = 64
    duration: float = 5.0
    warmup: float = 1.0
    threads: int = 1
    operation: str = "read"

    def label(self) -> str:
        return f"{human_bytes(self.block_size)}x{self.batch_size} t{self.threads}"


# --------------------------------------------------------------------------- #
# Memory-bounded latency histogram (HDR-style, ~0.1% relative precision)
# --------------------------------------------------------------------------- #
class Histogram:
    """Records latencies (seconds) into significant-figure buckets.

    Bounded memory regardless of sample count, so it is safe under full-load
    runs producing tens of millions of operations. Percentiles are accurate to
    the bucket width (``sig`` significant figures, default 3 -> ~0.1%).
    """

    __slots__ = ("sig", "counts", "n", "_sum_ns", "_max_ns", "_min_ns")

    def __init__(self, sig: int = 3) -> None:
        self.sig = sig
        self.counts: Dict[int, int] = {}
        self.n = 0
        self._sum_ns = 0
        self._max_ns = 0
        self._min_ns = 0

    def _bucket(self, ns: int) -> int:
        if ns <= 0:
            return 0
        digits = self.sig - 1 - int(math.floor(math.log10(ns)))
        if digits >= 0:
            return ns
        f = 10 ** (-digits)
        return (ns // f) * f

    def record(self, seconds: float) -> None:
        ns = int(seconds * 1e9)
        if ns < 1:
            ns = 1
        b = self._bucket(ns)
        self.counts[b] = self.counts.get(b, 0) + 1
        self.n += 1
        self._sum_ns += ns
        if ns > self._max_ns:
            self._max_ns = ns
        if self._min_ns == 0 or ns < self._min_ns:
            self._min_ns = ns

    def merge(self, other: "Histogram") -> None:
        for k, v in other.counts.items():
            self.counts[k] = self.counts.get(k, 0) + v
        self.n += other.n
        self._sum_ns += other._sum_ns
        self._max_ns = max(self._max_ns, other._max_ns)
        if other._min_ns and (self._min_ns == 0 or other._min_ns < self._min_ns):
            self._min_ns = other._min_ns

    def __len__(self) -> int:
        return self.n

    def percentile_us(self, p: float) -> float:
        if self.n == 0:
            return float("nan")
        target = p / 100.0 * self.n
        acc = 0
        for k in sorted(self.counts):
            acc += self.counts[k]
            if acc >= target:
                return k / 1000.0
        return self._max_ns / 1000.0

    def mean_us(self) -> float:
        return (self._sum_ns / self.n) / 1000.0 if self.n else float("nan")

    def max_us(self) -> float:
        return self._max_ns / 1000.0 if self.n else float("nan")

    def min_us(self) -> float:
        return self._min_ns / 1000.0 if self.n else float("nan")


# --------------------------------------------------------------------------- #
# Result schema
# --------------------------------------------------------------------------- #
@dataclass
class BenchResult:
    system: str                 # "peercache" | "mooncake"
    path: str                   # "transport-read" | "hicache-get" | "hicache-set" | ...
    protocol: str               # "rdma" | "tcp"
    block_size: int
    batch_size: int
    threads: int
    duration_s: float
    ops: int                    # total individual component reads/writes completed
    bytes_total: int
    throughput_gbps: float      # GB/s (10^9 bytes/s)
    ops_per_s: float
    lat_us_mean: float = float("nan")
    lat_us_p50: float = float("nan")
    lat_us_p90: float = float("nan")
    lat_us_p95: float = float("nan")
    lat_us_p99: float = float("nan")
    lat_us_p999: float = float("nan")
    lat_us_max: float = float("nan")
    # HiCache-oriented extras (optional).
    op: str = ""                # set | get | exists | mixed
    pages: int = 0              # logical KV pages processed (a page = N components)
    pages_per_s: float = 0.0
    tokens_per_s: float = 0.0
    hit_rate: float = float("nan")
    note: str = ""
    ok: bool = True

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class BaselineReport:
    """Top-level container written to JSON: env + all results."""

    created_at: str = field(
        default_factory=lambda: time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    )
    host: Dict[str, object] = field(default_factory=host_info)
    meta: Dict[str, object] = field(default_factory=dict)
    results: List[BenchResult] = field(default_factory=list)

    def add(self, r: BenchResult) -> None:
        self.results.append(r)

    def to_json(self) -> str:
        return json.dumps(
            {
                "created_at": self.created_at,
                "host": self.host,
                "meta": self.meta,
                "results": [r.to_dict() for r in self.results],
            },
            indent=2,
        )


# --------------------------------------------------------------------------- #
# Result construction
# --------------------------------------------------------------------------- #
def make_result(
    system: str,
    path: str,
    protocol: str,
    wl: Workload,
    ops: int,
    bytes_total: int,
    elapsed_s: float,
    hist: Optional[Histogram] = None,
    note: str = "",
    ok: bool = True,
    op: str = "",
    pages: int = 0,
    tokens_per_page: int = 0,
    hit_rate: float = float("nan"),
) -> BenchResult:
    thr = (bytes_total / 1e9) / elapsed_s if elapsed_s > 0 else 0.0
    ops_s = ops / elapsed_s if elapsed_s > 0 else 0.0
    pages_s = pages / elapsed_s if elapsed_s > 0 else 0.0
    r = BenchResult(
        system=system,
        path=path,
        protocol=protocol,
        block_size=wl.block_size,
        batch_size=wl.batch_size,
        threads=wl.threads,
        duration_s=round(elapsed_s, 3),
        ops=ops,
        bytes_total=bytes_total,
        throughput_gbps=round(thr, 4),
        ops_per_s=round(ops_s, 1),
        op=op,
        pages=pages,
        pages_per_s=round(pages_s, 1),
        tokens_per_s=round(pages_s * tokens_per_page, 1),
        hit_rate=hit_rate,
        note=note,
        ok=ok,
    )
    if hist is not None and len(hist) > 0:
        r.lat_us_mean = round(hist.mean_us(), 2)
        r.lat_us_p50 = round(hist.percentile_us(50), 2)
        r.lat_us_p90 = round(hist.percentile_us(90), 2)
        r.lat_us_p95 = round(hist.percentile_us(95), 2)
        r.lat_us_p99 = round(hist.percentile_us(99), 2)
        r.lat_us_p999 = round(hist.percentile_us(99.9), 2)
        r.lat_us_max = round(hist.max_us(), 2)
    return r


# --------------------------------------------------------------------------- #
# Rendering
# --------------------------------------------------------------------------- #
def _fmt(v: float, nd: int = 1) -> str:
    return "-" if (v is None or (isinstance(v, float) and math.isnan(v))) else f"{v:.{nd}f}"


def render_markdown(report: BaselineReport) -> str:
    """Compact transport-style table (system comparison)."""
    lines = [
        "| system | path | proto | block | batch | threads | throughput (GB/s) | ops/s | p50 (us) | p99 (us) | note |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in report.results:
        thr_field = "n/a" if not r.ok else f"**{r.throughput_gbps:.3f}**"
        ops_field = "n/a" if not r.ok else f"{r.ops_per_s:,.0f}"
        lines.append(
            f"| {r.system} | {r.path} | {r.protocol} | {human_bytes(r.block_size)} | "
            f"{r.batch_size} | {r.threads} | {thr_field} | {ops_field} | "
            f"{_fmt(r.lat_us_p50)} | {_fmt(r.lat_us_p99)} | {r.note} |"
        )
    return "\n".join(lines)


def render_hicache_markdown(report: BaselineReport) -> str:
    """Rich table for the HiCache simulation: thread model, throughput, tail."""
    lines = [
        "| op | proto | page | batch | threads | pages/s | tokens/s | GB/s | "
        "p50 µs | p95 µs | p99 µs | p999 µs | max µs | hit% | note |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in report.results:
        if not r.ok:
            lines.append(
                f"| {r.op or r.path} | {r.protocol} | {human_bytes(r.block_size)} | "
                f"{r.batch_size} | {r.threads} | n/a | n/a | n/a | - | - | - | - | - | - | {r.note} |"
            )
            continue
        hit = "-" if math.isnan(r.hit_rate) else f"{r.hit_rate * 100:.0f}"
        lines.append(
            f"| {r.op or r.path} | {r.protocol} | {human_bytes(r.block_size)} | "
            f"{r.batch_size} | {r.threads} | {human_count(r.pages_per_s)} | "
            f"{human_count(r.tokens_per_s)} | {r.throughput_gbps:.3f} | "
            f"{_fmt(r.lat_us_p50)} | {_fmt(r.lat_us_p95)} | {_fmt(r.lat_us_p99)} | "
            f"{_fmt(r.lat_us_p999)} | {_fmt(r.lat_us_max)} | {hit} | {r.note} |"
        )
    return "\n".join(lines)


def render_console(report: BaselineReport, hicache: bool = False) -> str:
    out = [
        f"PeerCache benchmark @ {report.created_at}",
        f"host: {report.host.get('platform')} | cpus={report.host.get('cpu_count')}",
    ]
    if report.meta:
        out.append("meta: " + ", ".join(f"{k}={v}" for k, v in report.meta.items()))
    out.append("")
    out.append(render_hicache_markdown(report) if hicache else render_markdown(report))
    return "\n".join(out)
