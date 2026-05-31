"""Shared utilities for the PeerCache benchmark harness.

Provides workload configuration, a latency reservoir with percentile reporting,
a uniform result schema (JSON-serialisable), and Markdown/console renderers so
PeerCache and Mooncake numbers can be reported side by side in one table.

The harness is hardware-agnostic: the *same* workload knobs (block size, batch
size, duration) drive PeerCache's data-plane transport, PeerCache's full store
path, and Mooncake's official ``transfer_engine_bench``. This is what makes the
numbers comparable -- see ``benchmarks/README.md`` for the methodology and the
important caveats about software-path (TCP) vs RDMA runs.
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


# --------------------------------------------------------------------------- #
# Workload
# --------------------------------------------------------------------------- #
@dataclass
class Workload:
    """A single comparable workload point.

    block_size : bytes per individual read/transfer (a KV "page").
    batch_size : reads submitted together per batch.
    duration   : measurement window in seconds (steady state).
    warmup     : seconds discarded before measurement starts.
    threads    : concurrent submitting threads (where a transport supports it).
    operation  : "read" only for now (matches one-sided RDMA READ on get()).
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
# Latency stats
# --------------------------------------------------------------------------- #
class Latencies:
    """Collects per-operation latencies (seconds) and reports percentiles."""

    def __init__(self) -> None:
        self._samples: List[float] = []

    def add(self, seconds: float) -> None:
        self._samples.append(seconds)

    def extend(self, seconds: List[float]) -> None:
        self._samples.extend(seconds)

    def __len__(self) -> int:
        return len(self._samples)

    def percentile(self, p: float) -> float:
        if not self._samples:
            return float("nan")
        xs = sorted(self._samples)
        if len(xs) == 1:
            return xs[0]
        k = (len(xs) - 1) * (p / 100.0)
        lo = math.floor(k)
        hi = math.ceil(k)
        if lo == hi:
            return xs[int(k)]
        return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)

    def mean(self) -> float:
        if not self._samples:
            return float("nan")
        return sum(self._samples) / len(self._samples)


# --------------------------------------------------------------------------- #
# Result schema
# --------------------------------------------------------------------------- #
@dataclass
class BenchResult:
    system: str                 # "peercache" | "mooncake"
    path: str                   # "transport-read" | "store-get" | "transfer-engine"
    protocol: str               # "tcp" | "rdma"
    block_size: int
    batch_size: int
    threads: int
    duration_s: float
    ops: int                    # total individual reads completed
    bytes_total: int
    throughput_gbps: float      # GB/s (10^9 bytes/s), matches Mooncake's default unit
    ops_per_s: float
    lat_us_mean: float = float("nan")
    lat_us_p50: float = float("nan")
    lat_us_p90: float = float("nan")
    lat_us_p99: float = float("nan")
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
    results: List[BenchResult] = field(default_factory=list)

    def add(self, r: BenchResult) -> None:
        self.results.append(r)

    def to_json(self) -> str:
        return json.dumps(
            {
                "created_at": self.created_at,
                "host": self.host,
                "results": [r.to_dict() for r in self.results],
            },
            indent=2,
        )


# --------------------------------------------------------------------------- #
# Result construction + rendering
# --------------------------------------------------------------------------- #
def make_result(
    system: str,
    path: str,
    protocol: str,
    wl: Workload,
    ops: int,
    bytes_total: int,
    elapsed_s: float,
    lat: Optional[Latencies] = None,
    note: str = "",
    ok: bool = True,
) -> BenchResult:
    thr = (bytes_total / 1e9) / elapsed_s if elapsed_s > 0 else 0.0
    ops_s = ops / elapsed_s if elapsed_s > 0 else 0.0
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
        note=note,
        ok=ok,
    )
    if lat is not None and len(lat) > 0:
        r.lat_us_mean = round(lat.mean() * 1e6, 2)
        r.lat_us_p50 = round(lat.percentile(50) * 1e6, 2)
        r.lat_us_p90 = round(lat.percentile(90) * 1e6, 2)
        r.lat_us_p99 = round(lat.percentile(99) * 1e6, 2)
    return r


def render_markdown(report: BaselineReport) -> str:
    lines: List[str] = []
    lines.append(
        "| system | path | proto | block | batch | threads | throughput (GB/s) | ops/s | p50 (us) | p99 (us) | note |"
    )
    lines.append("|---|---|---|---|---|---|---|---|---|---|---|")
    for r in report.results:
        thr_field = "n/a" if not r.ok else f"**{r.throughput_gbps:.3f}**"
        ops_field = "n/a" if not r.ok else f"{r.ops_per_s:,.0f}"
        p50 = "-" if math.isnan(r.lat_us_p50) else f"{r.lat_us_p50:.1f}"
        p99 = "-" if math.isnan(r.lat_us_p99) else f"{r.lat_us_p99:.1f}"
        lines.append(
            f"| {r.system} | {r.path} | {r.protocol} | {human_bytes(r.block_size)} | "
            f"{r.batch_size} | {r.threads} | {thr_field} | {ops_field} | {p50} | {p99} | {r.note} |"
        )
    return "\n".join(lines)


def render_console(report: BaselineReport) -> str:
    out = [
        f"PeerCache vs Mooncake benchmark baseline @ {report.created_at}",
        f"host: {report.host.get('platform')} | cpus={report.host.get('cpu_count')}",
        "",
    ]
    out.append(render_markdown(report))
    return "\n".join(out)
