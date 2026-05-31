"""Operational metrics + a self-contained Prometheus endpoint and dashboard.

`Metrics` collects counters, gauges (via providers) and latency reservoirs in a
thread-safe way. `MetricsServer` exposes them over HTTP:

- ``GET /metrics``   -> Prometheus text exposition (scrape with Prometheus/Grafana)
- ``GET /``          -> built-in HTML dashboard (auto-refreshing, no external deps)
- ``GET /healthz``   -> ``ok``

The dashboard is intentionally dependency-free (vanilla JS, inline canvas) so it
works on air-gapped inference nodes. It polls ``/metrics`` and renders current
gauges, the read hit-rate, per-window read/write rates, and latency p50/p99/avg.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable, Deque, Dict, List, Optional

logger = logging.getLogger(__name__)

_COUNTERS = (
    "read_requests",
    "read_hits",
    "read_misses",
    "read_local_hits",
    "read_remote_hits",
    "read_disk_hits",
    "write_requests",
    "bytes_read",
    "bytes_written",
    "evictions",
    "disk_writes",
    "disk_bytes_written",
    "disk_evictions",
    "promotes",
)

_QUANTILES = (0.5, 0.9, 0.99)


class Metrics:
    def __init__(self, node_id: str = "", reservoir: int = 8192):
        self.node_id = node_id
        self._lock = threading.Lock()
        self._counters: Dict[str, int] = {name: 0 for name in _COUNTERS}
        self._gauges: Dict[str, Callable[[], float]] = {}
        self._lat: Dict[str, Deque[float]] = {
            "read": deque(maxlen=reservoir),
            "write": deque(maxlen=reservoir),
        }
        self._start = time.time()

    # -- mutation ----------------------------------------------------------- #
    def inc(self, name: str, n: int = 1) -> None:
        with self._lock:
            self._counters[name] = self._counters.get(name, 0) + n

    def observe(self, op: str, seconds: float) -> None:
        with self._lock:
            self._lat.setdefault(op, deque(maxlen=8192)).append(seconds)

    def set_gauge_provider(self, name: str, fn: Callable[[], float]) -> None:
        self._gauges[name] = fn

    def record_read(self, hit: bool, nbytes: int, seconds: float,
                    source: Optional[str] = None) -> None:
        with self._lock:
            self._counters["read_requests"] += 1
            if hit:
                self._counters["read_hits"] += 1
                self._counters["bytes_read"] += nbytes
                if source in ("local", "remote", "disk"):
                    self._counters[f"read_{source}_hits"] += 1
            else:
                self._counters["read_misses"] += 1
            self._lat["read"].append(seconds)

    def record_write(self, nbytes: int, seconds: float) -> None:
        with self._lock:
            self._counters["write_requests"] += 1
            self._counters["bytes_written"] += nbytes
            self._lat["write"].append(seconds)

    # -- read-out ----------------------------------------------------------- #
    @staticmethod
    def _quantile(sorted_vals: List[float], q: float) -> float:
        if not sorted_vals:
            return 0.0
        idx = min(len(sorted_vals) - 1, int(round(q * (len(sorted_vals) - 1))))
        return sorted_vals[idx]

    def snapshot(self) -> dict:
        with self._lock:
            counters = dict(self._counters)
            lat = {op: sorted(v) for op, v in self._lat.items()}
            gauges = {}
            for name, fn in self._gauges.items():
                try:
                    gauges[name] = float(fn())
                except Exception:
                    gauges[name] = 0.0
        out = {"counters": counters, "gauges": gauges, "latency": {}}
        for op, vals in lat.items():
            avg = sum(vals) / len(vals) if vals else 0.0
            out["latency"][op] = {
                "avg": avg,
                "count": len(vals),
                **{f"p{int(q * 100)}": self._quantile(vals, q) for q in _QUANTILES},
            }
        reads = counters["read_requests"]
        out["read_hit_rate"] = (counters["read_hits"] / reads) if reads else 0.0
        out["uptime_seconds"] = time.time() - self._start
        return out

    def render_prometheus(self) -> str:
        s = self.snapshot()
        lines: List[str] = []
        lbl = f'{{node="{self.node_id}"}}' if self.node_id else ""

        def emit(metric: str, value, mtype: str, help_text: str, labels: str = ""):
            lines.append(f"# HELP peercache_{metric} {help_text}")
            lines.append(f"# TYPE peercache_{metric} {mtype}")
            lines.append(f"peercache_{metric}{labels or lbl} {value}")

        for name, value in s["counters"].items():
            emit(f"{name}_total", value, "counter", f"PeerCache {name}")
        for name, value in s["gauges"].items():
            emit(name, value, "gauge", f"PeerCache {name}")
        emit("read_hit_rate", f"{s['read_hit_rate']:.6f}", "gauge",
             "Read hit rate (hits / requests)")
        emit("uptime_seconds", f"{s['uptime_seconds']:.1f}", "gauge", "Process uptime")
        for op, st in s["latency"].items():
            base = f'{{node="{self.node_id}",op="{op}"}}'
            for q in _QUANTILES:
                ql = f'{{node="{self.node_id}",op="{op}",quantile="{q}"}}'
                lines.append("# TYPE peercache_op_latency_seconds summary")
                lines.append(f"peercache_op_latency_seconds{ql} {st[f'p{int(q*100)}']:.6f}")
            lines.append(f"peercache_op_latency_seconds_avg{base} {st['avg']:.6f}")
            lines.append(f"peercache_op_latency_seconds_count{base} {st['count']}")
        return "\n".join(lines) + "\n"


_DASHBOARD_HTML = """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>PeerCache metrics</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
 body{font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;margin:0;background:#0f1116;color:#e6e6e6}
 header{padding:14px 20px;background:#161a22;border-bottom:1px solid #232838;display:flex;justify-content:space-between;align-items:center}
 h1{font-size:16px;margin:0;font-weight:600} .sub{color:#8b93a7;font-size:12px}
 .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px;padding:20px}
 .card{background:#161a22;border:1px solid #232838;border-radius:10px;padding:14px}
 .card .k{color:#8b93a7;font-size:12px;text-transform:uppercase;letter-spacing:.04em}
 .card .v{font-size:24px;font-weight:600;margin-top:6px} .card .u{color:#8b93a7;font-size:12px}
 .charts{padding:0 20px 24px} canvas{background:#161a22;border:1px solid #232838;border-radius:10px;width:100%;height:160px}
 table{width:100%;border-collapse:collapse;font-size:13px} td{padding:4px 8px;border-bottom:1px solid #232838}
 td.n{color:#8b93a7} .ok{color:#4ade80} .warn{color:#fbbf24}
</style></head><body>
<header><div><h1>PeerCache</h1><div class="sub" id="node">node</div></div>
<div class="sub" id="uptime"></div></header>
<div class="grid" id="cards"></div>
<div class="charts">
 <div class="sub" style="margin:0 0 6px">Throughput (ops/s over scrape window)</div>
 <canvas id="chart" width="1000" height="160"></canvas>
</div>
<script>
const hist={read:[],write:[]}; let prev=null, prevT=0;
function fmtBytes(b){if(b<1024)return b+" B";const u=["KB","MB","GB","TB"];let i=-1;do{b/=1024;i++}while(b>=1024&&i<3);return b.toFixed(1)+" "+u[i]}
function parseProm(txt){const m={};txt.split("\\n").forEach(l=>{if(!l||l[0]=="#")return;const sp=l.lastIndexOf(" ");if(sp<0)return;const name=l.slice(0,sp);const val=parseFloat(l.slice(sp+1));m[name]=val;});return m}
function g(m,k){for(const n in m){if(n.startsWith("peercache_"+k))return m[n]}return 0}
async function tick(){
 let txt; try{txt=await (await fetch("/metrics")).text()}catch(e){return}
 const m=parseProm(txt); const now=Date.now()/1000;
 const rr=g(m,"read_requests_total"), rh=g(m,"read_hits_total"), wr=g(m,"write_requests_total");
 const hr=g(m,"read_hit_rate");
 document.getElementById("node").textContent="node "+(txt.match(/node="([^"]*)"/)||[])[1];
 document.getElementById("uptime").textContent="uptime "+Math.round(g(m,"uptime_seconds"))+"s";
 const cards=[
  ["pool used",fmtBytes(g(m,"pool_bytes_used")),"of "+fmtBytes(g(m,"pool_capacity_bytes"))],
  ["pool keys",g(m,"pool_keys")|0,""],
  ["disk used",fmtBytes(g(m,"disk_bytes_used")),"of "+fmtBytes(g(m,"disk_capacity_bytes"))],
  ["disk keys",g(m,"disk_keys")|0,""],
  ["read hit rate",(hr*100).toFixed(1)+"%",rh+"/"+rr],
  ["disk hits",g(m,"read_disk_hits_total")|0,"promotes "+(g(m,"promotes_total")|0)],
  ["bytes read",fmtBytes(g(m,"bytes_read_total")),""],
  ["bytes written",fmtBytes(g(m,"bytes_written_total")),""],
  ["read p50/p99",(g(m,"op_latency_seconds")*1e3).toFixed(2)+" ms",""],
  ["members",g(m,"members")|0,""],
 ];
 document.getElementById("cards").innerHTML=cards.map(c=>`<div class="card"><div class="k">${c[0]}</div><div class="v">${c[1]}</div><div class="u">${c[2]}</div></div>`).join("");
 if(prev){const dt=Math.max(0.1,now-prevT);hist.read.push((rr-prev.rr)/dt);hist.write.push((wr-prev.wr)/dt);if(hist.read.length>120){hist.read.shift();hist.write.shift()}}
 prev={rr,wr}; prevT=now; draw();
}
function draw(){const c=document.getElementById("chart"),x=c.getContext("2d");x.clearRect(0,0,c.width,c.height);
 const series=[["#4ade80",hist.read],["#60a5fa",hist.write]];const max=Math.max(1,...hist.read,...hist.write);
 series.forEach(([col,arr])=>{x.strokeStyle=col;x.lineWidth=2;x.beginPath();arr.forEach((v,i)=>{const px=i/Math.max(1,arr.length-1)*c.width,py=c.height-(v/max)*(c.height-10)-5;i?x.lineTo(px,py):x.moveTo(px,py)});x.stroke()});
 x.fillStyle="#8b93a7";x.font="11px sans-serif";x.fillText("read",8,14);x.fillStyle="#60a5fa";x.fillText("write",46,14);x.fillStyle="#8b93a7";x.fillText("max "+max.toFixed(0)+"/s",c.width-80,14);
}
tick();setInterval(tick,2000);
</script></body></html>"""


class MetricsServer:
    """Threaded HTTP server exposing /metrics and the embedded dashboard."""

    def __init__(self, metrics: Metrics, host: str, port: int, dashboard: bool = True):
        self.metrics = metrics
        self.dashboard = dashboard
        self._httpd: Optional[ThreadingHTTPServer] = None
        self.port = port

        m = metrics
        serve_dashboard = dashboard

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args):  # silence access log
                pass

            def _send(self, code, body: bytes, ctype: str):
                self.send_response(code)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                try:
                    self.wfile.write(body)
                except Exception:
                    pass

            def do_GET(self):
                if self.path.startswith("/metrics"):
                    self._send(200, m.render_prometheus().encode("utf-8"),
                               "text/plain; version=0.0.4; charset=utf-8")
                elif self.path.startswith("/healthz"):
                    self._send(200, b"ok", "text/plain")
                elif serve_dashboard and self.path in ("/", "/dashboard", "/index.html"):
                    self._send(200, _DASHBOARD_HTML.encode("utf-8"), "text/html; charset=utf-8")
                else:
                    self._send(404, b"not found", "text/plain")

        self._handler_cls = Handler
        self._host = host

    def start(self) -> Optional[int]:
        try:
            ThreadingHTTPServer.allow_reuse_address = True
            self._httpd = ThreadingHTTPServer((self._host, self.port), self._handler_cls)
        except OSError as e:
            logger.warning(
                "peercache metrics server could not bind %s:%d (%s); metrics "
                "disabled on this node.", self._host, self.port, e
            )
            self._httpd = None
            return None
        self.port = self._httpd.server_address[1]
        threading.Thread(target=self._httpd.serve_forever, daemon=True,
                         name="peercache-metrics").start()
        logger.info("PeerCache metrics on http://%s:%d/ (Prometheus: /metrics)",
                    self._host, self.port)
        return self.port

    def stop(self) -> None:
        if self._httpd is not None:
            try:
                self._httpd.shutdown()
                self._httpd.server_close()
            except Exception:
                pass
            self._httpd = None
