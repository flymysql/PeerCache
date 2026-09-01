"""SGLang + PeerCache end-to-end integration test (GPU required).

Launches a real sglang server with --enable-hierarchical-cache and the
PeerCache dynamic storage backend, then drives real /generate requests and
asserts PeerCache's Prometheus metrics show KV pages were published.

Design notes (from the L20 validation run):
  * protocol=tcp : exercises the full control plane (discovery -> DHT directory
    -> published pool -> cross-node fetch) without RDMA hardware.
  * --hicache-write-policy write_through : publish KV to L3 as it is produced;
    without it L3 often stays empty (write_requests=0).
  * --hicache-ratio 1.05 : sglang asserts host memory > device memory, so the
    ratio must stay above 1.0; with 1.05 the L2 tier is small enough that pages
    actually flow down to PeerCache.
  * --disable-cuda-graph : avoids JIT-compiling CUDA-graph kernels on machines
    with an older host gcc (functional test only, not a perf test).
  * --mem-fraction-static 0.4 : keeps GPU headroom for a second server (B).

Usage:
    python tests/sglang/test_sglang_e2e.py [--model-path PATH] [--port N]

Environment:
    PEERCACHE_SGLANG_PY : python interpreter with sglang+peercache installed
                          (default: sys.executable)
"""
import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.request

MODEL = os.environ.get(
    "PEERCACHE_E2E_MODEL",
    "Qwen/Qwen2.5-0.5B-Instruct",
)
PORT = int(os.environ.get("PEERCACHE_E2E_PORT", "30000"))
DISCOVERY = "127.0.0.1:31998"
METRICS_PORT = 31997
MODE = os.environ.get("PEERCACHE_E2E_MODE", "p2p")


def _extra_config(node_id="A", mode="p2p"):
    """Build the PeerCache extra-config JSON for the given mode."""
    base = (
        '"backend_name":"peercache","module_path":"peercache.store",'
        '"class_name":"PeerCacheStore","discovery_addr":"%s","protocol":"tcp",'
        '"local_hostname":"127.0.0.1","node_id":"%s","global_segment_size":"1gb",'
        '"metrics_enabled":true'
    ) % (DISCOVERY, node_id)
    if mode == "slotmap":
        # Deterministic slot addressing: no directory, key hashes to a fixed
        # physical slot; the reader READs the whole N-way bucket in one shot.
        # slot_max_page_bytes must be >= the KV page size sglang emits.
        base += (
            ',"mode":"slotmap","slot_max_page_bytes":262144,'
            '"slot_ways":4,"slot_num_buckets":0'
        )
    return "{%s}" % base


SHARED_PREFIX = (
    "The theory of general relativity, published by Albert Einstein in 1915, "
    "describes gravity as a geometric property of spacetime. Massive objects "
    "curve spacetime, and this curvature dictates the motion of other objects. "
    "Key predictions include the bending of light around massive bodies, the "
    "precession of planetary orbits, and gravitational time dilation."
)


def _http_json(url, payload):
    body = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=180) as r:
        return json.loads(r.read())


def wait_ready(base, timeout=180):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(base + "/health", timeout=5) as r:
                if r.status == 200:
                    return True
        except Exception:
            time.sleep(2)
    return False


def fetch_metric(name):
    with urllib.request.urlopen("http://127.0.0.1:%d/metrics" % METRICS_PORT, timeout=10) as r:
        for line in r.read().decode().splitlines():
            if line.startswith(name):
                return float(line.split()[-1])
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model-path", default=MODEL)
    ap.add_argument("--port", type=int, default=PORT)
    ap.add_argument("--requests", type=int, default=4)
    ap.add_argument("--mode", default=MODE, choices=["p2p", "slotmap"],
                    help="PeerCache mode: p2p (directory) or slotmap (directory-free)")
    ap.add_argument("--no-server", action="store_true",
                    help="skip server launch; attach to an already-running one")
    args = ap.parse_args()

    py = os.environ.get("PEERCACHE_SGLANG_PY", sys.executable)
    proc = None
    if not args.no_server:
        cmd = [
            py, "-m", "sglang.launch_server",
            "--model-path", args.model_path,
            "--host", "0.0.0.0", "--port", str(args.port),
            "--enable-hierarchical-cache",
            "--hicache-write-policy", "write_through",
            "--hicache-ratio", "1.05",
            "--hicache-storage-backend", "dynamic",
            "--hicache-storage-backend-extra-config", _extra_config(mode=args.mode),
            "--disable-cuda-graph",
        ]
        print("launching (%s mode): %s" % (args.mode, " ".join(cmd[:6])))
        # Build a clean env for the server child: inherit everything, then
        # make sure the CUDA libs are visible to BOTH the dynamic loader
        # (LD_LIBRARY_PATH) and the JIT linker (LIBRARY_PATH). On machines
        # whose conda env carries the CUDA toolkit (e.g. devcloud where the
        # system CUDA is older than sgl-kernel requires), point
        # PEERCACHE_CUDA_LIB at that directory.
        env = dict(os.environ)
        conda_lib = os.environ.get("PEERCACHE_CUDA_LIB", "")
        if conda_lib:
            env["LIBRARY_PATH"] = conda_lib + os.pathsep + env.get("LIBRARY_PATH", "")
            env["LD_LIBRARY_PATH"] = conda_lib + os.pathsep + env.get("LD_LIBRARY_PATH", "")
        # Redirect server output to a file, NOT a PIPE: sglang logs a heartbeat
        # line every few seconds; a full PIPE buffer would block the server.
        log_path = os.path.join(tempfile.gettempdir(), "peercache_sglang_e2e.log")
        logf = open(log_path, "wb")
        proc = subprocess.Popen(cmd, stdout=logf, stderr=subprocess.STDOUT, env=env)
        if not wait_ready("http://127.0.0.1:%d" % args.port):
            logf.flush()
            tail = open(log_path, "rb").read()[-4000:].decode(errors="replace")
            proc.kill()
            raise RuntimeError("sglang server failed to start:\n%s" % tail)
    else:
        if not wait_ready("http://127.0.0.1:%d" % args.port, timeout=30):
            raise RuntimeError("no server on port %d" % args.port)

    base = "http://127.0.0.1:%d" % args.port
    print("=== Phase 1: shared-prefix requests (write KV to PeerCache) ===")
    for i in range(args.requests):
        resp = _http_json(base + "/generate", {
            "text": SHARED_PREFIX + " Question %d: what follows?" % i,
            "sampling_params": {"max_new_tokens": 8, "temperature": 0},
        })
        meta = resp.get("meta_info", {}) or {}
        print("  req%d cached_tokens=%d" % (i, meta.get("cached_tokens", 0)))

    print("=== Phase 2: same prefix again (L2/L3 hit) ===")
    for i in range(args.requests):
        resp = _http_json(base + "/generate", {
            "text": SHARED_PREFIX + " Question %d: what follows?" % i,
            "sampling_params": {"max_new_tokens": 8, "temperature": 0},
        })
        meta = resp.get("meta_info", {}) or {}
        print("  req%d-again cached_tokens=%d" % (i, meta.get("cached_tokens", 0)))

    print("=== Phase 3: PeerCache metrics assertions ===")
    time.sleep(2)
    write_reqs = fetch_metric("peercache_write_requests_total")
    pool_keys = fetch_metric("peercache_pool_keys")
    members = fetch_metric("peercache_members")
    bytes_written = fetch_metric("peercache_bytes_written_total")
    print("  write_requests=%s pool_keys=%s members=%s bytes_written=%s"
          % (write_reqs, pool_keys, members, bytes_written))

    ok = True
    if not write_reqs or write_reqs < args.requests:
        print("  FAIL: PeerCache received no/too few writes (got %s, want >= %d)"
              % (write_reqs, args.requests))
        ok = False
    if args.mode == "p2p" and (not pool_keys or pool_keys <= 0):
        # p2p mode publishes pages into a node-local pool; slotmap mode has no
        # pool (keys hash straight to physical slots), so pool_keys stays 0.
        print("  FAIL: PeerCache pool is empty")
        ok = False
    if not bytes_written or bytes_written <= 0:
        print("  FAIL: no KV bytes written to PeerCache")
        ok = False
    if not members or members < 1:
        print("  FAIL: PeerCache membership ring empty")
        ok = False
    if ok:
        print("PASS: KV pages were published into PeerCache through sglang HiCache (%s)"
              % args.mode)

    if proc is not None:
        proc.terminate()
        try:
            proc.wait(timeout=15)
        except Exception:
            proc.kill()
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())

