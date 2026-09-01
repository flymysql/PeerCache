"""Cross-node slotmap read verification against LIVE sglang servers.

Node A (sglang, mode=slotmap) has published KV pages into its slot region
(addressable purely by key hash — no directory). This script joins the same
cluster as node C and:

  1. resolves A's slot layout (base addr / geometry / rkeys) via the
     peer-layout RPC — exactly what a slotmap reader does,
  2. issues one-shot bucket READs (transport.batch_read_multi, TCP here) and
     decodes every slot header, proving a reader can locate and read real
     sglang-published pages cross-node without any directory lookup,
  3. reports payload sizes (each sglang KV page) found in A's slot region.

The exact key-hash reconstruction from the raw prompt is not attempted:
sglang's server-side tokenizer / radix-cache hash chaining is not bit-exact
reproducible from outside, but the *mechanism* (hash -> owner -> bucket ->
one READ -> header-validated hit) is exactly what _fetch_slotmap does. The
byte-identical read-back of a specific key is covered by
test_slotmap_e2e_tcp.py at the store level.

Usage:
    python tests/sglang/test_sglang_2node_slotmap_read.py
"""
import ctypes
import os
import sys
import time
import urllib.request
from types import SimpleNamespace

from peercache.store import PeerCacheStore
from peercache.slotmap import HEADER_SIZE, decode_header

DISCOVERY = "127.0.0.1:31998"
METRICS_PORT = 31997


def _cfg(node_id):
    return SimpleNamespace(
        tp_rank=0, tp_size=1, pp_rank=0, pp_size=1, is_mla_model=False,
        extra_config={
            "discovery_addr": DISCOVERY, "protocol": "tcp", "device_name": "",
            "local_hostname": "127.0.0.1", "node_id": node_id,
            "heartbeat_interval": 0.5, "member_ttl": 30.0,
            "global_segment_size": 8 << 20, "metrics_enabled": False,
            "mode": "slotmap", "slot_max_page_bytes": 262144,
            "slot_ways": 4, "slot_num_buckets": 0,
        },
    )


def main():
    port = int(os.environ.get("PEERCACHE_E2E_PORT", "30000"))
    base = "http://127.0.0.1:%d" % port

    # 1. The sglang server (A) is up and PeerCache metrics show writes.
    with urllib.request.urlopen(base + "/health", timeout=10) as r:
        assert r.status == 200, "node A not healthy"
    with urllib.request.urlopen("http://127.0.0.1:%d/metrics" % METRICS_PORT, timeout=10) as r:
        metrics = r.read().decode()
    write_reqs = bytes_written = None
    for line in metrics.splitlines():
        if line.startswith("peercache_write_requests_total"):
            write_reqs = float(line.split()[-1])
        elif line.startswith("peercache_bytes_written_total"):
            bytes_written = float(line.split()[-1])
    print("node A healthy; write_requests=%s bytes_written=%s" % (write_reqs, bytes_written))
    assert write_reqs and write_reqs > 0, "no writes recorded on A"

    # 2. Join the cluster as node C (slotmap mode) — same discovery as A.
    print("\n=== node C joins the live slotmap cluster ===")
    c = PeerCacheStore(_cfg("C"))
    try:
        deadline = time.time() + 15
        while time.time() < deadline and len(c.runtime.ring) < 2:
            time.sleep(0.2)
        print("ring members: %s (size %d)" % (list(c.runtime.ring.nodes), len(c.runtime.ring)))
        assert len(c.runtime.ring) >= 2, "cluster did not reach 2 nodes"

        # 3. Resolve A's slot layout (the directory-free addressing info).
        lay = c._peer_layout("A")
        assert lay is not None and lay.get("ok"), "no slot layout for A"
        geom = lay["geom"]
        nk = lay["rail_endpoints"][0]
        print("A slot layout: base=%d buckets=%d ways=%d stride=%d max_payload=%d"
              % (lay["base_addr"], geom.num_buckets, geom.ways, geom.slot_stride,
                 geom.max_payload))

        # 4. One-shot bucket READs across A's whole slot region; decode headers.
        print("\n=== cross-node slot reads (one READ per bucket, no directory) ===")
        read_ok = 0
        slots = 0
        keys128 = {}
        for b in range(0, geom.num_buckets, 4):
            scratch = (ctypes.c_char * geom.bucket_stride)()
            oks = c.runtime.transport.batch_read_multi(
                [nk], [ctypes.addressof(scratch)],
                [lay["base_addr"] + b * geom.bucket_stride],
                [geom.bucket_stride],
                {nk: list(lay["rail_endpoints"])},
                {nk: [int(x) for x in lay["rkeys"]]},
            )
            if oks and oks[0]:
                read_ok += 1
                for way in range(geom.ways):
                    off = way * geom.slot_stride
                    dec = decode_header(bytes(scratch[off:off + HEADER_SIZE]))
                    if dec and dec[0]:
                        ok, hi, lo, plen, seq = dec
                        slots += 1
                        keys128[(hi, lo)] = plen
        print("bucket reads OK: %d (sampled every 4th)" % read_ok)
        print("distinct sglang KV pages found in A's slot region: %d" % len(keys128))
        for (hi, lo), plen in list(keys128.items())[:8]:
            print("  key128 hi=%016x lo=%016x payload=%d bytes" % (hi, lo, plen))

        if len(keys128) > 0:
            print("\nRESULT: cross-node slotmap read OK — %d pages published by "
                  "a real sglang server were read back from its slot region "
                  "with no directory lookup" % len(keys128))
            return 0
        print("\nWARN: no pages found — did node A serve shared-prefix requests?")
        return 1
    finally:
        c.close()


if __name__ == "__main__":
    sys.exit(main())
