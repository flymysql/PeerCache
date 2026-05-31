"""Mooncake benchmark adapter.

Wraps Mooncake's *official* ``transfer_engine_bench`` binary (shipped inside the
``mooncake-transfer-engine`` wheel) so it runs under the same workload knobs as
the PeerCache benchmark and reports into the same result schema. Using
Mooncake's own benchmark -- rather than re-implementing one -- keeps the
comparison honest: each project is measured by its own data-plane tool.

It launches:
  * an HTTP metadata server (``python -m mooncake.http_metadata_server``)
    (or reuses one passed via --metadata-url for a whole sweep),
  * a ``target`` transfer engine (owns the source segment),
  * an ``initiator`` transfer engine that reads from the target for --duration,

then parses the final ``throughput X GB/s`` line.

Requirements at runtime:
  * ``pip install mooncake-transfer-engine``
  * For ``--protocol rdma``: a working RDMA NIC + ``--device-name`` (e.g. mlx5_0).
  * For ``--protocol tcp``: no NIC needed, but the wheel's shared objects link
    libibverbs / libcudart / libcuda. On a box without those, point
    ``LD_LIBRARY_PATH`` at stubs (see benchmarks/README.md).

This adapter shells out; it never imports the mooncake C-extension into this
process, so a broken/absent install simply yields ``ok=False`` with a reason
instead of crashing the whole baseline run.
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.request
from typing import Optional, Tuple

from common import Workload, make_result, render_console, BaselineReport


def find_bench_binary() -> Optional[str]:
    """Locate the *real* ELF binary, not the console-script shim.

    The wheel installs a ``transfer_engine_bench`` console script on PATH that
    merely ``subprocess.call``s the real binary -- terminating the shim orphans
    the real process. So we prefer the binary inside the package directory.
    """
    try:
        import mooncake  # noqa: F401

        pkg_dir = os.path.dirname(mooncake.__file__)
        cand = os.path.join(pkg_dir, "transfer_engine_bench")
        if os.path.exists(cand):
            try:
                os.chmod(cand, 0o755)
            except OSError:
                pass
            return cand
    except Exception:
        pass
    return shutil.which("transfer_engine_bench")


def _free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _http_ready(url: str, timeout: float = 20.0) -> bool:
    end = time.time() + timeout
    probe = url + "?key=__ready_probe__"
    while time.time() < end:
        try:
            urllib.request.urlopen(probe, timeout=1.0)
            return True
        except urllib.error.HTTPError:
            return True  # any HTTP response (incl. 404) means it's up
        except Exception:
            time.sleep(0.2)
    return False


def _spawn(cmd, env):
    return subprocess.Popen(
        cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, start_new_session=True,
    )


def _kill(p: Optional[subprocess.Popen]) -> None:
    if p is None:
        return
    try:
        os.killpg(os.getpgid(p.pid), signal.SIGTERM)
    except Exception:
        try:
            p.terminate()
        except Exception:
            pass
    try:
        p.wait(timeout=5)
    except Exception:
        try:
            os.killpg(os.getpgid(p.pid), signal.SIGKILL)
        except Exception:
            try:
                p.kill()
            except Exception:
                pass


THROUGHPUT_RE = re.compile(r"throughput\s+([0-9.]+)\s*GB/s", re.IGNORECASE)


def start_metadata_server(port: Optional[int] = None) -> Tuple[Optional[subprocess.Popen], str]:
    """Start a standalone HTTP metadata server. Returns (proc, url)."""
    port = port or _free_port()
    url = f"http://127.0.0.1:{port}/metadata"
    proc = _spawn(
        [sys.executable, "-m", "mooncake.http_metadata_server", "--port", str(port)],
        dict(os.environ),
    )
    if not _http_ready(url, timeout=25.0):
        _kill(proc)
        raise RuntimeError("mooncake http_metadata_server did not become ready")
    return proc, url


def bench_transfer_engine(
    wl: Workload,
    protocol: str,
    device_name: str = "",
    metadata_url: Optional[str] = None,
):
    """Run one Mooncake transfer-engine read benchmark point.

    If ``metadata_url`` is given, reuse that metadata server; otherwise start a
    throwaway one for this single point.
    """
    binp = find_bench_binary()
    if binp is None:
        return make_result(
            "mooncake", "transfer-engine", protocol, wl, 0, 0, 0.0,
            note="transfer_engine_bench not found (pip install mooncake-transfer-engine)",
            ok=False,
        )

    env = dict(os.environ)
    owns_meta = metadata_url is None
    meta_proc = None
    target = None
    try:
        if owns_meta:
            meta_proc, metadata_url = start_metadata_server()

        target_port = _free_port()
        initiator_port = _free_port()

        common_flags = [
            f"-protocol={protocol}",
            "-use_vram=false",
            "-gpu_id=-1",
            f"-metadata_server={metadata_url}",
            f"-block_size={wl.block_size}",
            f"-batch_size={wl.batch_size}",
            f"-threads={wl.threads}",
            f"-buffer_size={max(wl.block_size * wl.batch_size * 8, 1 << 28)}",
        ]
        if protocol == "rdma" and device_name:
            common_flags.append(f"-device_name={device_name}")

        target = _spawn(
            [binp, "-mode=target", f"-local_server_name=127.0.0.1:{target_port}"] + common_flags,
            env,
        )
        time.sleep(3.0)  # let the target register its segment
        if target.poll() is not None:
            out = target.stdout.read() if target.stdout else ""
            return make_result(
                "mooncake", "transfer-engine", protocol, wl, 0, 0, 0.0,
                note=f"target exited early: {out.splitlines()[-1][:120] if out else 'no output'}",
                ok=False,
            )

        init_cmd = [
            binp,
            "-mode=initiator",
            f"-local_server_name=127.0.0.1:{initiator_port}",
            f"-segment_id=127.0.0.1:{target_port}",
            f"-operation={wl.operation}",
            f"-duration={int(max(1, wl.duration))}",
            "-report_unit=GB",
        ] + common_flags
        out = subprocess.run(
            init_cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, timeout=wl.duration + 90,
        ).stdout

        m = None
        for line in out.splitlines():
            mm = THROUGHPUT_RE.search(line)
            if mm:
                m = mm
        if m is None:
            tail = "\n".join(out.splitlines()[-6:])
            return make_result(
                "mooncake", "transfer-engine", protocol, wl, 0, 0, 0.0,
                note=f"no throughput parsed; tail: {tail[:160]}", ok=False,
            )

        gbps = float(m.group(1))
        elapsed = float(max(1, wl.duration))
        bytes_total = int(gbps * 1e9 * elapsed)
        ops = bytes_total // wl.block_size if wl.block_size else 0
        return make_result(
            "mooncake", "transfer-engine", protocol, wl, ops, bytes_total, elapsed,
            note="official transfer_engine_bench (read)",
        )
    except Exception as e:  # noqa: BLE001
        return make_result(
            "mooncake", "transfer-engine", protocol, wl, 0, 0, 0.0,
            note=f"error: {type(e).__name__}: {e}", ok=False,
        )
    finally:
        _kill(target)
        if owns_meta:
            _kill(meta_proc)


def main() -> None:
    ap = argparse.ArgumentParser(description="Mooncake transfer-engine benchmark")
    ap.add_argument("--protocol", default="tcp", choices=["tcp", "rdma"])
    ap.add_argument("--device-name", default="")
    ap.add_argument("--block-size", type=int, default=65536)
    ap.add_argument("--batch-size", type=int, default=64)
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--duration", type=float, default=5.0)
    ap.add_argument("--metadata-url", default=None)
    args = ap.parse_args()

    wl = Workload(
        block_size=args.block_size,
        batch_size=args.batch_size,
        threads=args.threads,
        duration=args.duration,
    )
    report = BaselineReport()
    report.add(bench_transfer_engine(wl, args.protocol, args.device_name, args.metadata_url))
    print(render_console(report))


if __name__ == "__main__":
    main()
