# Contributing to PeerCache

PeerCache is a systems project: most of it is a C++ RDMA data plane with a thin Python control surface. Changes here have to be correct under concurrency and honest about performance, so the bar for a patch is "I ran it on real hardware" rather than "it compiles".

## Good first contributions

- Bug reports with a **reproduction** — especially around discovery/heartbeat races, slotmap placement, or the TCP fallback path.
- Documentation fixes in `docs/` (English at `docs/*.md`, Chinese at `docs/*.zh.md`).
- Benchmark methodology improvements. If you have a different NIC/topology, a measured and clearly-described result is genuinely useful.
- Portability: this is developed against RoCE/IB on Linux; reports from other setups are welcome.

Please open an issue before starting anything non-trivial (new transport, on-disk format change, new placement mode) so we can agree on the design first.

## Development setup

```bash
git clone https://github.com/flymysql/PeerCache
cd PeerCache
pip install -e ".[test]"
pytest -q
```

To build the RDMA data plane you need the verbs headers:

```bash
# Debian/Ubuntu
apt-get install -y libibverbs-dev librdmacm-dev
# verify the extension actually built with RDMA (not the stub)
python -c "from peercache import _peercache; print(_peercache.HAS_RDMA)"
```

On a machine with no RDMA NIC the extension still loads and falls back to TCP with a clear log line — that path must keep working, so please run the suite both with and without `PEERCACHE_NO_RDMA=ON` if you touch the transport layer.

## Before you open a PR

- `pytest -q` must pass. If a test is already failing on `main`, say so in the PR so we can separate the two.
- One logical change per PR. Don't reformat unrelated files — large whitespace-only diffs will be asked to be reverted.
- **Line endings: the repo stores CRLF in some files and LF in others.** Do not let your editor normalize whole files; it buries the real diff.
- Match the surrounding style rather than importing a new one.
- New runtime dependencies need a reason in the PR description. The C++ side should stay free of new libraries unless there's a strong justification.

## Performance claims

This is the part that matters most and is easiest to get wrong:

- **Publishable numbers require real RDMA hardware (`ib_read_bw`-class).** State the NIC, link rate, topology (single/multi-rail), message size, concurrency, and how many runs.
- Never quote a TCP-fallback number next to an RDMA number without labelling it.
- If you change the data path, include a before/after on the same machine. `docs/performance.md` describes the reference methodology and `python/peercache/bench/README.md` covers the two-node `peercache-bench serve`/`drive` harness.

## Docs

Every `docs/<name>.md` has a Chinese counterpart `docs/<name>.zh.md`. If you change one, update the other (or say in the PR that you can't and ask for help).

## Reporting a security issue

Please **do not** open a public issue. See [SECURITY.md](./SECURITY.md).

## License

By contributing, you agree your contribution is licensed under the Apache License 2.0 (see [LICENSE](./LICENSE)).

## Code of conduct

Be decent to each other. See [CODE_OF_CONDUCT.md](./CODE_OF_CONDUCT.md).
