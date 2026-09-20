# Security Policy

## Scope

PeerCache is a **KV-cache data plane**. It runs as a library inside (or alongside) an
SGLang prefill/decode deployment, listens on the network for peer registrations and
one-sided RDMA reads, and writes KV pages into memory it has registered with the NIC.

The security-relevant properties are therefore about **what a peer on the network can
read or make the process do**, not about user-facing accounts.

## Threat model — be aware of these

- **There is no authentication or encryption between peers.** Discovery and data
  transfer assume a trusted network (the same assumption the surrounding inference
  cluster makes). Do not expose the discovery or data ports to an untrusted network
  or to the public internet.
- **A peer can request KV pages by key.** If an untrusted party can reach the data
  plane, it can read cached KV content. Treat the KV cache as sensitive: for
  multi-tenant deployments use the tenant/model isolation described in the README
  rather than relying on network isolation alone.
- **Registered memory is a shared resource.** Keyspace exhaustion, oversized or
  malformed requests, and pool misconfiguration can degrade or crash the process.
  Capacity/limit configuration is part of your deployment's safety envelope.
- **The RDMA fast path is written in C++ against raw libibverbs.** Memory-safety bugs
  there are plausible; prefer the TCP fallback if you are evaluating the project
  rather than running it.

## Supported versions

Only the latest published version on PyPI receives fixes.

## Reporting a vulnerability

Please **do not** open a public issue.

Preferred: GitHub [private vulnerability reporting](https://github.com/flymysql/PeerCache/security/advisories/new)
on this repository. If unavailable, email **flyphp@outlook.com**.

Please include:

- the affected version (`pip show peercache`),
- whether RDMA or the TCP fallback path is involved,
- a description of the impact and the attacker model,
- reproduction steps or a proof of concept,
- any suggested fix.

**Do not** include credentials, private keys, or hostnames of machines you don't own.

## What to expect

This is a spare-time project. You'll get an acknowledgement, an assessment, and — for
valid reports — a fix and credit in the release notes unless you prefer otherwise.

## Out of scope

- Deployments that deliberately expose the data plane to an untrusted network.
- Vulnerabilities in libibverbs, the NIC driver or firmware, SGLang, or the OS —
  please report those upstream.
- Denial of service that requires the ability to already send valid RDMA traffic.
- Performance claims that differ from yours on different hardware.
