"""Regenerate the performance-baseline charts in docs/assets/perf/.

Run:  python docs/assets/perf/make_charts.py
Data points come from the cross-host RDMA runs documented in performance.md.
Keep this in sync with that page so the figures can be reproduced.
"""

import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

OUT = os.path.dirname(os.path.abspath(__file__))
PRIMARY = "#3f51b5"   # indigo (matches the docs theme)
ACCENT = "#ff7043"
GREY = "#9e9e9e"
plt.rcParams.update({"font.size": 11, "figure.autolayout": True})


def scaling_ladder():
    labels = [
        "Bare ib_read_bw\n1 NIC",
        "PeerCache GET\n1 NIC (8 proc)",
        "PeerCache GET\n1 process, 8 rails\n(1 MiB pages)",
        "PeerCache GET\n8 NICs, multi-process\n(1 MiB pages)",
    ]
    vals = [49.0, 46.0, 147.6, 273.0]
    colors = [GREY, PRIMARY, PRIMARY, ACCENT]
    fig, ax = plt.subplots(figsize=(8, 4.2))
    bars = ax.barh(range(len(labels)), vals, color=colors)
    ax.set_yticks(range(len(labels)))
    ax.set_yticklabels(labels)
    ax.invert_yaxis()
    ax.set_xlabel("GET throughput (GB/s, 10⁹ bytes/s)")
    ax.set_title("Throughput scaling ladder (cross-host RDMA, MLA)", pad=14)
    ax.axvline(392.0, ls="--", color="#bdbdbd")
    ax.text(388.0, len(labels) - 0.5, "8-NIC bare ceiling ≈ 392 GB/s",
            color="#757575", ha="right", va="bottom", fontsize=9, rotation=90)
    for b, v in zip(bars, vals):
        ax.text(b.get_width() + 4, b.get_y() + b.get_height() / 2,
                f"{v:.1f}", va="center", fontsize=10)
    ax.set_xlim(0, 410)
    fig.savefig(os.path.join(OUT, "scaling_ladder.png"), dpi=140)
    plt.close(fig)


def single_process_scaling():
    fig, ax = plt.subplots(figsize=(7.5, 4.2))
    c128 = ([4, 8, 16], [40.4, 28.4, 21.4])
    c1m = ([2, 4], [147.6, 134.5])
    ax.plot(c1m[0], c1m[1], "o-", color=ACCENT, lw=2, label="1 MiB pages (batch 128)")
    ax.plot(c128[0], c128[1], "s-", color=PRIMARY, lw=2, label="128 KiB pages (batch 32)")
    for x, y in zip(*c1m):
        ax.annotate(f"{y:.1f}", (x, y), textcoords="offset points", xytext=(0, 8), ha="center", fontsize=9)
    for x, y in zip(*c128):
        ax.annotate(f"{y:.1f}", (x, y), textcoords="offset points", xytext=(0, 8), ha="center", fontsize=9)
    ax.set_xlabel("threads (single process, 8 rails)")
    ax.set_ylabel("GET throughput (GB/s)")
    ax.set_title("Single-process multi-rail: throughput vs concurrency")
    ax.set_xticks([2, 4, 8, 16])
    ax.set_ylim(0, 165)
    ax.grid(True, ls=":", alpha=0.5)
    ax.legend()
    fig.savefig(os.path.join(OUT, "single_process_scaling.png"), dpi=140)
    plt.close(fig)


def per_card():
    cards = [f"bond_{i}" for i in range(1, 9)]
    vals = [50.139, 35.187, 16.862, 17.202, 36.914, 38.946, 37.227, 40.561]
    fig, ax = plt.subplots(figsize=(8, 4.2))
    bars = ax.bar(cards, vals, color=PRIMARY)
    ax.axhline(49.0, ls="--", color=GREY)
    ax.text(7.4, 49.0 + 0.6, "single-NIC ceiling 49", color="#757575", ha="right", fontsize=9)
    for b, v in zip(bars, vals):
        ax.text(b.get_x() + b.get_width() / 2, v + 0.6, f"{v:.1f}", ha="center", fontsize=9)
    ax.set_ylabel("GET throughput (GB/s)")
    ax.set_title("Per-NIC throughput, 8-NIC multi-process run (Σ = 273.0 GB/s ≈ 2.18 Tbps)")
    ax.set_ylim(0, 56)
    fig.savefig(os.path.join(OUT, "per_card.png"), dpi=140)
    plt.close(fig)


if __name__ == "__main__":
    scaling_ladder()
    single_process_scaling()
    per_card()
    print("wrote charts to", OUT)
