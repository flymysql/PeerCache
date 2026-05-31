"""PeerCache benchmark suite.

Installed with the package and exposed as console commands:

    peercache-bench           # systematic SGLang-HiCache benchmark (RDMA-first)
    peercache-bench-micro     # low-level data-plane microbench
    peercache-bench-mooncake  # Mooncake transfer_engine_bench wrapper
    peercache-bench-compare   # PeerCache vs Mooncake sweep

So after ``pip install peercache`` you can run, e.g.::

    peercache-bench suite --device-name mlx5_0 --layout mla --page-size 131072 \
        --batch-size 32 --concurrencies 1,2,4,8,16,32,64 --duration 10

No need to clone the repo or set PYTHONPATH.
"""
