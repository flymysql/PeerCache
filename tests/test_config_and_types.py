"""Config validation + directory wire-format (DataLocation) compatibility."""

import pytest

from peercache.config import PeerCacheConfig
from peercache.types import DIRECTORY_SCHEMA_VERSION, DataLocation


def _cfg(**over):
    base = dict(discovery_addr="127.0.0.1:31998", protocol="tcp")
    base.update(over)
    return PeerCacheConfig(**base)


def test_config_valid_defaults():
    c = _cfg(device_names="mlx5_0,mlx5_1")
    assert c.device_rails() == ["mlx5_0", "mlx5_1"]
    # single / auto-pick rail
    assert _cfg().device_rails() == [""]
    assert _cfg(device_name="mlx5_3").device_rails() == ["mlx5_3"]
    assert _cfg(mode="centralized", role="storage").is_centralized()
    assert _cfg(mode="centralized").effective_role() == "inference"
    assert _cfg(mode="centralized").effective_role(for_storage_server=True) == "storage"


@pytest.mark.parametrize("over", [
    {"protocol": "udp"},
    {"discovery_addr": "no-port"},
    {"mode": "hybrid"},
    {"role": "worker"},
    {"mode": "p2p", "role": "storage"},
    {"ib_port": 0},
    {"ib_port": 999},
    {"gid_index": -2},
    {"global_segment_size": 0},
    {"max_channels_per_peer": 0},
    {"device_names": "mlx5_0,mlx5_0"},      # duplicate rail
    {"device_names": "mlx5_0,mlx5_1,mlx5_0"},
])
def test_config_rejects_bad(over):
    with pytest.raises(ValueError):
        _cfg(**over)


def test_datalocation_roundtrip_and_version():
    loc = DataLocation(
        node_id="n1", rdma_endpoint="10.0.0.1:5000", remote_addr=4096,
        rkey=7, length=131072, resident=True,
        rail_endpoints=["10.0.0.1:5000", "10.0.0.1:5001"],
        rail_rkeys=[7, 9],
    )
    d = loc.to_dict()
    assert d["v"] == DIRECTORY_SCHEMA_VERSION
    back = DataLocation.from_dict(d)
    assert back == loc
    assert back.endpoints() == ["10.0.0.1:5000", "10.0.0.1:5001"]
    assert back.rkeys() == [7, 9]


def test_datalocation_legacy_single_rail_compat():
    # A v1 producer (no rail_* fields, no version) must still deserialise, and
    # endpoints()/rkeys() must fall back to the single-rail values.
    legacy = {
        "node_id": "n1", "rdma_endpoint": "10.0.0.1:5000",
        "remote_addr": 4096, "rkey": 7, "length": 131072, "resident": True,
    }
    loc = DataLocation.from_dict(legacy)
    assert loc.endpoints() == ["10.0.0.1:5000"]
    assert loc.rkeys() == [7]
    # Unknown future keys are ignored.
    loc2 = DataLocation.from_dict({**legacy, "v": 999, "future_field": "x"})
    assert loc2.rkey == 7
