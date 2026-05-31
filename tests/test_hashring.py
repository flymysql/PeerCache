from collections import Counter

from peercache.hashring import ConsistentHashRing


def test_empty_ring_returns_none():
    ring = ConsistentHashRing(vnodes=16)
    assert ring.get_node("anything") is None
    assert ring.get_nodes("anything", 3) == []


def test_deterministic_mapping():
    a = ConsistentHashRing(vnodes=64)
    b = ConsistentHashRing(vnodes=64)
    for r in (a, b):
        r.set_nodes(["n1", "n2", "n3"])
    keys = [f"key-{i}" for i in range(200)]
    # Two independently built rings with the same membership must agree.
    assert [a.get_node(k) for k in keys] == [b.get_node(k) for k in keys]


def test_balanced_distribution():
    ring = ConsistentHashRing(vnodes=200)
    ring.set_nodes(["n1", "n2", "n3", "n4"])
    counts = Counter(ring.get_node(f"key-{i}") for i in range(8000))
    # No node should own less than half or more than double the fair share.
    fair = 8000 / 4
    for node in ring.nodes:
        assert fair * 0.5 < counts[node] < fair * 2.0


def test_minimal_remap_on_node_removal():
    ring = ConsistentHashRing(vnodes=200)
    ring.set_nodes(["n1", "n2", "n3", "n4"])
    keys = [f"key-{i}" for i in range(4000)]
    before = {k: ring.get_node(k) for k in keys}
    ring.remove_node("n4")
    after = {k: ring.get_node(k) for k in keys}
    moved = sum(1 for k in keys if before[k] != after[k])
    # Only keys previously owned by n4 should move (~1/4), not everything.
    assert moved < len(keys) * 0.45
    # Keys that did not belong to n4 keep their owner.
    for k in keys:
        if before[k] != "n4":
            assert after[k] == before[k]


def test_get_nodes_distinct_and_ordered():
    ring = ConsistentHashRing(vnodes=64)
    ring.set_nodes(["n1", "n2", "n3"])
    nodes = ring.get_nodes("key-42", 2)
    assert len(nodes) == 2
    assert len(set(nodes)) == 2
    assert nodes[0] == ring.get_node("key-42")
