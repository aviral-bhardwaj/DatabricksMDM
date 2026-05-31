"""The bug-fix test: clustering MUST be transitive.

v1 used monotonically_increasing_id and could split A-B-C into multiple clusters.
Union-find guarantees one component.
"""
from mdm.core.cluster.connected_components import ConnectedComponents
from mdm.core.model import MatchResult


def _m(a, b, p):
    return MatchResult(a, b, p, "test")


def test_transitive_closure_single_cluster():
    # A~B and B~C, but A and C never directly compared -> still ONE cluster.
    matches = [_m("A", "B", 0.97), _m("B", "C", 0.96)]
    clusters = ConnectedComponents(merge_threshold=0.9).cluster(matches)
    big = [c for c in clusters if c.size > 1]
    assert len(big) == 1
    assert set(big[0].member_ids) == {"A", "B", "C"}


def test_below_threshold_does_not_merge():
    matches = [_m("A", "B", 0.5)]
    clusters = ConnectedComponents(merge_threshold=0.9, review_low=0.7).cluster(matches)
    assert all(c.size == 1 for c in clusters)


def test_separate_components_stay_separate():
    matches = [_m("A", "B", 0.95), _m("C", "D", 0.95)]
    clusters = ConnectedComponents(merge_threshold=0.9).cluster(matches)
    sizes = sorted(c.size for c in clusters)
    assert sizes == [2, 2]


def test_review_zone_flags_status():
    matches = [_m("X", "Y", 0.75)]  # between review_low and merge
    clusters = ConnectedComponents(merge_threshold=0.9, review_low=0.7).cluster(matches)
    assert all(c.size == 1 for c in clusters)
    assert any(c.status == "review" for c in clusters)
