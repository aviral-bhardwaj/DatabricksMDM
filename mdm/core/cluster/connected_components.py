"""Transitive-closure clustering via union-find (implements ClusterPolicy).

This fixes the v1 bug where clusters were assigned with monotonically_increasing_id,
which is NOT transitive: given matches A-B and B-C, v1 could emit two clusters.
Union-find guarantees A, B, C land in ONE cluster.

A match contributes an edge only if its probability meets the merge threshold.
Gray-zone pairs (review_low <= prob < merge) are reported as review edges so a
StewardshipCase can be opened instead of auto-merging.
"""
from __future__ import annotations

import hashlib
from typing import Iterable

from ..model import Cluster, MatchResult


class _UnionFind:
    def __init__(self) -> None:
        self.parent: dict[str, str] = {}
        self.rank: dict[str, int] = {}

    def add(self, x: str) -> None:
        if x not in self.parent:
            self.parent[x] = x
            self.rank[x] = 0

    def find(self, x: str) -> str:
        self.add(x)
        root = x
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[x] != root:  # path compression
            self.parent[x], x = root, self.parent[x]
        return root

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        if self.rank[ra] < self.rank[rb]:
            ra, rb = rb, ra
        self.parent[rb] = ra
        if self.rank[ra] == self.rank[rb]:
            self.rank[ra] += 1


class ConnectedComponents:
    def __init__(self, merge_threshold: float = 0.9, review_low: float = 0.7):
        if not 0 <= review_low <= merge_threshold <= 1:
            raise ValueError("require 0 <= review_low <= merge_threshold <= 1")
        self.merge_threshold = merge_threshold
        self.review_low = review_low

    def cluster(self, matches: Iterable[MatchResult]) -> list[Cluster]:
        uf = _UnionFind()
        confidences: dict[frozenset[str], float] = {}
        review_members: set[str] = set()

        materialized = list(matches)
        for m in materialized:
            uf.add(m.record_a)
            uf.add(m.record_b)
            if m.prob >= self.merge_threshold:
                uf.union(m.record_a, m.record_b)
                key = frozenset((m.record_a, m.record_b))
                confidences[key] = max(confidences.get(key, 0.0), m.prob)
            elif m.prob >= self.review_low:
                review_members.update((m.record_a, m.record_b))

        groups: dict[str, list[str]] = {}
        for node in uf.parent:
            groups.setdefault(uf.find(node), []).append(node)

        clusters: list[Cluster] = []
        for members in groups.values():
            members_sorted = sorted(members)
            cid = _cluster_id(members_sorted)
            edge_confs = [
                c for k, c in confidences.items() if k <= set(members_sorted)
            ]
            confidence = min(edge_confs) if edge_confs else 1.0
            status = "review" if any(m in review_members for m in members_sorted) \
                and len(members_sorted) == 1 else "auto"
            clusters.append(Cluster(
                cluster_id=cid,
                member_ids=members_sorted,
                confidence=round(confidence, 6),
                status=status,
            ))
        return clusters


def _cluster_id(members: list[str]) -> str:
    h = hashlib.md5("|".join(members).encode()).hexdigest()[:16]
    return f"clu_{h}"
