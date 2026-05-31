"""ResolutionService — the domain composition root.

Wires standardization -> quality -> blocking -> matching -> clustering ->
survivorship into one flow, operating on plain lists. It is PURE: no Spark, no
I/O. The Spark pipelines (mdm.pipelines) reuse these exact components distributed
across a cluster; the demo and tests run them on a list in-process. Same logic,
two runtimes — the payoff of Dependency Inversion.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from itertools import combinations
from typing import Iterable, Optional

from .cluster.connected_components import ConnectedComponents
from .config.schema import DomainConfig
from .mastering.assembler import GoldenRecordAssembler
from .mastering.survivorship import SurvivorshipResolver
from .matching.blocking import ConfigBlocking
from .matching.registry import MatchRegistry
from .model import (
    Cluster,
    GoldenRecord,
    MatchResult,
    QualityVerdict,
    RawRecord,
    StandardizedRecord,
)
from .ports import Encoder
from .quality.engine import QualityEngine, Rule
from .standardize.engine import ConfigStandardizer


@dataclass
class ResolutionOutput:
    standardized: list[StandardizedRecord]
    matches: list[MatchResult]
    clusters: list[Cluster]
    golden: list[GoldenRecord]
    stats: dict = field(default_factory=dict)


class ResolutionService:
    def __init__(self, config: DomainConfig, encoder: Optional[Encoder] = None):
        self.config = config.validate()
        self.standardizer = ConfigStandardizer(
            domain=config.domain,
            pipelines=config.standardization,
            field_map=config.field_map,
        )
        self.quality = QualityEngine([Rule(**r) for r in config.quality_rules])
        self.blocking = ConfigBlocking.from_config(config.blocking)
        self.matcher = MatchRegistry.from_config(config.match_strategies, encoder)
        self.clusterer = ConnectedComponents(
            merge_threshold=config.merge_threshold,
            review_low=config.review_low,
        )
        self.assembler = GoldenRecordAssembler(
            domain=config.domain,
            resolver=SurvivorshipResolver(config.survivorship),
            attributes=config.entity_attributes,
        )

    # -- stages ----------------------------------------------------------- #
    def standardize(self, raw: Iterable[RawRecord]) -> list[StandardizedRecord]:
        out = []
        for r in raw:
            sr = self.standardizer.standardize(r)
            sr.quality = self.quality.evaluate(sr)
            out.append(sr)
        return out

    def candidate_pairs(self, records: list[StandardizedRecord]):
        """Blocking: only compare records that share a block key (O(n*b))."""
        buckets: dict[str, list[StandardizedRecord]] = {}
        for rec in records:
            for key in self.blocking.keys(rec):
                buckets.setdefault(key, []).append(rec)
        seen: set[tuple[str, str]] = set()
        for bucket in buckets.values():
            for a, b in combinations(bucket, 2):
                pair = tuple(sorted((a.record_id, b.record_id)))
                if pair not in seen:
                    seen.add(pair)
                    yield a, b

    def match(self, records: list[StandardizedRecord]) -> list[MatchResult]:
        return [self.matcher.score(a, b) for a, b in self.candidate_pairs(records)]

    # -- full flow -------------------------------------------------------- #
    def resolve(self, raw: Iterable[RawRecord]) -> ResolutionOutput:
        return self.resolve_from_standardized(self.standardize(raw))

    def resolve_from_standardized(
            self, standardized: list[StandardizedRecord]) -> ResolutionOutput:
        by_id = {r.record_id: r for r in standardized}
        matches = self.match(standardized)
        clusters = self.clusterer.cluster(matches)

        # ensure singletons (no matches at all) still become clusters
        clustered_ids = {m for c in clusters for m in c.member_ids}
        for rid in by_id:
            if rid not in clustered_ids:
                clusters.append(Cluster(cluster_id=f"clu_{rid}",
                                        member_ids=[rid], confidence=1.0))

        golden = [self.assembler.assemble(c, by_id) for c in clusters]
        stats = {
            "input_records": len(standardized),
            "candidate_pairs": len(matches),
            "auto_merges": sum(1 for m in matches if m.prob >= self.config.merge_threshold),
            "clusters": len(clusters),
            "golden_records": len(golden),
            "dedup_ratio": round(1 - len(golden) / len(standardized), 4)
            if standardized else 0.0,
        }
        return ResolutionOutput(standardized, matches, clusters, golden, stats)
