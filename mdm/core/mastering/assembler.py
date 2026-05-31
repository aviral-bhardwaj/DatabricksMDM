"""GoldenRecordAssembler — builds a GoldenRecord for a cluster by running the
survivorship resolver over every configured attribute, then computing an overall
trust score. Pure: cluster + records in, golden record out.
"""
from __future__ import annotations

import hashlib

from ..model import Cluster, GoldenRecord, StandardizedRecord
from .survivorship import SurvivorshipResolver


class GoldenRecordAssembler:
    def __init__(self, domain: str, resolver: SurvivorshipResolver,
                 attributes: list[str]):
        self.domain = domain
        self.resolver = resolver
        self.attributes = attributes

    def assemble(self, cluster: Cluster,
                 records_by_id: dict[str, StandardizedRecord]) -> GoldenRecord:
        members = [records_by_id[m] for m in cluster.member_ids
                   if m in records_by_id]
        survivors = {
            attr: self.resolver.survive_attribute(attr, members)
            for attr in self.attributes
        }
        trusts = [s.trust for s in survivors.values() if s.value is not None]
        trust_score = sum(trusts) / len(trusts) if trusts else 0.0
        return GoldenRecord(
            entity_id=_entity_id(self.domain, cluster.cluster_id),
            domain=self.domain,
            cluster_id=cluster.cluster_id,
            attributes=survivors,
            trust_score=round(trust_score, 4),
        )


def _entity_id(domain: str, cluster_id: str) -> str:
    h = hashlib.md5(f"{domain}:{cluster_id}".encode()).hexdigest()[:16]
    return f"{domain[:3]}_{h}"
