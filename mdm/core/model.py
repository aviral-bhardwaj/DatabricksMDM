"""Canonical domain entities (blueprint section E).

Plain dataclasses — no infrastructure, no Spark. These are the ubiquitous
language of the product: every zone speaks in these terms.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional


def _now() -> datetime:
    return datetime.now(timezone.utc)


# --------------------------------------------------------------------------- #
# Ingestion / quality tiers
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class RawRecord:
    """Verbatim source record as landed in Bronze (append-only)."""

    source_system: str
    source_pk: str
    attributes: dict[str, Any]
    ingested_at: datetime = field(default_factory=_now)
    batch_id: Optional[str] = None

    @property
    def record_id(self) -> str:
        return f"{self.source_system}:{self.source_pk}"


class QualityTier(str, Enum):
    GOLD = "GOLD"
    SILVER = "SILVER"
    BRONZE = "BRONZE"


@dataclass
class QualityVerdict:
    """Outcome of running quality rules against a record."""

    score: float
    tier: QualityTier
    failed_checks: list[str] = field(default_factory=list)

    @staticmethod
    def from_score(score: float, failed: list[str], gold: float = 0.9,
                   silver: float = 0.7) -> "QualityVerdict":
        tier = (QualityTier.GOLD if score >= gold
                else QualityTier.SILVER if score >= silver
                else QualityTier.BRONZE)
        return QualityVerdict(score=round(score, 4), tier=tier, failed_checks=failed)


@dataclass
class StandardizedRecord:
    """Cleaned, validated, conformed record (Silver)."""

    record_id: str
    source_system: str
    domain: str
    attributes: dict[str, Any]
    quality: Optional[QualityVerdict] = None
    standardized_at: datetime = field(default_factory=_now)

    def get(self, name: str) -> Any:
        return self.attributes.get(name)


# --------------------------------------------------------------------------- #
# Matching / clustering (Master/Trust tier)
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class MatchResult:
    """Calibrated outcome of comparing two records. Liskov contract: every
    MatchStrategy returns this exact shape with prob in [0, 1]."""

    record_a: str
    record_b: str
    prob: float
    strategy_id: str
    explanation: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not 0.0 <= self.prob <= 1.0:
            raise ValueError(f"prob must be in [0,1], got {self.prob}")


@dataclass(frozen=True)
class MatchCandidate:
    """A pair surfaced by blocking, before/with scoring."""

    record_a: str
    record_b: str
    block_key: str


@dataclass
class Cluster:
    """A set of records resolved to the same real-world entity."""

    cluster_id: str
    member_ids: list[str]
    confidence: float = 0.0
    status: str = "auto"  # auto | review | confirmed

    @property
    def size(self) -> int:
        return len(self.member_ids)


# --------------------------------------------------------------------------- #
# Mastering (golden records)
# --------------------------------------------------------------------------- #
@dataclass
class SurvivorAttribute:
    """A single surviving attribute value with its lineage and trust."""

    name: str
    value: Any
    source_system: str
    rule_id: str
    trust: float = 0.0
    contributing_records: list[str] = field(default_factory=list)
    overridden_by: Optional[str] = None  # steward id, if a manual override won


@dataclass
class GoldenRecord:
    """The surviving truth about a resolved entity (Master/Trust tier)."""

    entity_id: str
    domain: str
    cluster_id: str
    attributes: dict[str, SurvivorAttribute]
    trust_score: float = 0.0
    version: int = 1
    valid_from: datetime = field(default_factory=_now)
    valid_to: Optional[datetime] = None
    is_current: bool = True

    def value(self, name: str) -> Any:
        attr = self.attributes.get(name)
        return attr.value if attr else None

    def as_flat_dict(self) -> dict[str, Any]:
        d = {"entity_id": self.entity_id, "domain": self.domain,
             "cluster_id": self.cluster_id, "trust_score": round(self.trust_score, 4),
             "version": self.version}
        d.update({k: v.value for k, v in self.attributes.items()})
        return d


# --------------------------------------------------------------------------- #
# Stewardship / governance / publishing
# --------------------------------------------------------------------------- #
class CaseState(str, Enum):
    OPEN = "OPEN"
    IN_REVIEW = "IN_REVIEW"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"


@dataclass
class StewardshipCase:
    case_id: str
    cluster_id: str
    case_type: str  # merge | unmerge | override | exception
    state: CaseState = CaseState.OPEN
    assignee: Optional[str] = None
    reason: str = ""
    decisions: list[dict[str, Any]] = field(default_factory=list)
    opened_at: datetime = field(default_factory=_now)


@dataclass(frozen=True)
class PublishEvent:
    entity_id: str
    domain: str
    change_type: str  # insert | update | delete
    version: int
    emitted_at: datetime = field(default_factory=_now)


@dataclass(frozen=True)
class PolicyRule:
    rule_id: str
    scope: str       # catalog.schema.table[.column]
    kind: str        # access | mask | tag | retention
    expression: str
