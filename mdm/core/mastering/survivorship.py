"""Survivorship as rules-as-data (ported & purified from v1's GoldenRecordBuilder
+ AdvancedSurvivorshipRules, with Spark removed).

Each strategy is a pure function: (attribute, records, params) -> (value, source,
contributing_ids). The resolver picks the strategy named in config per attribute.
Because rules are data, they can be versioned, A/B-tested, and replayed against
history via Delta time travel in the runtime layer.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Callable

from ..model import StandardizedRecord, SurvivorAttribute

# A strategy returns (value, winning_source, contributing_record_ids, trust)
StrategyResult = tuple[Any, str, list[str], float]
Strategy = Callable[[str, list[StandardizedRecord], dict], StrategyResult]


def _non_null(attr: str, records: list[StandardizedRecord]):
    return [r for r in records if r.get(attr) is not None]


def _recency(r: StandardizedRecord) -> datetime:
    return r.standardized_at


def most_recent(attr, records, params) -> StrategyResult:
    cands = _non_null(attr, records)
    if not cands:
        return None, "", [], 0.0
    winner = max(cands, key=_recency)
    return winner.get(attr), winner.source_system, [winner.record_id], 0.9


def source_priority(attr, records, params) -> StrategyResult:
    """params['priority'] = {source: rank} (lower rank = higher priority)."""
    priority: dict[str, int] = params.get("priority", {})
    cands = _non_null(attr, records)
    if not cands:
        return None, "", [], 0.0
    winner = min(cands, key=lambda r: (priority.get(r.source_system, 10_000),
                                       -_recency(r).timestamp()))
    trust = 1.0 - priority.get(winner.source_system, 10_000) / 10_000.0
    return winner.get(attr), winner.source_system, [winner.record_id], max(trust, 0.5)


def most_complete(attr, records, params) -> StrategyResult:
    """Pick the value from the record with the most populated fields overall."""
    cands = _non_null(attr, records)
    if not cands:
        return None, "", [], 0.0
    def completeness(r: StandardizedRecord) -> int:
        return sum(1 for v in r.attributes.values() if v is not None)
    winner = max(cands, key=completeness)
    return winner.get(attr), winner.source_system, [winner.record_id], 0.8


def longest(attr, records, params) -> StrategyResult:
    cands = _non_null(attr, records)
    if not cands:
        return None, "", [], 0.0
    winner = max(cands, key=lambda r: len(str(r.get(attr))))
    return winner.get(attr), winner.source_system, [winner.record_id], 0.7


def most_frequent(attr, records, params) -> StrategyResult:
    """Plurality vote; ties broken by recency. Trust scales with agreement."""
    cands = _non_null(attr, records)
    if not cands:
        return None, "", [], 0.0
    counts: dict[Any, list[StandardizedRecord]] = {}
    for r in cands:
        counts.setdefault(r.get(attr), []).append(r)
    value, supporters = max(
        counts.items(),
        key=lambda kv: (len(kv[1]), max(_recency(r) for r in kv[1])),
    )
    trust = len(supporters) / len(cands)
    return value, supporters[0].source_system, [r.record_id for r in supporters], trust


def trust_decay(attr, records, params) -> StrategyResult:
    """Source trust attenuated by age (half-life in days). Highest effective
    trust wins. A principled blend of source_priority + most_recent."""
    weights: dict[str, float] = params.get("source_trust", {})
    half_life = float(params.get("half_life_days", 365.0))
    asof = params.get("_asof") or max((_recency(r) for r in records),
                                      default=datetime.utcnow())
    cands = _non_null(attr, records)
    if not cands:
        return None, "", [], 0.0

    def effective(r: StandardizedRecord) -> float:
        base = weights.get(r.source_system, 0.5)
        age_days = max((asof - _recency(r)).total_seconds() / 86400.0, 0.0)
        return base * (0.5 ** (age_days / half_life))

    winner = max(cands, key=effective)
    return winner.get(attr), winner.source_system, [winner.record_id], \
        round(effective(winner), 4)


STRATEGIES: dict[str, Strategy] = {
    "MOST_RECENT": most_recent,
    "SOURCE_PRIORITY": source_priority,
    "MOST_COMPLETE": most_complete,
    "LONGEST": longest,
    "MOST_FREQUENT": most_frequent,
    "TRUST_DECAY": trust_decay,
}


class SurvivorshipResolver:
    """Resolves one surviving attribute according to a config rule."""

    def __init__(self, rules: dict[str, dict]):
        """rules: {attribute: {"strategy": NAME, **params}}"""
        self.rules = rules

    def survive_attribute(self, attr: str,
                          records: list[StandardizedRecord]) -> SurvivorAttribute:
        rule = self.rules.get(attr, {"strategy": "MOST_RECENT"})
        name = rule.get("strategy", "MOST_RECENT")
        strategy = STRATEGIES.get(name)
        if strategy is None:
            raise KeyError(f"unknown survivorship strategy: {name!r}")
        value, source, contributing, trust = strategy(attr, records, rule)
        return SurvivorAttribute(
            name=attr, value=value, source_system=source,
            rule_id=name, trust=trust, contributing_records=contributing,
        )
