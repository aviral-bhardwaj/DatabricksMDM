"""Probabilistic/fuzzy matching: a weighted blend of per-field similarities,
calibrated into a probability.

Config: a list of field comparators (field, method, weight). Methods map to the
pure similarity functions. The weighted average is the calibrated prob — already
in [0,1], comparable across strategies (Liskov: same score semantics as the
deterministic and semantic strategies).
"""
from __future__ import annotations

from dataclasses import dataclass

from ..model import MatchResult, StandardizedRecord
from .similarity import jaro_winkler, token_set_ratio

_METHODS = {
    "jaro_winkler": jaro_winkler,
    "token_set": token_set_ratio,
}


@dataclass
class FieldComparator:
    field: str
    method: str = "jaro_winkler"
    weight: float = 1.0

    def compare(self, a, b) -> float:
        fn = _METHODS.get(self.method)
        if fn is None:
            raise KeyError(f"unknown comparator method: {self.method!r}")
        return fn(_as_str(a), _as_str(b))


def _as_str(v):
    return None if v is None else str(v)


class ProbabilisticMatch:
    def __init__(self, comparators: list[FieldComparator],
                 strategy_id: str = "probabilistic"):
        if not comparators:
            raise ValueError("probabilistic match needs at least one comparator")
        self.comparators = comparators
        self.strategy_id = strategy_id

    def score(self, a: StandardizedRecord, b: StandardizedRecord) -> MatchResult:
        total = sum(c.weight for c in self.comparators)
        acc = 0.0
        detail = {}
        for c in self.comparators:
            sim = c.compare(a.get(c.field), b.get(c.field))
            detail[c.field] = round(sim, 4)
            acc += sim * c.weight
        prob = acc / total if total else 0.0
        return MatchResult(a.record_id, b.record_id, round(prob, 6),
                           self.strategy_id, {"field_sim": detail})

    @staticmethod
    def from_config(comparators: list[dict]) -> "ProbabilisticMatch":
        return ProbabilisticMatch([
            FieldComparator(field=c["field"], method=c.get("method", "jaro_winkler"),
                            weight=c.get("weight", 1.0))
            for c in comparators
        ])
