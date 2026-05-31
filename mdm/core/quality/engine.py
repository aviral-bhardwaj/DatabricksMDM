"""Config-driven data-quality engine (implements QualityRule for a whole domain).

v1 hardcoded customer email/phone/uniqueness checks in an if/elif. Here every
rule is declarative data: type + field + params + weight, applied to any domain.
The score is a weighted pass-rate; the tier is GOLD/SILVER/BRONZE (kept from v1).
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from ..model import QualityVerdict, StandardizedRecord

_RULE_TYPES = {"not_null", "regex", "range", "value_set"}


@dataclass
class Rule:
    field: str
    kind: str
    weight: float = 1.0
    pattern: str | None = None
    min: float | None = None
    max: float | None = None
    values: list[Any] | None = None

    def __post_init__(self) -> None:
        if self.kind not in _RULE_TYPES:
            raise ValueError(f"unknown quality rule type: {self.kind!r}")
        self._rx = re.compile(self.pattern) if self.pattern else None

    def passes(self, record: StandardizedRecord) -> bool:
        value = record.get(self.field)
        if self.kind == "not_null":
            return value is not None and value != ""
        if value is None:
            return False  # other checks require a value to evaluate
        if self.kind == "regex":
            return bool(self._rx.match(str(value)))
        if self.kind == "range":
            try:
                v = float(value)
            except (TypeError, ValueError):
                return False
            if self.min is not None and v < self.min:
                return False
            if self.max is not None and v > self.max:
                return False
            return True
        if self.kind == "value_set":
            return value in (self.values or [])
        return False  # pragma: no cover


class QualityEngine:
    """Evaluates a list of declarative rules and returns a scored verdict."""

    def __init__(self, rules: list[Rule], gold: float = 0.9, silver: float = 0.7):
        self.rules = rules
        self.gold = gold
        self.silver = silver

    def evaluate(self, record: StandardizedRecord) -> QualityVerdict:
        if not self.rules:
            return QualityVerdict.from_score(1.0, [])
        total = sum(r.weight for r in self.rules)
        earned = 0.0
        failed: list[str] = []
        for rule in self.rules:
            if rule.passes(record):
                earned += rule.weight
            else:
                failed.append(f"{rule.field}:{rule.kind}")
        score = earned / total if total else 1.0
        return QualityVerdict.from_score(score, failed, self.gold, self.silver)
