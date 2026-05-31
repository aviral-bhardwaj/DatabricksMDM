"""Deterministic matching: exact agreement on one or more key fields.

Returns prob 1.0 on full agreement of all configured keys (after standardization),
else 0.0. Used for strong identifiers (email, tax id, GTIN).
"""
from __future__ import annotations

from ..model import MatchResult, StandardizedRecord


class DeterministicMatch:
    def __init__(self, keys: list[str], strategy_id: str = "deterministic"):
        if not keys:
            raise ValueError("deterministic match needs at least one key")
        self.keys = keys
        self.strategy_id = strategy_id

    def score(self, a: StandardizedRecord, b: StandardizedRecord) -> MatchResult:
        agree = {}
        all_match = True
        for k in self.keys:
            va, vb = a.get(k), b.get(k)
            match = va is not None and va == vb
            agree[k] = match
            if not match:
                all_match = False
        prob = 1.0 if all_match else 0.0
        return MatchResult(a.record_id, b.record_id, prob, self.strategy_id,
                           {"keys": agree})
