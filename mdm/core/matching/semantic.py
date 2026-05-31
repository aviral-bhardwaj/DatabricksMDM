"""Semantic / AI-assisted matching: cosine similarity over embeddings of a
composed text key.

The embedding model is injected as an Encoder port — in production this is a
served Databricks model or Vector Search; in tests it is a tiny deterministic
stub. The core stays free of any ML dependency. This is the native differentiator:
semantic similarity computed in-place, no data exfiltration.
"""
from __future__ import annotations

import math

from ..model import MatchResult, StandardizedRecord
from ..ports import Encoder


class SemanticMatch:
    def __init__(self, encoder: Encoder, fields: list[str],
                 strategy_id: str = "semantic"):
        if not fields:
            raise ValueError("semantic match needs at least one field")
        self.encoder = encoder
        self.fields = fields
        self.strategy_id = strategy_id

    def _text(self, r: StandardizedRecord) -> str:
        return " ".join(str(r.get(f)) for f in self.fields if r.get(f) is not None)

    def score(self, a: StandardizedRecord, b: StandardizedRecord) -> MatchResult:
        va = self.encoder.encode(self._text(a))
        vb = self.encoder.encode(self._text(b))
        sim = _cosine(va, vb)
        # cosine in [-1,1] -> prob in [0,1]
        prob = max(0.0, min(1.0, (sim + 1.0) / 2.0))
        return MatchResult(a.record_id, b.record_id, round(prob, 6),
                           self.strategy_id, {"cosine": round(sim, 4)})


def _cosine(a: list[float], b: list[float]) -> float:
    if not a or not b:
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)
