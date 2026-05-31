"""Match strategy registry (Open/Closed).

New algorithms register here and are selected by config. Adding a strategy is a
new class + a registry entry + a YAML line — never an edit to a matcher's body
or an if/elif ladder (the v1 anti-pattern).
"""
from __future__ import annotations

from typing import Any, Callable, Optional

from ..ports import Encoder, MatchStrategy
from .deterministic import DeterministicMatch
from .probabilistic import ProbabilisticMatch
from .semantic import SemanticMatch

_BUILDERS: dict[str, Callable[..., MatchStrategy]] = {}


def register(kind: str, builder: Callable[..., MatchStrategy]) -> None:
    _BUILDERS[kind] = builder


def build_strategy(spec: dict[str, Any],
                   encoder: Optional[Encoder] = None) -> MatchStrategy:
    """Construct a strategy from a config spec: {kind: ..., <params>}."""
    kind = spec.get("kind")
    builder = _BUILDERS.get(kind)
    if builder is None:
        raise KeyError(f"unknown match strategy kind: {kind!r} "
                       f"(known: {sorted(_BUILDERS)})")
    return builder(spec, encoder)


class MatchRegistry:
    """Holds an ordered list of strategies; scores a pair with the first strategy
    that yields a decisive result, else the highest-confidence one. Keeps the
    blended decision explainable."""

    def __init__(self, strategies: list[MatchStrategy]):
        if not strategies:
            raise ValueError("at least one strategy required")
        self.strategies = strategies

    def score(self, a, b):
        results = [s.score(a, b) for s in self.strategies]
        # deterministic hit wins outright
        for r in results:
            if r.prob >= 0.999:
                return r
        return max(results, key=lambda r: r.prob)

    @staticmethod
    def from_config(specs: list[dict], encoder: Optional[Encoder] = None
                    ) -> "MatchRegistry":
        return MatchRegistry([build_strategy(s, encoder) for s in specs])


# ----- default builders --------------------------------------------------- #
register("deterministic",
         lambda spec, enc: DeterministicMatch(
             keys=spec["keys"], strategy_id=spec.get("id", "deterministic")))
register("probabilistic",
         lambda spec, enc: ProbabilisticMatch.from_config(spec["comparators"]))


def _build_semantic(spec, enc):
    if enc is None:
        raise ValueError("semantic strategy requires an injected encoder")
    return SemanticMatch(encoder=enc, fields=spec["fields"],
                         strategy_id=spec.get("id", "semantic"))


register("semantic", _build_semantic)
