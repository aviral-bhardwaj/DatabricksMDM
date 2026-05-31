from mdm.core.model import QualityTier, StandardizedRecord
from mdm.core.quality.engine import QualityEngine, Rule


def _rec(attrs):
    return StandardizedRecord("s:1", "s", "customer", attrs)


def test_gold_when_all_rules_pass():
    engine = QualityEngine([
        Rule(field="name", kind="not_null", weight=2),
        Rule(field="email", kind="regex",
             pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$", weight=2),
    ])
    v = engine.evaluate(_rec({"name": "acme", "email": "a@b.com"}))
    assert v.tier == QualityTier.GOLD
    assert v.score == 1.0
    assert v.failed_checks == []


def test_weighted_score_and_failed_checks():
    engine = QualityEngine([
        Rule(field="name", kind="not_null", weight=3),
        Rule(field="email", kind="not_null", weight=1),
    ])
    v = engine.evaluate(_rec({"name": "acme", "email": None}))
    assert v.score == 0.75  # 3 of 4 weight
    assert "email:not_null" in v.failed_checks


def test_unknown_rule_type_rejected():
    import pytest
    with pytest.raises(ValueError):
        Rule(field="x", kind="bogus")
