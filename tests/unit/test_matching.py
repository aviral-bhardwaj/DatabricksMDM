import pytest

from mdm.core.matching import jaro_winkler, token_set_ratio
from mdm.core.matching.registry import MatchRegistry, build_strategy
from mdm.core.model import MatchResult, StandardizedRecord
from mdm.runtime.encoders import HashingEncoder


def _r(rid, **attrs):
    return StandardizedRecord(rid, rid.split(":")[0], "customer", attrs)


def test_similarity_bounds():
    assert jaro_winkler("acme", "acme") == 1.0
    assert 0.0 <= jaro_winkler("acme", "acmd") < 1.0
    assert token_set_ratio("san francisco", "francisco san") == 1.0


def test_deterministic_matches_on_key():
    s = build_strategy({"kind": "deterministic", "keys": ["email"]})
    a = _r("sap:1", email="x@y.com")
    b = _r("ora:1", email="x@y.com")
    assert s.score(a, b).prob == 1.0
    assert s.score(a, _r("ora:2", email="z@y.com")).prob == 0.0


def test_probabilistic_blend_and_bounds():
    s = build_strategy({"kind": "probabilistic", "comparators": [
        {"field": "name", "method": "jaro_winkler", "weight": 3},
        {"field": "city", "method": "token_set", "weight": 1},
    ]})
    res = s.score(_r("sap:1", name="acme", city="san francisco"),
                  _r("ora:1", name="acme", city="san francisco"))
    assert res.prob == pytest.approx(1.0)
    assert isinstance(res, MatchResult)


def test_registry_prefers_decisive_then_max():
    reg = MatchRegistry.from_config([
        {"kind": "deterministic", "keys": ["email"]},
        {"kind": "probabilistic", "comparators": [
            {"field": "name", "method": "jaro_winkler", "weight": 1}]},
    ])
    # different email, same name -> probabilistic wins via max
    res = reg.score(_r("sap:1", email="a@x.com", name="acme"),
                    _r("ora:1", email="b@x.com", name="acme"))
    assert res.prob == pytest.approx(1.0)


def test_semantic_strategy_uses_injected_encoder():
    reg = MatchRegistry.from_config(
        [{"kind": "semantic", "fields": ["name"]}], encoder=HashingEncoder())
    same = reg.score(_r("sap:1", name="acme corporation"),
                     _r("ora:1", name="acme corporation"))
    assert same.prob > 0.9


def test_unknown_strategy_raises():
    with pytest.raises(KeyError):
        build_strategy({"kind": "nope"})
