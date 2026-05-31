import pytest

from mdm.core.config.loader import load_domain_config_dict


def _base():
    return {
        "domain": "customer",
        "entity_attributes": ["name", "email"],
        "blocking": [{"fields": ["email"]}],
        "match_strategies": [{"kind": "deterministic", "keys": ["email"]}],
        "survivorship": {"name": {"strategy": "MOST_RECENT"}},
    }


def test_valid_config_loads():
    cfg = load_domain_config_dict(_base())
    assert cfg.domain == "customer"


def test_survivorship_unknown_attribute_rejected():
    data = _base()
    data["survivorship"]["bogus"] = {"strategy": "MOST_RECENT"}
    with pytest.raises(ValueError):
        load_domain_config_dict(data)


def test_threshold_ordering_enforced():
    data = _base()
    data["merge_threshold"] = 0.5
    data["review_low"] = 0.8
    with pytest.raises(ValueError):
        load_domain_config_dict(data)


def test_missing_strategies_rejected():
    data = _base()
    data["match_strategies"] = []
    with pytest.raises(ValueError):
        load_domain_config_dict(data)
