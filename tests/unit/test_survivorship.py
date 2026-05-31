from datetime import datetime, timedelta, timezone

from mdm.core.mastering.survivorship import SurvivorshipResolver
from mdm.core.model import StandardizedRecord


def _rec(source, attrs, ts):
    return StandardizedRecord(f"{source}:1", source, "customer", attrs,
                              standardized_at=ts)


def test_source_priority_picks_highest_rank():
    t = datetime(2024, 1, 1, tzinfo=timezone.utc)
    recs = [
        _rec("odoo", {"name": "acme odoo"}, t),
        _rec("sap", {"name": "acme sap"}, t),
    ]
    resolver = SurvivorshipResolver(
        {"name": {"strategy": "SOURCE_PRIORITY", "priority": {"sap": 1, "odoo": 4}}})
    surv = resolver.survive_attribute("name", recs)
    assert surv.value == "acme sap"
    assert surv.source_system == "sap"


def test_most_recent_picks_latest():
    old = datetime(2023, 1, 1, tzinfo=timezone.utc)
    new = old + timedelta(days=100)
    recs = [_rec("a", {"phone": "111"}, old), _rec("b", {"phone": "222"}, new)]
    resolver = SurvivorshipResolver({"phone": {"strategy": "MOST_RECENT"}})
    assert resolver.survive_attribute("phone", recs).value == "222"


def test_most_frequent_majority_vote_and_trust():
    t = datetime(2024, 1, 1, tzinfo=timezone.utc)
    recs = [
        _rec("a", {"country": "us"}, t),
        _rec("b", {"country": "us"}, t),
        _rec("c", {"country": "usa"}, t),
    ]
    resolver = SurvivorshipResolver({"country": {"strategy": "MOST_FREQUENT"}})
    surv = resolver.survive_attribute("country", recs)
    assert surv.value == "us"
    assert surv.trust == 2 / 3


def test_trust_decay_prefers_trusted_recent_source():
    now = datetime(2024, 6, 1, tzinfo=timezone.utc)
    recs = [
        _rec("sap", {"phone": "fresh-trusted"}, now),
        _rec("odoo", {"phone": "old-weak"}, now - timedelta(days=400)),
    ]
    resolver = SurvivorshipResolver({"phone": {
        "strategy": "TRUST_DECAY",
        "source_trust": {"sap": 0.9, "odoo": 0.4},
        "half_life_days": 180,
        "_asof": now,
    }})
    assert resolver.survive_attribute("phone", recs).value == "fresh-trusted"
