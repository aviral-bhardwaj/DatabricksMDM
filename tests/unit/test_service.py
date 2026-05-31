"""End-to-end on the bundled sample data, exercising the real domain engine."""
from pathlib import Path

from mdm.core.config.loader import load_domain_config
from mdm.core.service import ResolutionService
from mdm.runtime.sample_data import load_sample_raw

ROOT = Path(__file__).resolve().parents[2]


def _service():
    config = load_domain_config(ROOT / "conf" / "domains" / "customer.yml")
    return ResolutionService(config), config


def test_sample_resolves_to_seven_entities():
    service, config = _service()
    raw = load_sample_raw(config)
    out = service.resolve(raw)
    assert out.stats["input_records"] == 16
    assert out.stats["golden_records"] == 7  # Acme,Globex,Initech,Wayne,Umbrella,Stark,Hooli


def test_acme_cluster_unifies_three_sources():
    service, config = _service()
    out = service.resolve(load_sample_raw(config))
    acme = [g for g in out.golden if g.value("name") and "acme" in g.value("name")]
    assert len(acme) == 1
    members = next(c for c in out.clusters if c.cluster_id == acme[0].cluster_id)
    assert members.size == 3
    assert {m.split(":")[0] for m in members.member_ids} == {"sap", "salesforce", "oracle"}


def test_survivorship_provenance_present():
    service, config = _service()
    out = service.resolve(load_sample_raw(config))
    # every golden record's name survivor must carry a winning source + rule
    for g in out.golden:
        name_attr = g.attributes["name"]
        assert name_attr.rule_id == "SOURCE_PRIORITY"
