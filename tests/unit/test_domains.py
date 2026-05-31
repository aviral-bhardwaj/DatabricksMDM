"""Multi-domain proof: every shipped domain config loads, validates, and builds a
ResolutionService — with NO change to engine code. New domain = new YAML.
"""
from pathlib import Path

import pytest

from mdm.core.config.loader import load_domain_config
from mdm.core.service import ResolutionService
from mdm.runtime.encoders import HashingEncoder

DOMAINS = ["customer", "product", "supplier", "location"]
CONF = Path(__file__).resolve().parents[2] / "conf" / "domains"


@pytest.mark.parametrize("domain", DOMAINS)
def test_domain_config_builds_service(domain):
    config = load_domain_config(CONF / f"{domain}.yml")
    assert config.domain == domain
    # product uses a semantic strategy -> needs an encoder injected
    service = ResolutionService(config, encoder=HashingEncoder())
    assert service.matcher.strategies
