from mdm.core.model import RawRecord
from mdm.core.standardize import std_email, std_name, std_phone, std_text
from mdm.core.standardize.engine import ConfigStandardizer


def test_std_name_drops_legal_noise_and_accents():
    assert std_name("Acme Corporation Inc.") == "acme"
    assert std_name("ACME Corp") == "acme"
    assert std_name("Café Söhne GmbH") == "cafe sohne"


def test_std_phone_e164ish():
    assert std_phone("(415) 555-0100") == "+14155550100"
    assert std_phone("4155550100") == "+14155550100"
    assert std_phone("14155550100") == "+14155550100"
    assert std_phone(None) is None


def test_std_email_and_text():
    assert std_email("  Info@ACME.com ") == "info@acme.com"
    assert std_text("  New   York ") == "new york"


def test_config_standardizer_applies_field_map_and_pipelines():
    std = ConfigStandardizer(
        domain="customer",
        pipelines={"name": ["name"], "email": ["email"]},
        field_map={"sap": {"NAME1": "name", "SMTP_ADDR": "email"}},
    )
    raw = RawRecord("sap", "1", {"NAME1": "Acme Inc.", "SMTP_ADDR": "X@Y.COM"})
    sr = std.standardize(raw)
    assert sr.get("name") == "acme"
    assert sr.get("email") == "x@y.com"
    assert sr.domain == "customer"
