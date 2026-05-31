"""Pure, composable standardization functions. No Spark, no I/O.

Each is a str|None -> str|None transform, so they compose cleanly and are
trivially unit-testable. The config layer maps attribute names to a pipeline of
these by name (Open/Closed: add a function, reference it in YAML, no engine edit).
"""
from __future__ import annotations

import re
import unicodedata
from typing import Optional

_WS = re.compile(r"\s+")
_NON_ALNUM = re.compile(r"[^a-z0-9]+")
_NON_DIGIT = re.compile(r"\D+")

# Common business-name noise tokens, normalized away for matching.
_NAME_NOISE = {
    "inc", "incorporated", "llc", "ltd", "limited", "corp", "corporation",
    "co", "company", "gmbh", "plc", "sa", "ag", "pvt", "pte", "llp",
}


def _strip_accents(value: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFKD", value)
        if not unicodedata.combining(c)
    )


def std_text(value: Optional[str]) -> Optional[str]:
    """Trim, collapse whitespace, strip accents, lowercase."""
    if value is None:
        return None
    value = _strip_accents(str(value)).strip().lower()
    value = _WS.sub(" ", value)
    return value or None


def std_name(value: Optional[str]) -> Optional[str]:
    """Normalize a person/business name: text-normalize then drop legal-suffix
    noise tokens so 'Acme Inc.' and 'ACME' converge."""
    norm = std_text(value)
    if norm is None:
        return None
    tokens = [t for t in _NON_ALNUM.sub(" ", norm).split() if t not in _NAME_NOISE]
    return " ".join(tokens) or None


def std_email(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    value = str(value).strip().lower()
    return value or None


def std_phone(value: Optional[str], default_country: str = "1") -> Optional[str]:
    """Reduce to digits; best-effort E.164-ish without a heavy phone library."""
    if value is None:
        return None
    digits = _NON_DIGIT.sub("", str(value))
    if not digits:
        return None
    if len(digits) == 10:  # NANP local number, prepend default country code
        digits = default_country + digits
    return "+" + digits


# Registry so config can reference functions by name.
FUNCTIONS = {
    "text": std_text,
    "name": std_name,
    "email": std_email,
    "phone": std_phone,
}
