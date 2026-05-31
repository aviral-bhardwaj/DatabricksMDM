"""Config-driven blocking (implements BlockingStrategy).

v1 hardcoded the blocking key to "country". Here the block key(s) are config:
a list of "blocking specs", each a list of fields combined into a key, with an
optional prefix length so e.g. first 4 chars of a normalized name can block.
Blocking turns O(n^2) comparison into O(n * b).
"""
from __future__ import annotations

from dataclasses import dataclass

from ..model import StandardizedRecord


@dataclass
class BlockSpec:
    fields: list[str]
    prefix: int | None = None  # take first N chars of each field's value


class ConfigBlocking:
    def __init__(self, specs: list[BlockSpec]):
        if not specs:
            raise ValueError("at least one blocking spec is required")
        self.specs = specs

    def keys(self, record: StandardizedRecord) -> list[str]:
        out: list[str] = []
        for i, spec in enumerate(self.specs):
            parts: list[str] = []
            ok = True
            for fname in spec.fields:
                val = record.get(fname)
                if val is None:
                    ok = False
                    break
                sval = str(val)
                if spec.prefix:
                    sval = sval[: spec.prefix]
                parts.append(sval)
            if ok:
                out.append(f"b{i}|" + "|".join(parts))
        return out

    @staticmethod
    def from_config(specs: list[dict]) -> "ConfigBlocking":
        return ConfigBlocking([
            BlockSpec(fields=s["fields"], prefix=s.get("prefix")) for s in specs
        ])
