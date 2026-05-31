"""Config-driven standardizer (implements the Standardizer port).

A standardization policy is data: {attribute: [function_name, ...]}. The engine
applies the named pipeline to each attribute. Source-specific field names are
mapped to canonical names first (fixes v1's per-source hardcoding).
"""
from __future__ import annotations

from typing import Any, Optional

from ..model import RawRecord, StandardizedRecord
from .functions import FUNCTIONS


def normalize(value: Any, pipeline: list[str]) -> Any:
    """Apply a named function pipeline to a single value."""
    out = value
    for fn_name in pipeline:
        fn = FUNCTIONS.get(fn_name)
        if fn is None:
            raise KeyError(f"unknown standardization function: {fn_name!r}")
        out = fn(out)
    return out


class ConfigStandardizer:
    """Standardizer driven entirely by a domain config.

    field_map:   source attribute name -> canonical attribute name
    pipelines:   canonical attribute name -> [function names]
    """

    def __init__(
        self,
        domain: str,
        pipelines: dict[str, list[str]],
        field_map: Optional[dict[str, dict[str, str]]] = None,
    ) -> None:
        self.domain = domain
        self.pipelines = pipelines
        self.field_map = field_map or {}

    def _canonicalize(self, record: RawRecord) -> dict[str, Any]:
        mapping = self.field_map.get(record.source_system, {})
        if not mapping:
            return dict(record.attributes)
        out: dict[str, Any] = {}
        for src_name, value in record.attributes.items():
            out[mapping.get(src_name, src_name)] = value
        return out

    def standardize(self, record: RawRecord) -> StandardizedRecord:
        canonical = self._canonicalize(record)
        attrs: dict[str, Any] = {}
        for name, value in canonical.items():
            pipeline = self.pipelines.get(name)
            attrs[name] = normalize(value, pipeline) if pipeline else value
        return StandardizedRecord(
            record_id=record.record_id,
            source_system=record.source_system,
            domain=self.domain,
            attributes=attrs,
        )
