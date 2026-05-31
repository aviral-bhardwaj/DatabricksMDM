"""Typed domain configuration + validation.

A domain is data, not code (Open/Closed for new domains). This dataclass is the
single validated shape the engine consumes; the loader turns YAML into it and
fails fast on mistakes rather than deep in a pipeline.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class DomainConfig:
    domain: str
    entity_attributes: list[str]
    field_map: dict[str, dict[str, str]] = field(default_factory=dict)
    standardization: dict[str, list[str]] = field(default_factory=dict)
    quality_rules: list[dict[str, Any]] = field(default_factory=list)
    blocking: list[dict[str, Any]] = field(default_factory=list)
    match_strategies: list[dict[str, Any]] = field(default_factory=list)
    survivorship: dict[str, dict[str, Any]] = field(default_factory=dict)
    merge_threshold: float = 0.9
    review_low: float = 0.7

    def validate(self) -> "DomainConfig":
        errors: list[str] = []
        if not self.domain:
            errors.append("domain name is required")
        if not self.entity_attributes:
            errors.append("entity_attributes must be non-empty")
        if not self.blocking:
            errors.append("at least one blocking spec is required")
        if not self.match_strategies:
            errors.append("at least one match strategy is required")
        if not 0 <= self.review_low <= self.merge_threshold <= 1:
            errors.append("require 0 <= review_low <= merge_threshold <= 1")
        # survivorship attributes must be known
        unknown = set(self.survivorship) - set(self.entity_attributes)
        if unknown:
            errors.append(f"survivorship references unknown attributes: {unknown}")
        if errors:
            raise ValueError(
                f"invalid config for domain {self.domain!r}: " + "; ".join(errors))
        return self
