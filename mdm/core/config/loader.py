"""YAML -> validated DomainConfig."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from .schema import DomainConfig


def load_domain_config_dict(data: dict[str, Any]) -> DomainConfig:
    cfg = DomainConfig(
        domain=data["domain"],
        entity_attributes=data["entity_attributes"],
        field_map=data.get("field_map", {}),
        standardization=data.get("standardization", {}),
        quality_rules=data.get("quality_rules", []),
        blocking=data.get("blocking", []),
        match_strategies=data.get("match_strategies", []),
        survivorship=data.get("survivorship", {}),
        merge_threshold=data.get("merge_threshold", 0.9),
        review_low=data.get("review_low", 0.7),
    )
    return cfg.validate()


def load_domain_config(path: str | Path) -> DomainConfig:
    with open(path, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    return load_domain_config_dict(data)
