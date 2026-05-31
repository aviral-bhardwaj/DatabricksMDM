"""Unity Catalog operations — bootstrap, tagging, masking, audit.

SECURITY: every identifier (catalog/schema/table/column) is validated against a
strict allowlist and back-quoted before interpolation. This fixes the v1
SQL-injection vulnerabilities (AuditTrailManager.get_entity_history /
get_user_activity, LineageTracker._get_*_lineage, MatchReviewManager), which
interpolated raw user input straight into SQL. Values (not identifiers) are bound
as parameters, never string-formatted.

Adopt-don't-build: we apply UC grants/masks/tags rather than re-implementing a
security model. That is the native posture (blueprint section C).
"""
from __future__ import annotations

import re
from typing import Any, Optional

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def safe_ident(name: str) -> str:
    """Validate a SQL identifier and return it back-quoted. Raises on anything
    that is not a plain identifier — closing the injection hole."""
    if not isinstance(name, str) or not _IDENT_RE.match(name):
        raise ValueError(f"unsafe SQL identifier: {name!r}")
    return f"`{name}`"


def qualified(*parts: str) -> str:
    return ".".join(safe_ident(p) for p in parts)


class UnityCatalogManager:
    """Thin, parameterized wrapper over spark.sql for MDM governance ops."""

    LAYERS = ("bronze", "silver", "master", "gold", "steward", "reference", "ops")

    def __init__(self, spark: Any, catalog: str):
        self.spark = spark
        self.catalog = safe_ident(catalog).strip("`")

    def bootstrap(self, domain: str) -> None:
        safe_ident(domain)  # validate early
        self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {safe_ident(self.catalog)}")
        for layer in self.LAYERS:
            schema = f"{domain}_{layer}" if layer in ("bronze", "silver", "master", "gold") else layer
            self.spark.sql(
                f"CREATE SCHEMA IF NOT EXISTS {qualified(self.catalog, schema)}")

    def tag_pii(self, schema: str, table: str, columns: list[str]) -> None:
        for col in columns:
            self.spark.sql(
                f"ALTER TABLE {qualified(self.catalog, schema, table)} "
                f"ALTER COLUMN {safe_ident(col)} SET TAGS ('pii' = 'true')")

    def apply_column_mask(self, schema: str, table: str, column: str,
                          mask_function: str) -> None:
        self.spark.sql(
            f"ALTER TABLE {qualified(self.catalog, schema, table)} "
            f"ALTER COLUMN {safe_ident(column)} "
            f"SET MASK {qualified(self.catalog, 'ops', mask_function)}")

    def entity_history(self, schema: str, audit_table: str,
                       entity_id: str, limit: int = 100):
        """Parameterized query — entity_id is BOUND, never interpolated."""
        return self.spark.sql(
            f"SELECT * FROM {qualified(self.catalog, schema, audit_table)} "
            f"WHERE entity_id = :eid ORDER BY ts DESC LIMIT :lim",
            args={"eid": entity_id, "lim": int(limit)},
        )
