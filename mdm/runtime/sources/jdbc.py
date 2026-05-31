"""JDBC batch source with query-injection validation salvaged from v1.

The SOQL/SQL fragment validator (carried over from the old multi_source_connector)
rejects dangerous tokens before a query is ever issued, so source extraction can
parameterize table/column selection safely.
"""
from __future__ import annotations

import re
from typing import Any, Iterable

from ...core.model import RawRecord

# Reject statement-breakers / stacked queries / comments in any caller-supplied
# SOQL/SQL fragment (e.g. a WHERE clause). Salvaged & tightened from v1.
_DANGEROUS = re.compile(
    r"(;|--|/\*|\*/|\bdrop\b|\bdelete\b|\bupdate\b|\binsert\b|\bunion\b|\bexec\b)",
    re.IGNORECASE,
)


def validate_query_fragment(fragment: str) -> str:
    if _DANGEROUS.search(fragment or ""):
        raise ValueError(f"rejected unsafe query fragment: {fragment!r}")
    return fragment


class JdbcBatchSource:
    def __init__(self, spark: Any, jdbc_url: str, table: str,
                 source_system: str, pk_column: str,
                 where: str | None = None, properties: dict | None = None):
        self.spark = spark
        self.jdbc_url = jdbc_url
        self.table = table
        self.source_system = source_system
        self.pk_column = pk_column
        self.where = validate_query_fragment(where) if where else None
        self.properties = properties or {}

    def read(self) -> Iterable[RawRecord]:
        reader = (self.spark.read.format("jdbc")
                  .option("url", self.jdbc_url)
                  .option("dbtable", self.table))
        for k, v in self.properties.items():
            reader = reader.option(k, v)
        df = reader.load()
        if self.where:
            df = df.where(self.where)
        for row in df.collect():
            d = row.asDict(recursive=True)
            yield RawRecord(self.source_system, str(d.get(self.pk_column)), d)
