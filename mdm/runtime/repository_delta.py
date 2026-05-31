"""DeltaRecordRepository — Spark/Delta-backed RecordRepository (same port as the
in-memory one, Liskov-substitutable).

Reads StandardizedRecords from the Silver Delta table and upserts GoldenRecords
into the Master/Trust table via an idempotent MERGE (safe to re-run; replayable).
pyspark is imported lazily so the rest of mdm.runtime stays importable without a
cluster.
"""
from __future__ import annotations

from typing import Any

from ..core.model import GoldenRecord, StandardizedRecord
from .uc import qualified


class DeltaRecordRepository:
    def __init__(self, spark: Any, catalog: str, domain: str):
        self.spark = spark
        self.catalog = catalog
        self.domain = domain

    def _silver(self) -> str:
        return qualified(self.catalog, f"{self.domain}_silver", "standardized_records")

    def _master(self) -> str:
        return qualified(self.catalog, f"{self.domain}_master", "golden_records")

    def load_standardized(self, domain: str) -> list[StandardizedRecord]:
        from pyspark.sql import functions as F  # noqa: F401  (lazy import)

        rows = self.spark.read.table(self._silver()).collect()
        out: list[StandardizedRecord] = []
        for r in rows:
            d = r.asDict(recursive=True)
            out.append(StandardizedRecord(
                record_id=d["record_id"],
                source_system=d["source_system"],
                domain=domain,
                attributes=d.get("attributes", {}),
            ))
        return out

    def save_golden(self, domain: str, records: list[GoldenRecord]) -> None:
        from delta.tables import DeltaTable

        rows = [g.as_flat_dict() for g in records]
        if not rows:
            return
        updates = self.spark.createDataFrame(rows)
        target = self._master()
        if not self.spark.catalog.tableExists(target.replace("`", "")):
            updates.write.format("delta").option(
                "delta.enableChangeDataFeed", "true").saveAsTable(
                target.replace("`", ""))
            return
        dt = DeltaTable.forName(self.spark, target.replace("`", ""))
        (dt.alias("t")
           .merge(updates.alias("s"), "t.entity_id = s.entity_id")
           .whenMatchedUpdateAll()
           .whenNotMatchedInsertAll()
           .execute())
