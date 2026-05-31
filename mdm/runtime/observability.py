"""Observability via system tables — audit, lineage, and cost are queries, not a
bespoke logging system (blueprint anti-pattern #7: a native product reads
system.access / system.lineage / system.billing instead of hand-rolling them).
"""
from __future__ import annotations

from typing import Any


class SystemTableObservability:
    def __init__(self, spark: Any):
        self.spark = spark

    def lineage_for(self, table_full_name: str):
        return self.spark.sql(
            "SELECT * FROM system.access.table_lineage "
            "WHERE target_table_full_name = :t OR source_table_full_name = :t",
            args={"t": table_full_name})

    def run_cost(self, sku_filter: str = "%JOBS%"):
        return self.spark.sql(
            "SELECT usage_date, sku_name, SUM(usage_quantity) AS dbus "
            "FROM system.billing.usage WHERE sku_name LIKE :sku "
            "GROUP BY usage_date, sku_name ORDER BY usage_date DESC",
            args={"sku": sku_filter})
