"""Publication adapters.

Native posture (blueprint section F): activation is zero-copy by default. The
preferred adapter publishes nothing — downstream reads the Master table directly,
or via Delta Sharing / Change Data Feed. Kafka/JDBC adapters exist for systems
that genuinely need a push, but they are the exception, not the spine.
"""
from __future__ import annotations

from typing import Any, Iterable

from ...core.model import PublishEvent
from ..uc import qualified


class CDFAdapter:
    """'Publish' = enable Change Data Feed; consumers pull incremental deltas.
    The most native adapter: no copy, governed by Unity Catalog."""

    def __init__(self, spark: Any, catalog: str, domain: str):
        self.spark = spark
        self.table = qualified(catalog, f"{domain}_master", "golden_records")

    def publish(self, events: Iterable[PublishEvent]) -> int:
        self.spark.sql(
            f"ALTER TABLE {self.table} "
            f"SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
        return sum(1 for _ in events)


class DeltaSharingAdapter:
    """Share the Master table to a recipient via Delta Sharing (zero-copy)."""

    def __init__(self, spark: Any, share: str, catalog: str, domain: str):
        self.spark = spark
        self.share = share
        self.catalog = catalog
        self.domain = domain

    def publish(self, events: Iterable[PublishEvent]) -> int:
        table = qualified(self.catalog, f"{self.domain}_master", "golden_records")
        self.spark.sql(f"ALTER SHARE {self.share} ADD TABLE {table}")
        return sum(1 for _ in events)


class KafkaAdapter:
    """Push golden-record change events to Kafka for operational consumers."""

    def __init__(self, spark: Any, bootstrap: str, topic: str):
        self.spark = spark
        self.bootstrap = bootstrap
        self.topic = topic

    def publish(self, events: Iterable[PublishEvent]) -> int:
        from pyspark.sql import functions as F

        rows = [e.__dict__ for e in events]
        if not rows:
            return 0
        df = self.spark.createDataFrame(rows)
        (df.select(F.to_json(F.struct("*")).alias("value"))
           .write.format("kafka")
           .option("kafka.bootstrap.servers", self.bootstrap)
           .option("topic", self.topic).save())
        return len(rows)
