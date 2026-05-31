"""Publish golden-record changes (zero-copy first).

Emits PublishEvents from the Master Change Data Feed and routes them through the
configured PublicationAdapter. Default = CDFAdapter (no copy); downstream pulls
incremental deltas governed by Unity Catalog.
"""
from __future__ import annotations

from pyspark.sql import SparkSession

from mdm.core.model import PublishEvent
from mdm.runtime.adapters.publication import CDFAdapter
from mdm.runtime.uc import qualified


def run(catalog: str, domain: str, since_version: int = 0) -> int:
    spark = SparkSession.builder.getOrCreate()
    master = qualified(catalog, f"{domain}_master", "golden_records").replace("`", "")
    changes = (spark.read.format("delta")
               .option("readChangeData", "true")
               .option("startingVersion", since_version)
               .table(master))
    events = [
        PublishEvent(entity_id=r["entity_id"], domain=domain,
                     change_type=r["_change_type"], version=int(r["_commit_version"]))
        for r in changes.collect()
    ]
    return CDFAdapter(spark, catalog, domain).publish(events)


if __name__ == "__main__":
    import sys

    print(run(sys.argv[1], sys.argv[2], int(sys.argv[3]) if len(sys.argv) > 3 else 0))
