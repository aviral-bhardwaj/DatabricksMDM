"""Auto Loader ingestion -> Bronze (append-only, schema-evolving).

Lands raw source files as Delta with provenance columns. No external staging DB —
the lakehouse is the landing zone (native requirement #1).
"""
from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from mdm.runtime.sources.autoloader import AutoLoaderSource
from mdm.runtime.uc import qualified


def run(catalog: str, domain: str, source_path: str,
        source_system: str, fmt: str = "json") -> None:
    spark = SparkSession.builder.getOrCreate()
    bronze = qualified(catalog, f"{domain}_bronze", "raw_records").replace("`", "")
    df = (AutoLoaderSource(spark, source_path, fmt).read_stream()
          .withColumn("source_system", F.lit(source_system)))
    (df.writeStream.format("delta")
       .option("checkpointLocation", f"{source_path}/_checkpoint")
       .option("mergeSchema", "true")
       .trigger(availableNow=True)
       .toTable(bronze))


if __name__ == "__main__":
    import sys

    # args: catalog domain source_path source_system [format]
    run(*sys.argv[1:6])
