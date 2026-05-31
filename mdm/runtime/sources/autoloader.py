"""Auto Loader source: incremental, schema-evolving file ingestion to Bronze.

This is the native ingestion path — cloudFiles with schema inference + evolution,
no external staging database. Returns a streaming/batch DataFrame the pipeline
writes to a Bronze Delta table with provenance columns.
"""
from __future__ import annotations

from typing import Any


class AutoLoaderSource:
    def __init__(self, spark: Any, path: str, fmt: str = "json",
                 schema_location: str | None = None):
        self.spark = spark
        self.path = path
        self.fmt = fmt
        self.schema_location = schema_location or f"{path}/_schema"

    def read_stream(self):
        from pyspark.sql import functions as F

        return (self.spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", self.fmt)
                .option("cloudFiles.schemaLocation", self.schema_location)
                .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
                .load(self.path)
                .withColumn("_ingested_at", F.current_timestamp())
                .withColumn("_source_file", F.col("_metadata.file_path")))
