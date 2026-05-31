"""Lakeflow/DLT: Bronze -> Silver standardization + quality (declarative).

The DLT decorators define the dataset; the row-level work delegates to the PURE
core (ConfigStandardizer + QualityEngine) via a pandas UDF. Domain logic stays in
mdm.core; this file only wires it into a declarative table with expectations.

Deployed as a DLT pipeline (see databricks.yml). Imports dlt/pyspark, so it runs
on Databricks, not in local unit tests.
"""
from __future__ import annotations

import dlt  # type: ignore
from pyspark.sql import functions as F

from mdm.core.config.loader import load_domain_config
from mdm.core.model import RawRecord
from mdm.core.quality.engine import QualityEngine, Rule
from mdm.core.standardize.engine import ConfigStandardizer

DOMAIN = spark.conf.get("mdm.domain", "customer")  # type: ignore  # noqa: F821
CONFIG = load_domain_config(spark.conf.get("mdm.config_path"))  # type: ignore  # noqa: F821

_std = ConfigStandardizer(CONFIG.domain, CONFIG.standardization, CONFIG.field_map)
_dq = QualityEngine([Rule(**r) for r in CONFIG.quality_rules])


@dlt.table(name=f"{DOMAIN}_silver.standardized_records",
           comment="Standardized + quality-scored records (Silver).")
@dlt.expect_or_drop("has_record_id", "record_id IS NOT NULL")
@dlt.expect("acceptable_quality", "quality_score >= 0.5")
def standardized_records():
    bronze = dlt.read_stream(f"{DOMAIN}_bronze.raw_records")

    def _standardize(pdf):
        out = []
        for _, row in pdf.iterrows():
            raw = RawRecord(row["source_system"], str(row["source_pk"]),
                            row["attributes"])
            sr = _std.standardize(raw)
            v = _dq.evaluate(sr)
            out.append({"record_id": sr.record_id,
                        "source_system": sr.source_system,
                        "domain": sr.domain, "attributes": sr.attributes,
                        "quality_score": v.score, "quality_tier": v.tier.value})
        import pandas as pd
        return pd.DataFrame(out)

    return bronze.groupBy(F.spark_partition_id()).applyInPandas(
        _standardize, schema="record_id string, source_system string, "
        "domain string, attributes map<string,string>, "
        "quality_score double, quality_tier string")
