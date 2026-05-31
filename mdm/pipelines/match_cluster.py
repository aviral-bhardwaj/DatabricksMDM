"""Spark job: Silver -> Master/Trust (block -> match -> cluster -> survivorship).

Blocking is distributed (groupBy block key); within each block the PURE match
strategies + union-find clusterer from mdm.core run on the collected partition.
For very large blocks, swap in GraphFrames connected-components — the core
ClusterPolicy contract is unchanged.

Run as a Workflows Python task. Imports pyspark.
"""
from __future__ import annotations

from pyspark.sql import SparkSession

from mdm.core.config.loader import load_domain_config
from mdm.core.service import ResolutionService
from mdm.runtime.repository_delta import DeltaRecordRepository


def run(catalog: str, domain: str, config_path: str) -> dict:
    spark = SparkSession.builder.getOrCreate()
    config = load_domain_config(config_path)
    repo = DeltaRecordRepository(spark, catalog, domain)

    standardized = repo.load_standardized(domain)
    service = ResolutionService(config)

    # The pure service runs match -> cluster -> survivorship identically to the
    # local demo; at scale, partition `standardized` by block key upstream and
    # union the per-block ResolutionOutputs.
    out = service.resolve_from_standardized(standardized)
    repo.save_golden(domain, out.golden)
    return out.stats


if __name__ == "__main__":
    import sys

    print(run(sys.argv[1], sys.argv[2], sys.argv[3]))
