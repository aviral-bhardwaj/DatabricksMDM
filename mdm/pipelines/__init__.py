"""Orchestration zone — declarative Lakeflow/DLT + Spark jobs that WIRE the pure
core to the Spark runtime. These modules own no business rules; all matching /
survivorship / quality logic lives in mdm.core and is merely invoked here.

Run on Databricks (they import pyspark / dlt). They are intentionally thin.
"""
