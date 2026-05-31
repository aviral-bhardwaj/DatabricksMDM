"""Pure domain core. MUST NOT import pyspark, delta, or the Databricks SDK.

Enforced by tests/test_purity.py. If you need Spark, you are in the wrong zone —
put it in mdm.runtime and depend on a port from mdm.core.ports.
"""
