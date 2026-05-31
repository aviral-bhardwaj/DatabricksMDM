"""Databricks-native MDM engine.

Three strictly separated zones (see /docs blueprint, section D):

    mdm.core      — PURE DOMAIN. No pyspark/delta imports anywhere. Unit-testable
                    on pandas in milliseconds. Holds the model, ports (Protocols),
                    and the matching/survivorship/quality/standardization logic.
    mdm.runtime   — INFRASTRUCTURE. The ONLY zone that imports pyspark/delta/SDK.
                    Adapters implementing the core ports (Delta repository, sources,
                    publication adapters, Unity Catalog ops, Vector Search).
    mdm.pipelines — ORCHESTRATION. Declarative wiring of core + runtime into
                    Lakeflow/DLT + Workflows. Owns no business rules.

The dependency rule points one way: pipelines -> runtime -> core. core depends on
nothing infrastructural. That single constraint is Dependency Inversion in practice
and is enforced by tests/test_purity.py.
"""

__version__ = "2.0.0"
