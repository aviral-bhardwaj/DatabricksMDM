# Legacy MDM (v1) — superseded

This folder contains the **original v1 implementation** of the Databricks MDM
project. It still works and remains here for reference and migration, but it has
been **superseded by the native engine in [`/mdm`](../mdm)**.

## Why it was replaced

The v1 modules were functional but followed the "traditional MDM transplanted
onto Databricks" pattern that the [architecture blueprint](../docs) explicitly
warns against:

| v1 issue | Where | Fixed in `/mdm` |
|----------|-------|-----------------|
| Domain logic welded to Spark (untestable without a cluster) | all modules | Pure `mdm/core` (pandas-testable) + `mdm/runtime` adapters |
| Hardcoded `country` blocking key | `02_matching/entity_resolution.py` | Config-driven blocking (`mdm/core/matching/blocking.py`) |
| Customer-only DQ rules (if/elif) | `04_quality/dq_framework.py` | Config-driven rule engine, any domain (`mdm/core/quality`) |
| Broken transitive closure (`monotonically_increasing_id`) | `02_matching` clustering | Union-find connected components (`mdm/core/cluster`) |
| **SQL injection** via string interpolation | `MatchReviewManager`, `AuditTrailManager.get_entity_history/get_user_activity`, `LineageTracker._get_*_lineage` | Parameterized + identifier-validated queries (`mdm/runtime/uc.py`) |
| Stub methods (`_trigger_*` print stubs) | matching / golden record | Real wiring via ports + events |
| Single hardcoded RandomForest on the match path | `02_matching` | Pluggable `MatchStrategy` registry (deterministic / probabilistic / semantic) |

## What was salvaged

The genuinely valuable pieces were carried forward into `/mdm`:
- SOQL-injection validation → `mdm/runtime/sources`
- Survivorship strategy framework (MOST_RECENT / SOURCE_PRIORITY / MOST_COMPLETE
  + weighted-average / consensus / longest / most-frequent) → pure evaluators in
  `mdm/core/mastering/survivorship.py`
- Delta CDF + lineage + audit patterns → `mdm/runtime/adapters` and `mdm/runtime/uc.py`
- GOLD/SILVER/BRONZE quality tiering → `mdm/core/quality`

Do not build new features here — extend `/mdm` instead.
