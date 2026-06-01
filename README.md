# Databricks-Native MDM

A **truly Databricks-native** Master Data Management engine. The golden record is a
governed Delta table, identity resolution and survivorship are declarative lakehouse
pipelines, and all domain logic is a pure, unit-tested Python core with **no Spark
coupling**. Built with SOLID + DDD, governed by Unity Catalog.

> 📐 **Architecture blueprint** (the design behind this engine) is published as a
> website under [`/docs`](docs/) → **https://aviral-bhardwaj.github.io/databricksmdm/**

---

## Table of contents

1. [What you get](#1-what-you-get)
2. [Architecture in 30 seconds](#2-architecture-in-30-seconds)
3. [Prerequisites](#3-prerequisites)
4. [Run locally (no Databricks needed)](#4-run-locally-no-databricks-needed)
5. [Deploy to Databricks — end to end](#5-deploy-to-databricks--end-to-end)
6. [Run the pipeline](#6-run-the-pipeline)
7. [Verify the results](#7-verify-the-results)
8. [Configure a domain (customer / product / supplier / location)](#8-configure-a-domain)
9. [Connect your own sources](#9-connect-your-own-sources)
10. [Project layout](#10-project-layout)
11. [Testing & development](#11-testing--development)
12. [Troubleshooting](#12-troubleshooting)
13. [Legacy v1](#13-legacy-v1)

---

## 1. What you get

| Capability | How |
|---|---|
| Multi-source ingestion → **Bronze** | Auto Loader (files) / JDBC connectors, append-only Delta with provenance |
| Standardization + quality → **Silver** | Declarative **DLT** pipeline with expectations; config-driven rules |
| Identity resolution → **Master/Trust** | Pluggable match strategies (deterministic / probabilistic / **semantic-vector**) + **union-find** clustering |
| Golden records | **Rules-as-data** survivorship (trust/decay), Delta `MERGE`, per-attribute lineage |
| Activation | **Zero-copy** via Change Data Feed / Delta Sharing; Kafka/JDBC adapters optional |
| Governance | **Unity Catalog** — grants, masking, tags, lineage (adopted, not rebuilt) |
| Multi-domain | customer · product · supplier · location — **by config, not code** |

## 2. Architecture in 30 seconds

```
mdm/core      PURE DOMAIN   no pyspark anywhere (enforced by tests/test_purity.py)
mdm/runtime   INFRASTRUCTURE  the only zone importing pyspark/delta/SDK
mdm/pipelines ORCHESTRATION  declarative Lakeflow/DLT + Workflows; owns no rules
```

Dependency rule points one way — `pipelines → runtime → core` — so the **same**
match/survivorship logic runs in a 0.1s local test and on a billion rows on a cluster.

---

## 3. Prerequisites

### To run locally
- **Python 3.9+** and `pip`

### To deploy to Databricks
- A **Databricks workspace with Unity Catalog** enabled
- **Databricks CLI 0.205+** (the new bundle-aware CLI)
- Permissions to create **catalogs, schemas, jobs, DLT pipelines, and clusters** (or a workspace admin to grant them)
- (Optional) Network access / credentials to your source systems (SAP, Salesforce, Oracle, Odoo, cloud storage)

```bash
# Install the Databricks CLI
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
databricks --version          # expect v0.205.0 or newer
python --version              # expect 3.9+
```

---

## 4. Run locally (no Databricks needed)

This proves the engine end-to-end on bundled multi-source sample data, entirely on
pandas — **no cluster, no account required.**

```bash
# 1. Clone
git clone https://github.com/aviral-bhardwaj/DatabricksMDM.git
cd DatabricksMDM

# 2. Install the package + dev extras (pytest, pandas)
pip install -e ".[dev]"

# 3. Run the test suite (pure-domain, ~0.1s)
pytest                        # -> 33 passed

# 4. Run the end-to-end demo
python -m mdm demo            # default domain: customer
```

Expected demo output (abridged):

```
========================================================================
  Databricks-native MDM — demo  (domain: customer)
========================================================================
  loaded 16 raw records from 4 sources

  Resolution stats
    input_records     : 16
    golden_records    : 7
    dedup_ratio       : 0.5625

  ● 'acme'  trust=0.81  members=3  sources=['oracle','salesforce','sap']
    email=info@acme.com  phone=+14155550100  city=san francisco
    provenance: name<-sap  phone<-oracle  email<-MOST_FREQUENT
  ...
  16 records collapsed into 7 golden records (56% dedup).
========================================================================
```

Try another domain config:

```bash
python -m mdm demo --domain customer --config conf/domains/customer.yml
```

---

## 5. Deploy to Databricks — end to end

Everything is a **Databricks Asset Bundle** ([`databricks.yml`](databricks.yml)).
There are two environments (`dev`, `prod`) and three bundle variables
(`catalog`, `domain`, `config_path`).

### Step 1 — Authenticate

```bash
# OAuth (recommended)
databricks auth login --host https://dbc-bd6bd71d-fa92.cloud.databricks.com

# …or a Personal Access Token
databricks configure --token
#   Host:  https://dbc-bd6bd71d-fa92.cloud.databricks.com
#   Token: <your token from User Settings → Developer → Access tokens>

databricks current-user me     # sanity check
```

### Step 2 — Create the Unity Catalog (one-time)

The bundle uses catalog `mdm_dev` for the `dev` target and `mdm_prod` for `prod`.
Create the catalog with a storage root your workspace can write to:

```bash
# dev catalog (replace the storage root with your own external location)
databricks catalogs create mdm_dev \
  --storage-root "s3://<your-bucket>/unity-catalog/mdm_dev" \
  --comment "Databricks-native MDM (dev)"
```

The pipeline creates the per-domain schemas it needs (`<domain>_bronze`,
`<domain>_silver`, `<domain>_master`, `<domain>_gold`) automatically on first run
via `mdm/runtime/uc.py`. To pre-create them manually:

```bash
databricks schemas create customer_bronze  mdm_dev
databricks schemas create customer_silver  mdm_dev
databricks schemas create customer_master  mdm_dev
databricks schemas create customer_gold    mdm_dev
```

### Step 3 — Validate and deploy the bundle

```bash
# from the repo root
databricks bundle validate --target dev      # checks YAML + resources
databricks bundle deploy   --target dev      # uploads code, creates job + DLT pipeline
```

`deploy` creates:
- the **DLT pipeline** `MDM_Silver_Standardize_customer` (Bronze → Silver), and
- the **Workflows job** `MDM_Native_Pipeline_customer` (ingest → standardize → match/cluster/master → publish).

### Step 4 — (Optional) point the config path at the deployed repo

The job reads the domain YAML at runtime via the `config_path` variable. After
`deploy`, the repo lives under your workspace files; set the variable to match, e.g.:

```bash
databricks bundle deploy --target dev \
  --var="config_path=/Workspace/Users/<you>/.bundle/databricks-mdm/dev/files/conf/domains/customer.yml"
```

(Use `databricks bundle summary --target dev` to see the exact deployed path.)

---

## 6. Run the pipeline

```bash
# Run the full native pipeline
databricks bundle run mdm_native_pipeline --target dev

# Run a different domain without editing files
databricks bundle run mdm_native_pipeline --target dev \
  --var="domain=supplier" \
  --var="config_path=/Workspace/.../conf/domains/supplier.yml"
```

To run against **production**:

```bash
databricks bundle deploy --target prod
databricks bundle run    mdm_native_pipeline --target prod
```

The pipeline stages (each a task in the job):

| Task | File | Output |
|---|---|---|
| `ingest_bronze` | `mdm/pipelines/bronze_ingest.py` | `<catalog>.<domain>_bronze.raw_records` |
| `standardize_silver` (DLT) | `mdm/pipelines/silver_standardize.py` | `<catalog>.<domain>_silver.standardized_records` |
| `match_cluster_master` | `mdm/pipelines/match_cluster.py` | `<catalog>.<domain>_master.golden_records` |
| `publish` | `mdm/pipelines/master_publish.py` | Change Data Feed enabled for downstream |

> **Landing data for Auto Loader:** the `ingest_bronze` task reads from
> `/Volumes/<catalog>/<domain>_bronze/landing`. Create that Volume and drop source
> files there, or swap the task for a JDBC source (see [section 9](#9-connect-your-own-sources)).

---

## 7. Verify the results

```sql
-- Golden records (the Master/Trust tier)
SELECT entity_id, name, email, phone, city, trust_score, version
FROM mdm_dev.customer_master.golden_records
ORDER BY trust_score DESC
LIMIT 20;

-- How many sources collapsed into each entity
SELECT cluster_id, count(*) AS members
FROM mdm_dev.customer_master.golden_records
GROUP BY cluster_id ORDER BY members DESC;

-- Silver quality distribution
SELECT quality_tier, count(*) FROM mdm_dev.customer_silver.standardized_records
GROUP BY quality_tier;
```

Check the run in the UI: **Workflows → `MDM_Native_Pipeline_customer`**, and the
DLT graph under **Delta Live Tables / Pipelines**. Lineage and audit are available
via Unity Catalog and the `system.*` tables (no custom logging required).

---

## 8. Configure a domain

A domain is **pure configuration** — adding one needs no engine code. Configs live
in [`conf/domains/`](conf/domains/) (`customer.yml`, `product.yml`, `supplier.yml`,
`location.yml`). Each declares:

```yaml
domain: customer
entity_attributes: [name, email, phone, city, country]
field_map:            # source column -> canonical attribute (per source)
  sap:   { KUNNR: source_pk, NAME1: name, SMTP_ADDR: email, ... }
standardization:      # canonical attribute -> function pipeline
  name: [name]
quality_rules:        # declarative, any domain
  - { field: email, kind: regex, pattern: "...", weight: 2 }
blocking:             # candidate generation keys (fixes v1's hardcoded country)
  - { fields: [email] }
  - { fields: [name], prefix: 4 }
match_strategies:     # pluggable; registry picks the decisive one
  - { kind: deterministic, keys: [email] }
  - { kind: probabilistic, comparators: [ { field: name, method: jaro_winkler, weight: 3 } ] }
merge_threshold: 0.85
survivorship:         # rules-as-data
  name:  { strategy: SOURCE_PRIORITY, priority: { sap: 1, oracle: 2 } }
  phone: { strategy: MOST_RECENT }
```

Validate any config locally before deploying:

```bash
python -c "from mdm.core.config.loader import load_domain_config as L; print(L('conf/domains/product.yml').domain, 'OK')"
```

---

## 9. Connect your own sources

Two native ingestion paths, both landing in Bronze:

- **Files (recommended):** drop CSV/JSON/Parquet into the Volume
  `/Volumes/<catalog>/<domain>_bronze/landing`; `bronze_ingest.py` (Auto Loader)
  picks them up incrementally with schema evolution.
- **JDBC (SAP/Oracle/etc.):** use `mdm/runtime/sources/jdbc.py` (`JdbcBatchSource`)
  — it includes query-fragment validation to block injection. Store credentials in
  a Databricks **secret scope** and reference them, never in code:

```bash
databricks secrets create-scope mdm
databricks secrets put-secret mdm oracle_jdbc_url
databricks secrets put-secret mdm oracle_password
```

Map each source's columns to canonical attributes in the domain's `field_map`.

---

## 10. Project layout

```
DatabricksMDM/
├── mdm/                    native engine
│   ├── core/               pure domain (no pyspark)
│   ├── runtime/            Spark/Delta adapters (the only Spark-aware zone)
│   ├── pipelines/          declarative DLT + Workflows wiring
│   └── cli.py              `python -m mdm demo`
├── conf/domains/           per-domain config (customer/product/supplier/location)
├── data/sample/            multi-source sample CSVs for the demo
├── tests/                  pure-domain unit + contract + purity tests
├── docs/                   architecture blueprint website (GitHub Pages)
├── legacy/                 superseded v1 — see legacy/README.md
├── databricks.yml          Asset Bundle (native pipeline + legacy job)
├── pyproject.toml          installable package `mdm`, extras [spark] [dev] [demo]
└── requirements.txt
```

---

## 11. Testing & development

```bash
pip install -e ".[dev]"

pytest                      # all tests
pytest tests/unit/test_cluster.py -v      # the transitivity (bug-fix) test
pytest tests/test_purity.py               # asserts core never imports pyspark
```

Design rules to keep the engine native:
- **Never import pyspark in `mdm/core`** — put Spark in `mdm/runtime` behind a port.
- **New match algorithm / survivorship rule = a class + a registry entry + YAML**, not an edit to existing logic (Open/Closed).
- **New domain = a YAML file** in `conf/domains/`.

---

## 12. Troubleshooting

| Symptom | Fix |
|---|---|
| `bundle validate` fails on variables | Pass `--var="catalog=...,domain=...,config_path=..."` or set them in the target. |
| `Catalog 'mdm_dev' does not exist` | Create it (section 5, step 2) with a valid `--storage-root`. |
| DLT pipeline can't import `mdm` | Ensure the bundle deployed the repo (`bundle deploy`) and the cluster uses the bundled wheel/files; the package must be on the path. |
| Auto Loader finds no data | Create the Volume `/Volumes/<catalog>/<domain>_bronze/landing` and drop source files there. |
| `python -m mdm demo` import error | Run `pip install -e ".[dev]"` from the repo root first. |
| Permission denied creating schemas | Ask an admin to grant `CREATE SCHEMA` on the catalog, or pre-create schemas. |
| Secrets not found | `databricks secrets create-scope mdm` and add keys (section 9). |

---

## 13. Legacy v1

The previous implementation lives under [`legacy/`](legacy/) and is no longer the
default. It is kept for reference and migration; its README documents the
before/after, including the **SQL-injection** and **non-transitive-clustering** fixes
made in v2. Do not build new features there — extend `/mdm`.

## License

Proprietary — All rights reserved.
