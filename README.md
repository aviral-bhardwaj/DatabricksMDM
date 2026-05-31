# Databricks-Native MDM

A **truly Databricks-native** Master Data Management engine — the golden record is
a governed Delta table, identity resolution and survivorship are declarative
lakehouse pipelines, and all the domain logic is a pure, unit-tested Python core
with **no Spark coupling**. Built with SOLID + DDD, governed by Unity Catalog.

> 📐 **Architecture Blueprint:** the opinionated, SOLID-based design behind this
> engine (market research, anti-patterns, layer→service mapping, domain model,
> roadmap) is published as a website under [`/docs`](docs/), served via **GitHub
> Pages** at **https://aviral-bhardwaj.github.io/databricksmdm/** (after merge to
> `main`).

---

## Why v2 (and what changed)

The previous implementation (now under [`legacy/`](legacy/)) was *traditional MDM
transplanted onto Databricks* — the anti-pattern the blueprint warns against. v2
rebuilds it natively. See [`legacy/README.md`](legacy/README.md) for the full
before/after, including the **SQL-injection fixes** and the **broken-clustering
fix**.

| | Legacy v1 | Native v2 (`/mdm`) |
|---|---|---|
| Domain logic | welded to Spark, untestable offline | pure `mdm/core`, 33 tests run in 0.1s on pandas |
| Matching | one hardcoded Random Forest | pluggable strategy registry: deterministic / probabilistic / **semantic (vector)** |
| Blocking | hardcoded to `country` | config-driven keys |
| Clustering | `monotonically_increasing_id` (non-transitive **bug**) | union-find connected components |
| Quality | customer-only `if/elif` | config-driven rules, any domain |
| Survivorship | Spark-coupled | rules-as-data evaluators (+ `TRUST_DECAY`) |
| Governance/SQL | string-interpolated (**injectable**) | parameterized + identifier-validated |
| New domain | code fork | a YAML file |

## Architecture — three strictly separated zones

```
mdm/core      PURE DOMAIN   no pyspark anywhere (enforced by tests/test_purity.py)
              model · ports · standardize · quality · matching · cluster ·
              mastering(survivorship) · stewardship · service
mdm/runtime   INFRASTRUCTURE  the ONLY zone importing pyspark/delta/SDK
              repository_delta · sources · adapters · uc · vector · observability
              (+ repository_memory & encoders that need no cluster)
mdm/pipelines ORCHESTRATION  declarative Lakeflow/DLT + Workflows; owns no rules
              bronze_ingest · silver_standardize(DLT) · match_cluster · master_publish
```

The dependency rule points one way — `pipelines → runtime → core` — so the same
match/survivorship logic runs **identically** in a 0.1s local test and on a
billion rows on a cluster. That is Dependency Inversion made real.

## Run it locally (no Databricks required)

```bash
pip install -e ".[dev]"

pytest                 # 33 tests, pure-domain, ~0.1s
python -m mdm demo     # end-to-end on bundled multi-source sample data
```

The demo resolves 16 records across SAP / Salesforce / Oracle / Odoo into 7 golden
records (56% dedup), applying config-driven matching and survivorship with full
per-attribute provenance — all on pandas, proving the engine works without a cluster.

## Deploy to Databricks

```bash
databricks bundle validate --target dev
databricks bundle deploy   --target dev
databricks bundle run mdm_native_pipeline --target dev
```

The bundle ([`databricks.yml`](databricks.yml)) wires Auto Loader → Bronze, a DLT
pipeline for Silver standardization + quality expectations, a match/cluster/master
job, and zero-copy publish via Change Data Feed. Multi-domain via `--var domain=…`
(configs in [`conf/domains/`](conf/domains/): customer, product, supplier, location).

## Repository layout

```
mdm/                 native engine (core / runtime / pipelines / cli)
conf/domains/        per-domain configuration (a domain is data, not code)
data/sample/         multi-source sample CSVs for the demo
tests/               pure-domain unit + contract + purity tests
docs/                architecture blueprint website (GitHub Pages)
legacy/              superseded v1 (notebooks, api, sdk) — see legacy/README.md
databricks.yml       Asset Bundle (native pipeline + legacy job)
pyproject.toml       installable package `mdm`, extras [spark] [dev] [demo]
```

## License

Proprietary — All rights reserved.
