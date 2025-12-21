# Quick Start Guide - Databricks MDM

Get your MDM solution running quickly.

---

## Prerequisites

- Databricks Workspace with Unity Catalog enabled
- Python 3.8+
- Databricks CLI installed
- Access to at least one source system (SAP, Salesforce, Oracle, or Odoo)

---

## 5-Minute Installation & Setup

These steps follow the repository README and are the minimal commands to get started.

### 1. Install & Authenticate

```bash
# Install Databricks CLI (follow Databricks installer)
curl https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

# Check CLI version
databricks -v

# Authenticate (replace with your workspace host)
databricks auth login --host https://dbc-XXXXXXX-fa92.cloud.databricks.com
```

### 2. Clone Repository

```bash
git clone https://github.com/aviral-bhardwaj/DatabricksMDM.git
cd DatabricksMDM
```

### 3. Create Unity Catalog and Schemas (include storage root)

Follow the README's working CLI syntax: create the catalog with a storage root and specify targets when creating schemas.

```bash
# Create catalog with storage root (replace with your storage location)
databricks catalogs create mdm_catalog -t dev --storage-root "s3://your-bucket/unity-catalog/mdm/"

# Create schemas (note: schema name then catalog name, with target)
databricks schemas create bronze mdm_catalog -t dev
databricks schemas create silver mdm_catalog -t dev
databricks schemas create gold mdm_catalog -t dev
```

---

## 10-Minute Deployment

### 4. Configure

Edit the repo configuration files for your environment:

```bash
# Update databricks.yml with your workspace URL and settings
nano databricks.yml

# Update mdm configuration (source connections, credentials, etc.)
nano config/mdm_config.yaml
```

Store credentials securely in Databricks Secret Scopes (example):

```bash
databricks secrets create-scope mdm-secrets

# Example: add Salesforce credentials
databricks secrets put-secret mdm-secrets salesforce-username --string-value "your-email@company.com"
databricks secrets put-secret mdm-secrets salesforce-password --string-value "your-password"
databricks secrets put-secret mdm-secrets salesforce-security-token --string-value "your-token"
```

### 5. Deploy with Bundle

Use the same target (`dev`) as when creating catalogs/schemas:

```bash
# Validate config for target dev
databricks bundle validate --target dev

# Deploy resources (notebooks, jobs, clusters, catalogs)
databricks bundle deploy --target dev
```

### 6. Run Your First Pipeline

```bash
# Start the MDM pipeline on target dev
databricks bundle run mdm_pipeline --target dev

# Monitor recent runs
databricks jobs list-runs --limit 5
```

---

## Verify It's Working

Open Databricks SQL Editor and run:

```sql
USE CATALOG mdm_catalog;

SELECT * FROM mdm_catalog.bronze.customer_bronze LIMIT 10;
SELECT * FROM mdm_catalog.silver.customer_matched LIMIT 10;
SELECT * FROM mdm_catalog.gold.customer_golden LIMIT 10;
```

Check data quality metrics:

```sql
SELECT * FROM mdm_catalog.gold.data_quality_metrics
ORDER BY check_timestamp DESC
LIMIT 10;
```

---

## What Just Happened?

- Created Unity Catalog `mdm_catalog` with storage root and dev target
- Created Bronze/Silver/Gold schemas
- Configured secure credential storage (Databricks secrets)
- Deployed notebooks, jobs, and clusters via bundle deploy
- Ran the MDM pipeline to ingest, match, and produce golden records

---

## Next Steps

1. Schedule daily runs by adding schedule to `databricks.yml` resources for the job (use the same `dev` target if applicable).
2. Explore golden records and match quality with SQL queries.
3. Create manual overrides via the survivorship management notebooks (03_golden_record).
4. If you need API access, deploy and test the REST API and SDK as described in README.

---

## Troubleshooting Quick Commands

```bash
# Re-authenticate
databricks auth login

# Show config
databricks configure show

# Check a job run
databricks jobs get-run --run-id <run-id>

# Get run output
databricks jobs get-run-output --run-id <run-id>
```

SQL checks:

```sql
SHOW TABLES IN mdm_catalog.bronze;
SELECT 'Bronze' as layer, COUNT(*) FROM mdm_catalog.bronze.customer_bronze
UNION ALL
SELECT 'Silver', COUNT(*) FROM mdm_catalog.silver.customer_matched
UNION ALL
SELECT 'Gold', COUNT(*) FROM mdm_catalog.gold.customer_golden;
```

---

## Quick Reference

- Deploy: databricks bundle deploy --target dev
- Run: databricks bundle run mdm_pipeline --target dev
- Create catalog (with storage root): databricks catalogs create mdm_catalog -t dev --storage-root "<STORAGE_PATH>"
- Create schemas: databricks schemas create <schema_name> mdm_catalog -t dev

---

For full production deployment and detailed steps, see DEPLOYMENT_GUIDE.md.
