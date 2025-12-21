# Databricks MDM - Complete Deployment Guide

This guide will walk you through deploying the Master Data Management (MDM) solution to Databricks step by step.

---

## Table of Contents
1. [Prerequisites](#prerequisites)
2. [Environment Setup](#environment-setup)
3. [Unity Catalog Configuration](#unity-catalog-configuration)
4. [Deploy Using Databricks Asset Bundles](#deploy-using-databricks-asset-bundles)
5. [Configuration Setup](#configuration-setup)
6. [Deploy and Run MDM Pipelines](#deploy-and-run-mdm-pipelines)
7. [Verify Deployment](#verify-deployment)
8. [Common Issues and Troubleshooting](#common-issues-and-troubleshooting)

---

## Prerequisites

### Required Tools
```bash
# 1. Install Databricks CLI (version 0.200+)
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

# Verify installation
databricks --version

# 2. Install Python 3.8+
python --version

# 3. Install Git
git --version
```

### Required Access
- ✅ Databricks workspace with Unity Catalog enabled
- ✅ Workspace Admin or permissions to:
  - Create clusters
  - Create jobs
  - Create catalogs and schemas
  - Install libraries
- ✅ Access to source systems (SAP, Salesforce, Oracle, Odoo)

---

## Environment Setup

### Step 1: Clone the Repository

```bash
cd /path/to/your/workspace
git clone <your-repo-url>
cd DatabricksMDM
```

### Step 2: Configure Databricks CLI

```bash
# Configure authentication (choose one method)

# Method 1: OAuth (Recommended for interactive use)
databricks auth login --host https://<your-workspace>.cloud.databricks.com

# Method 2: Personal Access Token
databricks configure --token
# Enter your workspace URL: https://<your-workspace>.cloud.databricks.com
# Enter your token: <your-databricks-token>

# Verify connection
databricks workspace list /
```

**To create a Personal Access Token:**
1. Go to Databricks workspace → User Settings → Access Tokens
2. Click "Generate New Token"
3. Give it a name (e.g., "MDM Deployment")
4. Set expiration (e.g., 90 days)
5. Copy and save the token securely

### Step 3: Set Up Python Environment

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

---

## Unity Catalog Configuration

### Step 1: Create Unity Catalog Resources

```bash
# Log into Databricks workspace
databricks workspace current

# Create catalog (if not exists)
databricks unity-catalog catalogs create mdm_catalog \
  --comment "Master Data Management Catalog"

# Create schemas
databricks unity-catalog schemas create mdm_catalog.bronze \
  --comment "Bronze layer - raw ingested data"

databricks unity-catalog schemas create mdm_catalog.silver \
  --comment "Silver layer - matched entities"

databricks unity-catalog schemas create mdm_catalog.gold \
  --comment "Gold layer - golden records"

# Verify creation
databricks unity-catalog catalogs list
databricks unity-catalog schemas list mdm_catalog
```

**Using SQL (Alternative):**
```sql
-- Run in Databricks SQL Editor or notebook
CREATE CATALOG IF NOT EXISTS mdm_catalog
  COMMENT 'Master Data Management Catalog';

CREATE SCHEMA IF NOT EXISTS mdm_catalog.bronze
  COMMENT 'Bronze layer - raw ingested data';

CREATE SCHEMA IF NOT EXISTS mdm_catalog.silver
  COMMENT 'Silver layer - matched entities';

CREATE SCHEMA IF NOT EXISTS mdm_catalog.gold
  COMMENT 'Gold layer - golden records';

-- Grant permissions (adjust as needed)
GRANT USE CATALOG ON CATALOG mdm_catalog TO `data_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA mdm_catalog.bronze TO `data_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA mdm_catalog.silver TO `data_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA mdm_catalog.gold TO `data_engineers`;
```

### Step 2: Set Up Secret Scopes for Credentials

```bash
# Create secret scope for source system credentials
databricks secrets create-scope mdm-secrets

# Add secrets for source systems
# SAP credentials
databricks secrets put-secret mdm-secrets sap-username --string-value "your-sap-user"
databricks secrets put-secret mdm-secrets sap-password --string-value "your-sap-password"

# Salesforce credentials
databricks secrets put-secret mdm-secrets salesforce-username --string-value "your-sf-user"
databricks secrets put-secret mdm-secrets salesforce-password --string-value "your-sf-password"
databricks secrets put-secret mdm-secrets salesforce-security-token --string-value "your-sf-token"

# Oracle credentials
databricks secrets put-secret mdm-secrets oracle-user --string-value "your-oracle-user"
databricks secrets put-secret mdm-secrets oracle-password --string-value "your-oracle-password"

# Odoo credentials
databricks secrets put-secret mdm-secrets odoo-username --string-value "your-odoo-user"
databricks secrets put-secret mdm-secrets odoo-password --string-value "your-odoo-password"

# Verify secrets
databricks secrets list-secrets mdm-secrets
```

---

## Deploy Using Databricks Asset Bundles

### Step 1: Update Bundle Configuration

Edit `databricks.yml` to match your environment:

```yaml
bundle:
  name: databricks-mdm

targets:
  dev:
    # Update with your dev workspace details
    workspace:
      host: https://<your-dev-workspace>.cloud.databricks.com

  prod:
    # Update with your prod workspace details
    workspace:
      host: https://<your-prod-workspace>.cloud.databricks.com
```

### Step 2: Validate Bundle

```bash
# Validate bundle configuration
databricks bundle validate -t dev

# Expected output: "Bundle configuration is valid"
```

### Step 3: Deploy to Development

```bash
# Deploy to dev environment
databricks bundle deploy -t dev

# This will:
# - Upload notebooks to workspace
# - Create job definitions
# - Configure clusters
# - Set up dependencies
```

### Step 4: Deploy to Production

```bash
# Deploy to prod environment (when ready)
databricks bundle deploy -t prod
```

---

## Configuration Setup

### Step 1: Configure MDM Settings

Edit `config/mdm_config.yaml`:

```yaml
# Update source connections with your actual endpoints
source_connections:
  sap:
    jdbc_url: "jdbc:sap://your-sap-host:30015"
    table: "CUSTOMER_TABLE"
    primary_key: "CUSTOMER_ID"

  salesforce:
    instance_url: "https://your-instance.salesforce.com"
    object: "Account"
    fields: ["Id", "Name", "Email", "Phone", "Industry"]

  oracle:
    jdbc_url: "jdbc:oracle:thin:@your-oracle-host:1521/ORCL"
    table: "CUSTOMERS"
    primary_key: "CUST_ID"

  odoo:
    url: "https://your-odoo-instance.com"
    database: "your_database"
    model: "res.partner"

# Update matching rules for your use case
matching_rules:
  customer:
    exact_match_fields:
      - email
    fuzzy_match_fields:
      - name:
          algorithm: jaro_winkler
          threshold: 0.85
```

### Step 2: Upload Configuration to Databricks

```bash
# Upload configuration file
databricks workspace import config/mdm_config.yaml \
  /Workspace/Users/<your-email>/mdm/config/mdm_config.yaml \
  --format AUTO

# Upload environment template
databricks workspace import .env.template \
  /Workspace/Users/<your-email>/mdm/config/.env.template \
  --format AUTO
```

### Step 3: Set Environment Variables

Create `.env` file (DON'T commit this):

```bash
# Copy template
cp .env.template .env

# Edit .env with your actual values
nano .env

# Example content:
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=<your-token>
CATALOG_NAME=mdm_catalog
CHECKPOINT_PATH=/mnt/mdm/checkpoints
```

---

## Deploy and Run MDM Pipelines

### Step 1: Upload Notebooks to Workspace

```bash
# Upload all notebooks
databricks workspace import-dir \
  --overwrite \
  . \
  /Workspace/Users/<your-email>/mdm/

# Verify upload
databricks workspace list /Workspace/Users/<your-email>/mdm/
```

### Step 2: Create Cluster for MDM Workloads

```bash
# Create cluster using bundle
databricks bundle run mdm_cluster -t dev

# Or create manually via UI:
# - Cluster name: mdm-cluster
# - Databricks Runtime: 13.3 LTS or higher
# - Worker type: r3.xlarge (or equivalent)
# - Min workers: 2, Max workers: 8
# - Enable autoscaling
```

**Cluster Configuration (JSON):**
```json
{
  "cluster_name": "mdm-cluster",
  "spark_version": "13.3.x-scala2.12",
  "node_type_id": "r3.xlarge",
  "autoscale": {
    "min_workers": 2,
    "max_workers": 8
  },
  "spark_conf": {
    "spark.databricks.delta.preview.enabled": "true",
    "spark.databricks.delta.properties.defaults.enableChangeDataFeed": "true"
  },
  "custom_tags": {
    "project": "mdm",
    "environment": "dev"
  }
}
```

### Step 3: Run Data Ingestion

```bash
# Option 1: Run via bundle
databricks bundle run mdm_ingestion_job -t dev

# Option 2: Run notebook directly
databricks jobs run-now --notebook-path /Workspace/Users/<your-email>/mdm/01_ingestion/multi_source_connector

# Monitor job
databricks jobs list-runs --job-id <job-id>
```

**Run Ingestion Notebook Interactively:**
1. Open Databricks workspace
2. Navigate to `/Workspace/Users/<your-email>/mdm/01_ingestion/multi_source_connector`
3. Attach to `mdm-cluster`
4. Run all cells
5. Check bronze tables: `SELECT * FROM mdm_catalog.bronze.customer_bronze LIMIT 10`

### Step 4: Run Entity Matching

```bash
# Run matching job
databricks bundle run mdm_matching_job -t dev

# Or run notebook
databricks jobs run-now --notebook-path /Workspace/Users/<your-email>/mdm/02_matching/entity_resolution
```

### Step 5: Generate Golden Records

```bash
# Run golden record job
databricks bundle run mdm_golden_record_job -t dev

# Or run notebook
databricks jobs run-now --notebook-path /Workspace/Users/<your-email>/mdm/03_golden_record/survivorship
```

### Step 6: Run Complete MDM Pipeline

```bash
# Run full workflow
databricks bundle run mdm_pipeline -t dev

# This runs:
# 1. Ingestion
# 2. Matching
# 3. Golden Record creation
# 4. Data Quality checks
# 5. Catalog updates
```

---

## Verify Deployment

### Step 1: Check Tables Created

```sql
-- Run in Databricks SQL Editor
USE CATALOG mdm_catalog;

-- Check bronze tables
SHOW TABLES IN bronze;
SELECT COUNT(*) FROM bronze.customer_bronze;

-- Check silver tables
SHOW TABLES IN silver;
SELECT COUNT(*) FROM silver.customer_matched;

-- Check gold tables
SHOW TABLES IN gold;
SELECT COUNT(*) FROM gold.customer_golden;
```

### Step 2: Verify Data Quality

```python
# Run in notebook
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# Check data quality metrics
dq_metrics = spark.table("mdm_catalog.gold.data_quality_metrics")
display(dq_metrics.orderBy("check_timestamp", ascending=False).limit(10))

# Check golden records
golden_records = spark.table("mdm_catalog.gold.customer_golden")
display(golden_records.limit(100))
```

### Step 3: Test API Endpoints

```bash
# Start API server (if deployed)
databricks jobs run-now --notebook-path /Workspace/Users/<your-email>/mdm/api/mdm_api

# Test health check
curl -X GET "https://your-api-endpoint/health" \
  -H "Authorization: Bearer $DATABRICKS_TOKEN"

# Search entities
curl -X POST "https://your-api-endpoint/api/v1/entities/search" \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "entity_type": "customer",
    "search_criteria": {"name": "Acme Corp"},
    "match_threshold": 0.85
  }'
```

### Step 4: Check Job History

```bash
# List all job runs
databricks jobs list

# Get details of specific job
databricks jobs get --job-id <job-id>

# View run history
databricks jobs list-runs --job-id <job-id> --limit 10
```

---

## Schedule Recurring Jobs

### Option 1: Using Bundle Configuration

Add to `databricks.yml`:

```yaml
resources:
  jobs:
    mdm_daily_pipeline:
      name: "MDM Daily Pipeline"
      schedule:
        quartz_cron_expression: "0 0 2 * * ?"  # Run daily at 2 AM
        timezone_id: "America/Los_Angeles"
      tasks:
        - task_key: ingestion
          notebook_task:
            notebook_path: /Workspace/Users/<email>/mdm/workflows/mdm_pipeline
```

### Option 2: Using Databricks CLI

```bash
# Create scheduled job
databricks jobs create --json '{
  "name": "MDM Daily Pipeline",
  "schedule": {
    "quartz_cron_expression": "0 0 2 * * ?",
    "timezone_id": "America/Los_Angeles"
  },
  "tasks": [{
    "task_key": "mdm_pipeline",
    "notebook_task": {
      "notebook_path": "/Workspace/Users/<email>/mdm/workflows/mdm_pipeline"
    },
    "existing_cluster_id": "<cluster-id>"
  }]
}'
```

---

## Common Issues and Troubleshooting

### Issue 1: Authentication Errors

**Error**: "Authentication failed"

**Solution**:
```bash
# Re-authenticate
databricks auth login --host https://your-workspace.cloud.databricks.com

# Or regenerate token
# Go to User Settings → Access Tokens → Generate New Token
```

### Issue 2: Secrets Not Found

**Error**: "Secret not found: mdm-secrets/..."

**Solution**:
```bash
# Verify secret scope exists
databricks secrets list-scopes

# If missing, create it
databricks secrets create-scope mdm-secrets

# Add missing secrets
databricks secrets put-secret mdm-secrets <secret-name>
```

### Issue 3: Table Already Exists

**Error**: "Table 'customer_bronze' already exists"

**Solution**:
```sql
-- Option 1: Drop and recreate
DROP TABLE IF EXISTS mdm_catalog.bronze.customer_bronze;

-- Option 2: Use REPLACE
CREATE OR REPLACE TABLE mdm_catalog.bronze.customer_bronze ...
```

### Issue 4: Cluster Library Installation Fails

**Error**: "Library installation failed"

**Solution**:
```bash
# Install libraries manually
databricks libraries install \
  --cluster-id <cluster-id> \
  --pypi-package great-expectations

# Restart cluster
databricks clusters restart --cluster-id <cluster-id>
```

### Issue 5: JDBC Connection Failures

**Error**: "Connection refused"

**Solution**:
1. Check firewall rules allow Databricks IPs
2. Verify JDBC URL format:
   - SAP: `jdbc:sap://host:port`
   - Oracle: `jdbc:oracle:thin:@host:port/service`
3. Test connection:
```python
jdbc_url = "jdbc:oracle:thin:@host:1521/ORCL"
df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "(SELECT 1 FROM DUAL)") \
    .option("user", "username") \
    .option("password", "password") \
    .load()
df.show()
```

### Issue 6: Performance Issues

**Symptoms**: Jobs running slowly

**Solutions**:
1. **Increase cluster size**:
   ```bash
   databricks clusters edit --cluster-id <id> --num-workers 8
   ```

2. **Enable caching**:
   ```python
   df.cache()
   df.count()  # Force cache
   ```

3. **Partition large tables**:
   ```sql
   ALTER TABLE mdm_catalog.bronze.customer_bronze
   PARTITION BY (ingestion_date);
   ```

4. **Use Z-ordering**:
   ```sql
   OPTIMIZE mdm_catalog.gold.customer_golden
   ZORDER BY (customer_id);
   ```

---

## Next Steps

After successful deployment:

1. **Set Up Monitoring**:
   - Configure alerts for job failures
   - Set up dashboards for data quality metrics
   - Monitor data freshness

2. **Configure Data Quality Rules**:
   - Review and adjust quality thresholds
   - Add custom validation rules
   - Set up remediation workflows

3. **Enable Change Data Capture**:
   ```sql
   ALTER TABLE mdm_catalog.gold.customer_golden
   SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
   ```

4. **Set Up API Gateway**:
   - Deploy FastAPI endpoints
   - Configure authentication
   - Set up rate limiting

5. **Train Users**:
   - Data stewards on manual override UI
   - Analysts on querying golden records
   - Engineers on extending connectors

---

## Support and Resources

- **Documentation**: See `/docs` folder for detailed guides
- **Configuration**: Review `config/mdm_config.yaml` for all settings
- **Examples**: Check `examples/` for sample notebooks
- **Issues**: Report issues in your project repository

---

## Deployment Checklist

Use this checklist to track your deployment progress:

- [ ] Install prerequisites (Databricks CLI, Python, Git)
- [ ] Configure Databricks authentication
- [ ] Create Unity Catalog resources (catalog, schemas)
- [ ] Set up secret scopes and credentials
- [ ] Validate bundle configuration
- [ ] Deploy bundle to dev environment
- [ ] Upload configuration files
- [ ] Create MDM cluster
- [ ] Test source connections
- [ ] Run ingestion job successfully
- [ ] Run matching job successfully
- [ ] Generate golden records
- [ ] Verify data quality metrics
- [ ] Test API endpoints (if applicable)
- [ ] Schedule recurring jobs
- [ ] Set up monitoring and alerts
- [ ] Deploy to production
- [ ] Train users
- [ ] Document environment-specific settings

---

**Congratulations!** Your Databricks MDM solution is now deployed and running. 🎉
