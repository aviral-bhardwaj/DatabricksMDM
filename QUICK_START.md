# Quick Start Guide - Databricks MDM

Get your MDM solution running in 15 minutes! ⚡

---

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- Databricks CLI installed
- Access to at least one source system (SAP, Salesforce, Oracle, or Odoo)

---

## 5-Minute Setup

### 1. Install & Authenticate (2 minutes)

```bash
# Install Databricks CLI
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

# Authenticate
databricks auth login --host https://YOUR-WORKSPACE.cloud.databricks.com

# Test connection
databricks workspace list /
```

### 2. Create Unity Catalog (1 minute)

```bash
# Create catalog and schemas
databricks unity-catalog catalogs create mdm_catalog
databricks unity-catalog schemas create mdm_catalog.bronze
databricks unity-catalog schemas create mdm_catalog.silver
databricks unity-catalog schemas create mdm_catalog.gold
```

### 3. Set Up Secrets (2 minutes)

```bash
# Create secret scope
databricks secrets create-scope mdm-secrets

# Add credentials (example for Salesforce)
databricks secrets put-secret mdm-secrets salesforce-username --string-value "your-email@company.com"
databricks secrets put-secret mdm-secrets salesforce-password --string-value "your-password"
databricks secrets put-secret mdm-secrets salesforce-security-token --string-value "your-token"
```

---

## 10-Minute Deployment

### 4. Clone & Configure (3 minutes)

```bash
# Clone repository
git clone <your-repo-url>
cd DatabricksMDM

# Update databricks.yml with your workspace URL
nano databricks.yml
# Change host to: https://YOUR-WORKSPACE.cloud.databricks.com

# Update config/mdm_config.yaml with your source connections
nano config/mdm_config.yaml
```

### 5. Deploy with Bundle (5 minutes)

```bash
# Validate configuration
databricks bundle validate -t dev

# Deploy everything
databricks bundle deploy -t dev

# This uploads notebooks, creates jobs, and configures clusters
```

### 6. Run Your First Pipeline (2 minutes)

```bash
# Start the MDM pipeline
databricks bundle run mdm_pipeline -t dev

# Monitor progress
databricks jobs list-runs --limit 5
```

---

## Verify It's Working

### Check Your Data

Open Databricks SQL Editor and run:

```sql
USE CATALOG mdm_catalog;

-- See ingested data
SELECT * FROM bronze.customer_bronze LIMIT 10;

-- See matched entities
SELECT * FROM silver.customer_matched LIMIT 10;

-- See golden records
SELECT * FROM gold.customer_golden LIMIT 10;
```

### Check Data Quality

```sql
-- View data quality metrics
SELECT * FROM gold.data_quality_metrics
ORDER BY check_timestamp DESC
LIMIT 10;
```

---

## What Just Happened?

✅ Created Unity Catalog with 3-tier architecture (Bronze/Silver/Gold)
✅ Set up secure credential storage
✅ Deployed all MDM notebooks and workflows
✅ Created compute clusters for processing
✅ Ran complete MDM pipeline:
   - 📥 Ingested data from source systems
   - 🔍 Matched duplicate entities using ML
   - ⭐ Created golden records with survivorship rules
   - 📊 Generated data quality reports

---

## Next Steps

### 1. Schedule Daily Runs

```bash
# Edit databricks.yml to add schedule
resources:
  jobs:
    mdm_pipeline:
      schedule:
        quartz_cron_expression: "0 0 2 * * ?"  # Daily at 2 AM
```

### 2. Explore the Data

**View Golden Records:**
```sql
SELECT
  master_entity_id,
  name,
  email,
  phone,
  source_systems,
  last_updated
FROM mdm_catalog.gold.customer_golden
WHERE is_manually_overridden = false
LIMIT 100;
```

**Check Match Quality:**
```sql
SELECT
  match_score,
  COUNT(*) as entity_count
FROM mdm_catalog.silver.customer_matched
GROUP BY match_score
ORDER BY match_score DESC;
```

### 3. Create Manual Overrides

```python
# Run in Databricks notebook
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# Import override manager
exec(open('/Workspace/Users/<your-email>/mdm/03_golden_record/survivorship.py').read())

# Create override manager
override_mgr = ManualOverrideManager(spark, catalog="mdm_catalog", schema="gold")

# Create an override
override_mgr.create_override(
    master_entity_id="CUST-001",
    field_name="email",
    override_value="corrected@email.com",
    override_by="data.steward@company.com",
    reason="Customer requested email correction"
)
```

### 4. Query via API (Optional)

If you deployed the API:

```bash
# Get entity by ID
curl -X GET "https://your-api-endpoint/api/v1/entities/customer/CUST-001" \
  -H "Authorization: Bearer YOUR-TOKEN"

# Search entities
curl -X POST "https://your-api-endpoint/api/v1/entities/search" \
  -H "Authorization: Bearer YOUR-TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "entity_type": "customer",
    "search_criteria": {"name": "Acme"},
    "match_threshold": 0.85
  }'
```

---

## Common Quick Fixes

### Can't Connect to Databricks?
```bash
# Re-authenticate
databricks auth login

# Check configuration
databricks configure show
```

### Job Failed?
```bash
# Check logs
databricks jobs get-run --run-id <run-id>

# View error details in workspace:
# Workflows → Select job → View run → Check logs
```

### No Data in Tables?
```sql
-- Check if tables exist
SHOW TABLES IN mdm_catalog.bronze;

-- Check table permissions
SHOW GRANTS ON TABLE mdm_catalog.bronze.customer_bronze;
```

### Source Connection Issues?

**Test JDBC connection:**
```python
# Run in notebook
jdbc_url = "jdbc:oracle:thin:@host:1521/ORCL"
test_df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "(SELECT 1 FROM DUAL)") \
    .option("user", dbutils.secrets.get("mdm-secrets", "oracle-user")) \
    .option("password", dbutils.secrets.get("mdm-secrets", "oracle-password")) \
    .load()
test_df.show()
```

**Test Salesforce:**
```python
from simple_salesforce import Salesforce

sf = Salesforce(
    username=dbutils.secrets.get("mdm-secrets", "salesforce-username"),
    password=dbutils.secrets.get("mdm-secrets", "salesforce-password"),
    security_token=dbutils.secrets.get("mdm-secrets", "salesforce-security-token")
)

# Test query
result = sf.query("SELECT Id, Name FROM Account LIMIT 5")
print(result)
```

---

## Performance Tips

### Speed Up Ingestion
```python
# Use partitioned reads for large tables
df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", table) \
    .option("partitionColumn", "id") \
    .option("lowerBound", 1) \
    .option("upperBound", 1000000) \
    .option("numPartitions", 10) \
    .load()
```

### Optimize Golden Records
```sql
-- Add Z-ordering for faster lookups
OPTIMIZE mdm_catalog.gold.customer_golden
ZORDER BY (master_entity_id, email);

-- Vacuum old versions (after 7 days)
VACUUM mdm_catalog.gold.customer_golden RETAIN 168 HOURS;
```

### Cache Frequently Used Tables
```python
# In your pipeline notebook
golden_df = spark.table("mdm_catalog.gold.customer_golden")
golden_df.cache()
golden_df.count()  # Force caching
```

---

## Getting Help

### View Logs
```bash
# Job run logs
databricks jobs get-run-output --run-id <run-id>

# Cluster logs
databricks clusters events --cluster-id <cluster-id>
```

### Debug Mode
Add to notebook cell:
```python
# Enable verbose logging
import logging
logging.basicConfig(level=logging.DEBUG)

# Show DataFrame schema
df.printSchema()

# Show execution plan
df.explain(True)
```

### Check System Status
```sql
-- Count records in each layer
SELECT 'Bronze' as layer, COUNT(*) as record_count FROM mdm_catalog.bronze.customer_bronze
UNION ALL
SELECT 'Silver', COUNT(*) FROM mdm_catalog.silver.customer_matched
UNION ALL
SELECT 'Gold', COUNT(*) FROM mdm_catalog.gold.customer_golden;
```

---

## Success Checklist

After following this guide, you should have:

✅ Unity Catalog with MDM schemas created
✅ Secure credential storage configured
✅ All notebooks deployed to workspace
✅ At least one successful pipeline run
✅ Data visible in Bronze, Silver, and Gold layers
✅ Data quality metrics generated
✅ Understanding of how to query golden records

---

## What's Next?

📚 **Read the full deployment guide**: [DEPLOYMENT_GUIDE.md](./DEPLOYMENT_GUIDE.md)
🔧 **Customize configurations**: Edit `config/mdm_config.yaml`
📊 **Build dashboards**: Use Databricks SQL or PowerBI
🔄 **Add more sources**: Extend connectors in `01_ingestion/`
⚙️ **Tune matching rules**: Update `config/mdm_config.yaml`
📈 **Monitor performance**: Set up alerts and metrics

---

**You're now running a production-ready MDM solution!** 🎉

For detailed documentation, see [DEPLOYMENT_GUIDE.md](./DEPLOYMENT_GUIDE.md)
