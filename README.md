# Databricks MDM (Master Data Management)

Enterprise-grade Master Data Management solution built on Databricks platform with Unity Catalog integration.

> 📐 **Architecture Blueprint:** A deep, opinionated, SOLID-based blueprint for building a *truly Databricks-native* MDM product (market research, anti-patterns, layer→service mapping, domain model, roadmap) is published as a static website under [`/docs`](docs/). It is served via **GitHub Pages** (`main` branch `/docs` folder) at **https://aviral-bhardwaj.github.io/databricksmdm/** once merged to `main`.

## Overview

Databricks MDM is a comprehensive master data management solution providing multi-source integration, intelligent entity matching, golden record management, data quality monitoring, and full audit capabilities.

## Features

### ✅ Multi-Source Integration Hub
- Pre-built connectors: SAP, Salesforce, Oracle, Odoo
- Custom connector framework
- Real-time streaming (Kafka/Kinesis) and batch ingestion

### ✅ Intelligent Entity Matching
- ML-powered fuzzy matching with Random Forest
- Configurable matching rules
- Manual match review UI
- Auto-merge with configurable thresholds

### ✅ Golden Record Management
- Configurable survivorship rules (Most Recent, Source Priority, Most Complete, etc.)
- Source prioritization
- Manual override capability with full audit trail

### ✅ Data Quality Dashboard
- Real-time DQ metrics
- Quality score tracking (Gold/Silver/Bronze)
- Issue remediation workflows
- Automated alerts

### ✅ Master Data Catalog
- Unity Catalog integration
- Full data lineage tracking
- Change history and audit trail
- PII/sensitive data tagging

### ✅ API & SDK
- Complete RESTful API
- Python SDK
- Webhook support for real-time events

## Quick Start

⚡ **New to MDM?** Follow our [Quick Start Guide](./QUICK_START.md) to get running in 15 minutes!

📚 **Production Deployment?** See the complete [Deployment Guide](./DEPLOYMENT_GUIDE.md) for step-by-step instructions.

### Prerequisites
- Databricks Workspace with Unity Catalog
- Python 3.8+
- Databricks CLI

### 5-Minute Installation

```bash
# 0. Install Databricks CLI
curl  https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh 

# 1. Check version
databricks -v

# 2. Authenticate
databricks auth login --host <YOUR DATABRICKS HOSTNAME LIKE THIS  https://dbc-XXXXXXX-fa92.cloud.databricks.com>

# 3. Clone repository
git clone https://github.com/aviral-bhardwaj/DatabricksMDM.git

# 4. Create catalog with storage root (ONLY way that worked)
databricks catalogs create mdm_catalog -t dev --storage-root "YOUR STORAGE LOCATION LIKE THIS s3://databricks-xxxxxxx/unity-catalog/XXXXXXX "

# 5. Create schemas - THE KEY SYNTAX (NAME then CATALOG_NAME, then -t TARGET)
databricks schemas create bronze mdm_catalog -t dev
databricks schemas create silver mdm_catalog -t dev
databricks schemas create gold mdm_catalog -t dev

# 5. Deploy
databricks bundle deploy --target dev

# 6. Run your first pipeline
databricks bundle run mdm_pipeline --target dev
```

✅ **Done!** Check your data: `SELECT * FROM mdm_catalog.gold.customer_golden LIMIT 10;`

## Project Structure

```
DatabricksMDM/
├── 01_ingestion/          # Multi-source connectors
├── 02_matching/           # Entity resolution & ML matching
├── 03_golden_record/      # Survivorship rules & overrides
├── 04_quality/            # Data quality framework
├── 05_catalog/            # Unity Catalog & lineage
├── api/                   # RESTful API
├── sdk/                   # Python SDK
├── workflows/             # Databricks workflows
├── config/                # Configuration
├── requirements.txt
├── databricks.yml
└── README.md
```

## API Usage

```python
from sdk.mdm_python_sdk import MDMClient

client = MDMClient(
    api_url="https://your-api.com",
    api_key="your-key"
)

# Search entities
results = client.search_entities(
    entity_type="customer",
    search_criteria={"email": "john@example.com"}
)

# Create override
client.create_override(
    master_id="MDM_123",
    field_name="email",
    override_value="new@example.com",
    reason="Customer request"
)
```

## Databricks Deployment

This repository is ready to import into Databricks:

1. **Import via Git**: Use Databricks Repos to import this repository
2. **Run Notebooks**: All Python files are formatted as Databricks notebooks
3. **Execute Workflows**: Use `workflows/mdm_pipeline.py` to create jobs
4. **Unity Catalog**: Automatically creates `mdm_catalog` with proper schemas

## Support

- Documentation: See inline code comments
- Issues: Create GitHub issue
- Email: mdm-support@your-company.com

## License

Proprietary - All rights reserved
