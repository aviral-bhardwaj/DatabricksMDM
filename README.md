# Databricks MDM (Master Data Management)

Enterprise-grade Master Data Management solution built on Databricks platform with Unity Catalog integration.

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

### Prerequisites
- Databricks Workspace with Unity Catalog
- Python 3.8+
- Databricks CLI

### Installation

```bash
# Clone repository
git clone https://github.com/your-org/DatabricksMDM.git
cd DatabricksMDM

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.template .env
# Edit .env with your settings

# Deploy to Databricks
databricks bundle deploy --target dev
```

### Configuration

Edit `config/mdm_config.yaml` to configure source connections, matching rules, and survivorship strategies.

### Run MDM Pipeline

```bash
databricks bundle run mdm_pipeline
```

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