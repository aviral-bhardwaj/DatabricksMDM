#!/usr/bin/env python3
"""
Databricks MDM Setup Script
Initializes Unity Catalog, creates tables, and sets up the MDM environment
"""

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession
import os


def setup_mdm_environment():
    """
    Setup complete MDM environment in Databricks
    """
    print("🚀 Setting up Databricks MDM environment...")

    # Initialize clients
    w = WorkspaceClient()
    spark = SparkSession.builder.getOrCreate()

    # 1. Create Unity Catalog
    print("\n📚 Step 1: Creating Unity Catalog structure...")
    create_unity_catalog(spark)

    # 2. Create Delta tables
    print("\n📊 Step 2: Creating Delta tables...")
    create_delta_tables(spark)

    # 3. Enable CDC and lineage
    print("\n🔄 Step 3: Enabling Change Data Feed...")
    enable_cdc(spark)

    # 4. Create workflows
    print("\n⚙️  Step 4: Creating Databricks workflows...")
    create_workflows(w)

    print("\n✅ MDM environment setup complete!")
    print("\nNext steps:")
    print("1. Configure source connections in config/mdm_config.yaml")
    print("2. Update .env with your credentials")
    print("3. Run: databricks bundle run mdm_pipeline")


def create_unity_catalog(spark):
    """Create Unity Catalog and schemas"""
    catalog_name = "mdm_catalog"

    # Create catalog
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
    spark.sql(f"COMMENT ON CATALOG {catalog_name} IS 'Master Data Management Catalog'")

    # Create schemas
    schemas = ["bronze", "silver", "gold", "quality", "lineage", "audit"]

    for schema in schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema}")
        spark.sql(f"""
            COMMENT ON SCHEMA {catalog_name}.{schema}
            IS 'MDM {schema} layer - {get_schema_description(schema)}'
        """)

    print(f"✓ Created catalog: {catalog_name}")
    print(f"✓ Created schemas: {', '.join(schemas)}")


def get_schema_description(schema):
    """Get description for schema"""
    descriptions = {
        "bronze": "Raw data from source systems",
        "silver": "Matched and cleansed data",
        "gold": "Golden records and master data",
        "quality": "Data quality metrics and issues",
        "lineage": "Data lineage and transformation tracking",
        "audit": "Audit trail and change history"
    }
    return descriptions.get(schema, "")


def create_delta_tables(spark):
    """Create initial Delta tables"""

    # Bronze tables (examples)
    spark.sql("""
        CREATE TABLE IF NOT EXISTS mdm_catalog.bronze.customer_bronze (
            source_record_id STRING,
            source_system STRING,
            customer_name STRING,
            email STRING,
            phone STRING,
            address STRING,
            city STRING,
            state STRING,
            country STRING,
            postal_code STRING,
            ingestion_timestamp TIMESTAMP,
            ingestion_mode STRING
        )
        USING DELTA
        PARTITIONED BY (source_system)
        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    """)

    # Silver tables
    spark.sql("""
        CREATE TABLE IF NOT EXISTS mdm_catalog.silver.match_review_queue (
            review_id STRING,
            left_entity_id STRING,
            right_entity_id STRING,
            match_score DOUBLE,
            match_type STRING,
            review_status STRING,
            review_created_at TIMESTAMP,
            review_assigned_to STRING,
            review_completed_at TIMESTAMP,
            review_completed_by STRING,
            review_notes STRING
        )
        USING DELTA
        PARTITIONED BY (review_status)
    """)

    # Gold tables
    spark.sql("""
        CREATE TABLE IF NOT EXISTS mdm_catalog.gold.customer_golden (
            master_entity_id STRING,
            customer_name STRING,
            email STRING,
            phone STRING,
            address STRING,
            city STRING,
            state STRING,
            country STRING,
            postal_code STRING,
            source_systems ARRAY<STRING>,
            source_record_ids ARRAY<STRING>,
            last_updated TIMESTAMP,
            golden_record_created_at TIMESTAMP,
            is_manually_overridden BOOLEAN,
            override_history ARRAY<STRUCT<field:STRING,value:STRING,by:STRING,at:TIMESTAMP,reason:STRING>>
        )
        USING DELTA
        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    """)

    # Manual overrides table
    spark.sql("""
        CREATE TABLE IF NOT EXISTS mdm_catalog.gold.manual_overrides (
            master_entity_id STRING,
            field_name STRING,
            override_value STRING,
            override_by STRING,
            override_at TIMESTAMP,
            reason STRING,
            status STRING,
            removed_by STRING,
            removed_at TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (status)
    """)

    # Quality tables
    spark.sql("""
        CREATE TABLE IF NOT EXISTS mdm_catalog.quality.dq_metrics (
            entity_type STRING,
            total_records LONG,
            avg_quality_score DOUBLE,
            gold_records LONG,
            silver_records LONG,
            bronze_records LONG,
            metric_timestamp TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (entity_type)
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS mdm_catalog.quality.dq_issues (
            issue_id LONG,
            master_entity_id STRING,
            source_system STRING,
            source_record_id STRING,
            dq_score DOUBLE,
            failed_checks STRING,
            dq_check_timestamp TIMESTAMP,
            issue_status STRING,
            issue_severity STRING,
            assigned_to STRING,
            assigned_at TIMESTAMP,
            resolution_notes STRING
        )
        USING DELTA
        PARTITIONED BY (issue_status)
    """)

    # Lineage tables
    spark.sql("""
        CREATE TABLE IF NOT EXISTS mdm_catalog.lineage.data_lineage (
            source_table STRING,
            target_table STRING,
            transformation_type STRING,
            transformation_logic STRING,
            processed_records LONG,
            job_id STRING,
            execution_timestamp TIMESTAMP,
            executed_by STRING
        )
        USING DELTA
        PARTITIONED BY (transformation_type)
    """)

    # Audit tables
    spark.sql("""
        CREATE TABLE IF NOT EXISTS mdm_catalog.audit.audit_log (
            operation_type STRING,
            entity_type STRING,
            entity_id STRING,
            changed_fields STRING,
            old_values STRING,
            new_values STRING,
            performed_by STRING,
            operation_timestamp TIMESTAMP,
            reason STRING,
            metadata STRING
        )
        USING DELTA
        PARTITIONED BY (operation_type)
    """)

    print("✓ Created Delta tables in all layers")


def enable_cdc(spark):
    """Enable Change Data Feed on all tables"""
    tables = [
        "mdm_catalog.bronze.customer_bronze",
        "mdm_catalog.gold.customer_golden"
    ]

    for table in tables:
        spark.sql(f"""
            ALTER TABLE {table}
            SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
        """)

    print("✓ Enabled Change Data Feed on key tables")


def create_workflows(workspace_client):
    """Create Databricks workflows (placeholder)"""
    print("✓ Workflow creation skipped (use databricks bundle deploy)")


if __name__ == "__main__":
    setup_mdm_environment()
