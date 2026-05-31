# Databricks notebook source
# MAGIC %md
# MAGIC # Master Data Catalog
# MAGIC Unity Catalog integration, lineage tracking, change history, and audit trail

# COMMAND ----------

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import *
import json
from datetime import datetime


class UnityCatalogManager:
    """
    Manage MDM data in Unity Catalog with full lineage and governance
    """

    def __init__(self, catalog_name="mdm_catalog"):
        self.spark = SparkSession.builder.getOrCreate()
        self.workspace = WorkspaceClient()
        self.catalog_name = catalog_name

    def create_mdm_catalog(self):
        """
        Create Unity Catalog structure for MDM
        """
        # Create catalog
        try:
            self.workspace.catalogs.create(
                name=self.catalog_name,
                comment="Master Data Management Catalog",
                properties={"purpose": "MDM", "owner": "mdm-team"}
            )
            print(f"Created catalog: {self.catalog_name}")
        except Exception as e:
            print(f"Catalog may already exist: {e}")

        # Create schemas
        schemas = ["bronze", "silver", "gold", "quality", "lineage", "audit"]

        for schema in schemas:
            try:
                self.workspace.schemas.create(
                    name=schema,
                    catalog_name=self.catalog_name,
                    comment=f"MDM {schema} layer"
                )
                print(f"Created schema: {self.catalog_name}.{schema}")
            except Exception as e:
                print(f"Schema may already exist: {e}")

    def register_table(self, table_name, schema, entity_type, description, tags=None):
        """
        Register a table in Unity Catalog with metadata
        """
        full_table_name = f"{self.catalog_name}.{schema}.{table_name}"

        # Add table properties
        properties = {
            "entity_type": entity_type,
            "mdm_layer": schema,
            "created_by": "mdm_system",
            "created_at": datetime.now().isoformat()
        }

        if tags:
            properties.update(tags)

        # Update table properties
        self.spark.sql(f"""
            ALTER TABLE {full_table_name}
            SET TBLPROPERTIES (
                {', '.join([f"'{k}' = '{v}'" for k, v in properties.items()])}
            )
        """)

        # Add table comment
        self.spark.sql(f"""
            COMMENT ON TABLE {full_table_name} IS '{description}'
        """)

        print(f"Registered table: {full_table_name}")

    def tag_sensitive_columns(self, table_name, schema, sensitive_columns):
        """
        Tag columns containing PII/sensitive data
        """
        full_table_name = f"{self.catalog_name}.{schema}.{table_name}"

        for column, classification in sensitive_columns.items():
            self.spark.sql(f"""
                ALTER TABLE {full_table_name}
                ALTER COLUMN {column}
                SET TAGS ('classification' = '{classification}')
            """)

            # Add column comment
            self.spark.sql(f"""
                COMMENT ON COLUMN {full_table_name}.{column}
                IS 'Sensitive data - {classification}'
            """)

        print(f"Tagged {len(sensitive_columns)} sensitive columns in {full_table_name}")

    def setup_row_level_security(self, table_name, schema, filter_function):
        """
        Setup row-level security policies
        """
        full_table_name = f"{self.catalog_name}.{schema}.{table_name}"

        # Create row filter function
        self.spark.sql(f"""
            CREATE OR REPLACE FUNCTION {self.catalog_name}.{schema}.{filter_function}_filter(region STRING)
            RETURN SELECT * FROM {full_table_name} WHERE region = current_user()
        """)

        print(f"Created row filter for {full_table_name}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Lineage Tracking

# COMMAND ----------

class LineageTracker:
    """
    Track data lineage across MDM pipeline
    """

    def __init__(self, spark, catalog="mdm_catalog", schema="lineage"):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.lineage_table = f"{catalog}.{schema}.data_lineage"

    def record_lineage(self, source_table, target_table, transformation_type,
                      transformation_logic, processed_records, job_id=None):
        """
        Record lineage relationship between tables
        """
        lineage_data = [(
            source_table,
            target_table,
            transformation_type,
            transformation_logic,
            processed_records,
            job_id,
            datetime.now(),
            self._get_current_user()
        )]

        schema = StructType([
            StructField("source_table", StringType(), False),
            StructField("target_table", StringType(), False),
            StructField("transformation_type", StringType(), False),
            StructField("transformation_logic", StringType(), True),
            StructField("processed_records", LongType(), False),
            StructField("job_id", StringType(), True),
            StructField("execution_timestamp", TimestampType(), False),
            StructField("executed_by", StringType(), False)
        ])

        lineage_df = self.spark.createDataFrame(lineage_data, schema)

        lineage_df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(self.lineage_table)

    def get_table_lineage(self, table_name, direction="both", depth=5):
        """
        Get lineage graph for a table

        Args:
            table_name: Table to get lineage for
            direction: "upstream", "downstream", or "both"
            depth: How many levels to traverse
        """
        if direction in ["upstream", "both"]:
            upstream = self._get_upstream_lineage(table_name, depth)
        else:
            upstream = self.spark.createDataFrame([], StructType([]))

        if direction in ["downstream", "both"]:
            downstream = self._get_downstream_lineage(table_name, depth)
        else:
            downstream = self.spark.createDataFrame([], StructType([]))

        return {
            "upstream": upstream,
            "downstream": downstream
        }

    def _get_upstream_lineage(self, table_name, depth):
        """
        Get upstream lineage (sources of this table)
        """
        query = f"""
        WITH RECURSIVE lineage_cte (source_table, target_table, level) AS (
            SELECT source_table, target_table, 1 as level
            FROM {self.lineage_table}
            WHERE target_table = '{table_name}'

            UNION ALL

            SELECT l.source_table, l.target_table, c.level + 1
            FROM {self.lineage_table} l
            INNER JOIN lineage_cte c ON l.target_table = c.source_table
            WHERE c.level < {depth}
        )
        SELECT DISTINCT source_table, target_table, level
        FROM lineage_cte
        ORDER BY level, source_table
        """

        return self.spark.sql(query)

    def _get_downstream_lineage(self, table_name, depth):
        """
        Get downstream lineage (consumers of this table)
        """
        query = f"""
        WITH RECURSIVE lineage_cte (source_table, target_table, level) AS (
            SELECT source_table, target_table, 1 as level
            FROM {self.lineage_table}
            WHERE source_table = '{table_name}'

            UNION ALL

            SELECT l.source_table, l.target_table, c.level + 1
            FROM {self.lineage_table} l
            INNER JOIN lineage_cte c ON l.source_table = c.target_table
            WHERE c.level < {depth}
        )
        SELECT DISTINCT source_table, target_table, level
        FROM lineage_cte
        ORDER BY level, target_table
        """

        return self.spark.sql(query)

    def get_column_lineage(self, table_name, column_name):
        """
        Get column-level lineage
        """
        column_lineage_table = f"{self.catalog}.{self.schema}.column_lineage"

        query = f"""
        SELECT
            source_table,
            source_column,
            target_table,
            target_column,
            transformation_logic,
            execution_timestamp
        FROM {column_lineage_table}
        WHERE target_table = '{table_name}'
          AND target_column = '{column_name}'
        ORDER BY execution_timestamp DESC
        """

        return self.spark.sql(query)

    def visualize_lineage(self, table_name):
        """
        Generate lineage visualization data for UI
        """
        lineage = self.get_table_lineage(table_name)

        # Convert to graph format for visualization
        nodes = []
        edges = []

        # Add upstream nodes and edges
        for row in lineage['upstream'].collect():
            nodes.append({"id": row['source_table'], "type": "source"})
            edges.append({
                "from": row['source_table'],
                "to": row['target_table'],
                "level": row['level']
            })

        # Add target node
        nodes.append({"id": table_name, "type": "target"})

        # Add downstream nodes and edges
        for row in lineage['downstream'].collect():
            nodes.append({"id": row['target_table'], "type": "consumer"})
            edges.append({
                "from": row['source_table'],
                "to": row['target_table'],
                "level": row['level']
            })

        return {
            "nodes": nodes,
            "edges": edges
        }

    def _get_current_user(self):
        """Get current Databricks user"""
        try:
            return self.spark.sql("SELECT current_user() as user").collect()[0]['user']
        except:
            return "system"


# COMMAND ----------

# MAGIC %md
# MAGIC ## Change History & Audit Trail

# COMMAND ----------

class AuditTrailManager:
    """
    Comprehensive audit trail for all MDM operations
    """

    def __init__(self, spark, catalog="mdm_catalog", schema="audit"):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.audit_table = f"{catalog}.{schema}.audit_log"

    def log_operation(self, operation_type, entity_type, entity_id,
                     changed_fields=None, old_values=None, new_values=None,
                     performed_by=None, reason=None, metadata=None):
        """
        Log an MDM operation to audit trail
        """
        audit_data = [(
            operation_type,  # CREATE, UPDATE, DELETE, MERGE, SPLIT
            entity_type,
            entity_id,
            json.dumps(changed_fields) if changed_fields else None,
            json.dumps(old_values) if old_values else None,
            json.dumps(new_values) if new_values else None,
            performed_by or self._get_current_user(),
            datetime.now(),
            reason,
            json.dumps(metadata) if metadata else None
        )]

        schema = StructType([
            StructField("operation_type", StringType(), False),
            StructField("entity_type", StringType(), False),
            StructField("entity_id", StringType(), False),
            StructField("changed_fields", StringType(), True),
            StructField("old_values", StringType(), True),
            StructField("new_values", StringType(), True),
            StructField("performed_by", StringType(), False),
            StructField("operation_timestamp", TimestampType(), False),
            StructField("reason", StringType(), True),
            StructField("metadata", StringType(), True)
        ])

        audit_df = self.spark.createDataFrame(audit_data, schema)

        audit_df.write \
            .format("delta") \
            .mode("append") \
            .partitionBy("operation_type") \
            .saveAsTable(self.audit_table)

    def get_entity_history(self, entity_id, limit=100):
        """
        Get complete change history for an entity
        """
        query = f"""
        SELECT
            operation_type,
            changed_fields,
            old_values,
            new_values,
            performed_by,
            operation_timestamp,
            reason
        FROM {self.audit_table}
        WHERE entity_id = '{entity_id}'
        ORDER BY operation_timestamp DESC
        LIMIT {limit}
        """

        return self.spark.sql(query)

    def get_user_activity(self, user_email, days=30):
        """
        Get all operations performed by a user
        """
        query = f"""
        SELECT
            operation_type,
            entity_type,
            entity_id,
            operation_timestamp,
            reason
        FROM {self.audit_table}
        WHERE performed_by = '{user_email}'
          AND operation_timestamp >= CURRENT_DATE - INTERVAL '{days}' DAY
        ORDER BY operation_timestamp DESC
        """

        return self.spark.sql(query)

    def get_compliance_report(self, start_date, end_date):
        """
        Generate compliance audit report
        """
        query = f"""
        SELECT
            operation_type,
            entity_type,
            COUNT(*) as operation_count,
            COUNT(DISTINCT entity_id) as entities_affected,
            COUNT(DISTINCT performed_by) as unique_users
        FROM {self.audit_table}
        WHERE operation_timestamp BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY operation_type, entity_type
        ORDER BY operation_count DESC
        """

        return self.spark.sql(query)

    def enable_time_travel(self, table_name, schema="gold"):
        """
        Enable Delta Lake time travel for auditing
        """
        full_table_name = f"{self.catalog}.{schema}.{table_name}"

        # Set retention period (e.g., 90 days)
        self.spark.sql(f"""
            ALTER TABLE {full_table_name}
            SET TBLPROPERTIES (
                delta.logRetentionDuration = '90 days',
                delta.deletedFileRetentionDuration = '90 days'
            )
        """)

        print(f"Enabled time travel for {full_table_name}")

    def query_historical_data(self, table_name, schema, version=None, timestamp=None):
        """
        Query historical version of data
        """
        full_table_name = f"{self.catalog}.{schema}.{table_name}"

        if version:
            return self.spark.read.format("delta").option("versionAsOf", version).table(full_table_name)
        elif timestamp:
            return self.spark.read.format("delta").option("timestampAsOf", timestamp).table(full_table_name)
        else:
            raise ValueError("Must provide either version or timestamp")

    def _get_current_user(self):
        """Get current Databricks user"""
        try:
            return self.spark.sql("SELECT current_user() as user").collect()[0]['user']
        except:
            return "system"


# COMMAND ----------

# MAGIC %md
# MAGIC ## Change Data Feed Integration

# COMMAND ----------

class ChangeFeedManager:
    """
    Manage Change Data Feed for downstream consumers
    """

    def __init__(self, spark, catalog="mdm_catalog"):
        self.spark = spark
        self.catalog = catalog

    def enable_cdf(self, table_name, schema):
        """
        Enable Change Data Feed on a table
        """
        full_table_name = f"{self.catalog}.{schema}.{table_name}"

        self.spark.sql(f"""
            ALTER TABLE {full_table_name}
            SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
        """)

        print(f"Enabled Change Data Feed for {full_table_name}")

    def get_changes(self, table_name, schema, start_version, end_version=None):
        """
        Get changes from Change Data Feed
        """
        full_table_name = f"{self.catalog}.{schema}.{table_name}"

        if end_version:
            changes = (self.spark.read
                      .format("delta")
                      .option("readChangeData", "true")
                      .option("startingVersion", start_version)
                      .option("endingVersion", end_version)
                      .table(full_table_name))
        else:
            changes = (self.spark.read
                      .format("delta")
                      .option("readChangeData", "true")
                      .option("startingVersion", start_version)
                      .table(full_table_name))

        return changes

    def stream_changes(self, table_name, schema):
        """
        Stream changes in real-time
        """
        full_table_name = f"{self.catalog}.{schema}.{table_name}"

        changes_stream = (self.spark.readStream
                         .format("delta")
                         .option("readChangeData", "true")
                         .option("startingVersion", "latest")
                         .table(full_table_name))

        return changes_stream

    def publish_changes_to_kafka(self, table_name, schema, kafka_config):
        """
        Publish changes to Kafka for downstream systems
        """
        changes_stream = self.stream_changes(table_name, schema)

        # Convert to JSON
        changes_json = changes_stream.selectExpr("to_json(struct(*)) as value")

        # Write to Kafka
        query = (changes_json.writeStream
                .format("kafka")
                .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'])
                .option("topic", kafka_config['topic'])
                .option("checkpointLocation", kafka_config['checkpoint_path'])
                .start())

        return query
