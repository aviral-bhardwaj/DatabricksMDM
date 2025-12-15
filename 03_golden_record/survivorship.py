# Databricks notebook source
# MAGIC %md
# MAGIC # Golden Record Management
# MAGIC Configurable survivorship rules, source prioritization, and manual override capability

# COMMAND ----------

from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import *
from delta.tables import DeltaTable
from datetime import datetime


class GoldenRecordBuilder:
    """
    Build golden records using survivorship rules
    """

    def __init__(self, spark, survivorship_config):
        self.spark = spark
        self.config = survivorship_config

    def build_golden_record(self, matched_entities, entity_type):
        """
        Apply survivorship rules to create golden records
        """
        rules = self.config[entity_type]

        golden_df = matched_entities

        for field, rule in rules.items():
            golden_df = self._apply_survivorship_rule(golden_df, field, rule)

        # Aggregate to master entity level
        golden_records = self._aggregate_to_master(golden_df)

        return golden_records

    def _apply_survivorship_rule(self, df, field, rule):
        """
        Apply specific survivorship rule

        Rules:
        - MOST_RECENT: Latest non-null value
        - MOST_COMPLETE: Most complete value
        - SOURCE_PRIORITY: Based on source system priority
        - MOST_FREQUENT: Most frequently occurring value
        - LONGEST: Longest string value
        """

        if rule['strategy'] == 'MOST_RECENT':
            window = (Window.partitionBy("master_entity_id")
                      .orderBy(F.col("ingestion_timestamp").desc()))

            df = df.withColumn(
                f"golden_{field}",
                F.first(F.col(field), ignorenulls=True).over(window)
            )

        elif rule['strategy'] == 'SOURCE_PRIORITY':
            # Source priority: SAP > Salesforce > Others
            priority_map = rule['source_priority']

            window = (Window.partitionBy("master_entity_id")
                      .orderBy(F.col("source_priority_rank"),
                               F.col("ingestion_timestamp").desc()))

            df = (df.withColumn("source_priority_rank",
                                F.when(F.col("source_system") == "SAP", 1)
                                .when(F.col("source_system") == "Salesforce", 2)
                                .otherwise(3))
                  .withColumn(f"golden_{field}",
                              F.first(F.col(field), ignorenulls=True).over(window)))

        elif rule['strategy'] == 'MOST_COMPLETE':
            # Choose record with most non-null fields
            window = Window.partitionBy("master_entity_id")

            # Calculate completeness score
            all_fields = rule.get('completeness_fields', [field])
            completeness_expr = sum(
                F.when(F.col(f).isNotNull(), 1).otherwise(0)
                for f in all_fields
            )

            df = (df.withColumn("completeness_score", completeness_expr)
                  .withColumn(f"golden_{field}",
                              F.first(F.col(field), ignorenulls=True)
                              .over(window.orderBy(F.col("completeness_score").desc()))))

        return df

    def _aggregate_to_master(self, df):
        """
        Create final golden record per master entity
        """
        golden_cols = [c for c in df.columns if c.startswith("golden_")]

        golden_records = (df.groupBy("master_entity_id")
                          .agg(*[F.first(c).alias(c.replace("golden_", ""))
                                 for c in golden_cols],
                               F.collect_list("source_system").alias("source_systems"),
                               F.collect_list("source_record_id").alias("source_record_ids"),
                               F.max("ingestion_timestamp").alias("last_updated"))
                          .withColumn("golden_record_created_at", F.current_timestamp())
                          .withColumn("is_manually_overridden", F.lit(False))
                          .withColumn("override_history", F.array()))

        return golden_records

    def apply_manual_overrides(self, golden_df, overrides_table):
        """
        Apply manual overrides to golden records

        Manual overrides take precedence over all survivorship rules
        """
        # Load manual overrides
        overrides = self.spark.read.table(overrides_table)

        # Join with golden records
        for override_row in overrides.collect():
            master_id = override_row['master_entity_id']
            field = override_row['field_name']
            override_value = override_row['override_value']
            override_by = override_row['override_by']
            override_at = override_row['override_at']
            reason = override_row['reason']

            # Update golden record with override
            golden_df = golden_df.withColumn(
                field,
                F.when(F.col("master_entity_id") == master_id, F.lit(override_value))
                .otherwise(F.col(field))
            )

            # Track override history
            override_metadata = F.struct(
                F.lit(field).alias("field"),
                F.lit(override_value).alias("value"),
                F.lit(override_by).alias("by"),
                F.lit(override_at).alias("at"),
                F.lit(reason).alias("reason")
            )

            golden_df = golden_df.withColumn(
                "override_history",
                F.when(F.col("master_entity_id") == master_id,
                       F.array_union(F.col("override_history"), F.array(override_metadata)))
                .otherwise(F.col("override_history"))
            )

            golden_df = golden_df.withColumn(
                "is_manually_overridden",
                F.when(F.col("master_entity_id") == master_id, True)
                .otherwise(F.col("is_manually_overridden"))
            )

        return golden_df

    def save_golden_records(self, golden_df, catalog, schema, entity_type):
        """
        Save golden records to Unity Catalog with CDC enabled
        """
        table_name = f"{catalog}.{schema}.{entity_type}_golden"

        # Use merge to handle updates
        if DeltaTable.isDeltaTable(self.spark, table_name):
            delta_table = DeltaTable.forName(self.spark, table_name)

            delta_table.alias("target").merge(
                golden_df.alias("source"),
                "target.master_entity_id = source.master_entity_id"
            ).whenMatchedUpdate(set={
                "last_updated": "source.last_updated",
                **{col: f"source.{col}" for col in golden_df.columns if col != "master_entity_id"}
            }).whenNotMatchedInsertAll().execute()

        else:
            # First time creation
            golden_df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("delta.enableChangeDataFeed", "true") \
                .option("delta.columnMapping.mode", "name") \
                .saveAsTable(table_name)

        print(f"Saved {golden_df.count()} golden records to {table_name}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Manual Override Manager

# COMMAND ----------

class ManualOverrideManager:
    """
    Manage manual overrides for golden records
    Allows data stewards to override system-generated values
    """

    def __init__(self, spark, catalog="mdm_catalog", schema="gold"):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.overrides_table = f"{catalog}.{schema}.manual_overrides"

    def create_override(self, master_entity_id, field_name, override_value, override_by, reason):
        """
        Create a manual override for a golden record field
        """
        override_data = [(
            master_entity_id,
            field_name,
            str(override_value),
            override_by,
            datetime.now(),
            reason,
            "ACTIVE"
        )]

        schema = StructType([
            StructField("master_entity_id", StringType(), False),
            StructField("field_name", StringType(), False),
            StructField("override_value", StringType(), False),
            StructField("override_by", StringType(), False),
            StructField("override_at", TimestampType(), False),
            StructField("reason", StringType(), True),
            StructField("status", StringType(), False)
        ])

        override_df = self.spark.createDataFrame(override_data, schema)

        # Append to overrides table
        override_df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(self.overrides_table)

        print(f"Created override for {master_entity_id}.{field_name}")

        # Trigger golden record refresh
        self._trigger_golden_record_refresh(master_entity_id)

    def remove_override(self, master_entity_id, field_name, removed_by):
        """
        Remove a manual override (soft delete)
        """
        delta_table = DeltaTable.forName(self.spark, self.overrides_table)

        delta_table.update(
            condition=(F.col("master_entity_id") == master_entity_id) &
                     (F.col("field_name") == field_name) &
                     (F.col("status") == "ACTIVE"),
            set={
                "status": F.lit("REMOVED"),
                "removed_by": F.lit(removed_by),
                "removed_at": F.current_timestamp()
            }
        )

        print(f"Removed override for {master_entity_id}.{field_name}")

        # Trigger golden record refresh
        self._trigger_golden_record_refresh(master_entity_id)

    def get_overrides(self, master_entity_id=None):
        """
        Get all active overrides, optionally filtered by master entity ID
        """
        query = f"SELECT * FROM {self.overrides_table} WHERE status = 'ACTIVE'"

        if master_entity_id:
            query += f" AND master_entity_id = '{master_entity_id}'"

        return self.spark.sql(query)

    def get_override_history(self, master_entity_id, field_name=None):
        """
        Get override history for an entity
        """
        query = f"""
        SELECT *
        FROM {self.overrides_table}
        WHERE master_entity_id = '{master_entity_id}'
        """

        if field_name:
            query += f" AND field_name = '{field_name}'"

        query += " ORDER BY override_at DESC"

        return self.spark.sql(query)

    def _trigger_golden_record_refresh(self, master_entity_id):
        """
        Trigger golden record recalculation after override change
        """
        # This would trigger the golden record rebuild for the specific entity
        print(f"Triggering golden record refresh for {master_entity_id}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Advanced Survivorship Strategies

# COMMAND ----------

class AdvancedSurvivorshipRules:
    """
    Additional survivorship strategies beyond basic rules
    """

    @staticmethod
    def longest_value(df, field, partition_col="master_entity_id"):
        """
        Select longest string value
        """
        window = Window.partitionBy(partition_col).orderBy(F.length(F.col(field)).desc())
        return df.withColumn(
            f"golden_{field}",
            F.first(F.col(field), ignorenulls=True).over(window)
        )

    @staticmethod
    def most_frequent(df, field, partition_col="master_entity_id"):
        """
        Select most frequently occurring value
        """
        # Count occurrences of each value
        window_count = Window.partitionBy(partition_col, field)
        window_rank = Window.partitionBy(partition_col).orderBy(F.desc("value_count"))

        return (df.withColumn("value_count", F.count(field).over(window_count))
                  .withColumn("rank", F.row_number().over(window_rank))
                  .withColumn(f"golden_{field}",
                              F.when(F.col("rank") == 1, F.col(field)))
                  .drop("value_count", "rank"))

    @staticmethod
    def weighted_average(df, field, source_weights, partition_col="master_entity_id"):
        """
        Calculate weighted average based on source reliability scores
        Useful for numeric fields
        """
        # Add weights based on source
        df_weighted = df

        for source, weight in source_weights.items():
            df_weighted = df_weighted.withColumn(
                "source_weight",
                F.when(F.col("source_system") == source, weight)
                .otherwise(F.col("source_weight"))
            )

        # Calculate weighted average
        window = Window.partitionBy(partition_col)

        df_weighted = df_weighted.withColumn(
            f"golden_{field}",
            F.sum(F.col(field) * F.col("source_weight")).over(window) /
            F.sum(F.col("source_weight")).over(window)
        )

        return df_weighted

    @staticmethod
    def consensus_value(df, field, min_agreement=2, partition_col="master_entity_id"):
        """
        Select value that appears in at least N sources (consensus)
        """
        window_count = Window.partitionBy(partition_col, field)
        window_rank = Window.partitionBy(partition_col).orderBy(F.desc("source_count"))

        return (df.withColumn("source_count",
                              F.count(F.col("source_system")).over(window_count))
                  .filter(F.col("source_count") >= min_agreement)
                  .withColumn("rank", F.row_number().over(window_rank))
                  .withColumn(f"golden_{field}",
                              F.when(F.col("rank") == 1, F.col(field)))
                  .drop("source_count", "rank"))

    @staticmethod
    def custom_rule(df, field, rule_function, partition_col="master_entity_id"):
        """
        Apply custom user-defined survivorship rule
        """
        window = Window.partitionBy(partition_col)

        # Apply custom rule function
        df_result = rule_function(df, field, window)

        return df_result


# COMMAND ----------

# MAGIC %md
# MAGIC ## Source Priority Configuration

# COMMAND ----------

class SourcePriorityManager:
    """
    Manage source system priority rankings
    """

    def __init__(self, spark, catalog="mdm_catalog", schema="gold"):
        self.spark = spark
        self.priority_table = f"{catalog}.{schema}.source_priorities"

    def set_source_priority(self, entity_type, source_priorities):
        """
        Set source priority for an entity type

        Args:
            entity_type: customer, product, etc.
            source_priorities: dict like {"SAP": 1, "Salesforce": 2, "Oracle": 3}
        """
        priority_data = [
            (entity_type, source, priority)
            for source, priority in source_priorities.items()
        ]

        schema = StructType([
            StructField("entity_type", StringType(), False),
            StructField("source_system", StringType(), False),
            StructField("priority_rank", IntegerType(), False)
        ])

        priority_df = self.spark.createDataFrame(priority_data, schema)

        # Merge with existing priorities
        priority_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .option("partitionBy", "entity_type") \
            .saveAsTable(self.priority_table)

    def get_source_priority(self, entity_type):
        """
        Get source priority configuration for entity type
        """
        return self.spark.sql(f"""
            SELECT source_system, priority_rank
            FROM {self.priority_table}
            WHERE entity_type = '{entity_type}'
            ORDER BY priority_rank
        """)

    def apply_priority(self, df, entity_type, field):
        """
        Apply source priority to select field value
        """
        priorities = self.get_source_priority(entity_type)

        # Create priority mapping
        priority_map = {row['source_system']: row['priority_rank']
                        for row in priorities.collect()}

        # Add priority rank column
        df_with_priority = df

        for source, rank in priority_map.items():
            df_with_priority = df_with_priority.withColumn(
                "priority_rank",
                F.when(F.col("source_system") == source, rank)
                .otherwise(F.col("priority_rank"))
            )

        # Select value from highest priority source
        window = (Window.partitionBy("master_entity_id")
                  .orderBy(F.col("priority_rank"), F.col("ingestion_timestamp").desc()))

        df_with_priority = df_with_priority.withColumn(
            f"golden_{field}",
            F.first(F.col(field), ignorenulls=True).over(window)
        )

        return df_with_priority