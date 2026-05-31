# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Dashboard
# MAGIC Real-time DQ metrics, quality score tracking, and issue remediation workflows

# COMMAND ----------

from great_expectations.dataset import SparkDFDataset
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import *
from delta.tables import DeltaTable
from datetime import datetime
import json


class MDMDataQuality:
    """
    Data quality framework for MDM
    """

    def __init__(self, spark):
        self.spark = spark

    def apply_dq_checks(self, df, entity_type):
        """
        Apply data quality rules
        """
        ge_df = SparkDFDataset(df)

        # Define expectations based on entity type
        if entity_type == "customer":
            # Email format validation
            ge_df.expect_column_values_to_match_regex(
                "email",
                r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
            )

            # Phone number validation
            ge_df.expect_column_values_to_match_regex(
                "phone",
                r"^\+?1?\d{9,15}$"
            )

            # Required fields
            ge_df.expect_column_values_to_not_be_null("customer_name")
            ge_df.expect_column_values_to_not_be_null("email")

            # Uniqueness
            ge_df.expect_column_values_to_be_unique("email")

        # Get validation results
        results = ge_df.validate()

        # Flag records with quality issues
        df_with_dq = self._flag_quality_issues(df, results)

        return df_with_dq, results

    def _flag_quality_issues(self, df, validation_results):
        """Add data quality flags to dataframe"""

        # Create quality score based on validation results
        passed_checks = sum(1 for result in validation_results.results
                            if result.success)
        total_checks = len(validation_results.results)

        df = df.withColumn(
            "dq_score",
            F.lit(passed_checks / total_checks if total_checks > 0 else 0)
        )

        df = df.withColumn(
            "dq_status",
            F.when(F.col("dq_score") >= 0.9, "GOLD")
            .when(F.col("dq_score") >= 0.7, "SILVER")
            .otherwise("BRONZE")
        )

        # Add failed checks details
        failed_checks = [
            result.expectation_config.kwargs.get('column', 'general')
            for result in validation_results.results
            if not result.success
        ]

        df = df.withColumn("failed_checks", F.lit(json.dumps(failed_checks)))
        df = df.withColumn("dq_check_timestamp", F.current_timestamp())

        return df

    def track_quality_metrics(self, df, entity_type, catalog="mdm_catalog", schema="quality"):
        """
        Track data quality metrics over time
        """
        metrics_table = f"{catalog}.{schema}.dq_metrics"

        # Aggregate quality metrics
        metrics = df.agg(
            F.count("*").alias("total_records"),
            F.avg("dq_score").alias("avg_quality_score"),
            F.sum(F.when(F.col("dq_status") == "GOLD", 1).otherwise(0)).alias("gold_records"),
            F.sum(F.when(F.col("dq_status") == "SILVER", 1).otherwise(0)).alias("silver_records"),
            F.sum(F.when(F.col("dq_status") == "BRONZE", 1).otherwise(0)).alias("bronze_records"),
            F.current_timestamp().alias("metric_timestamp")
        ).withColumn("entity_type", F.lit(entity_type))

        # Save metrics
        metrics.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(metrics_table)

        return metrics

    def identify_quality_issues(self, df, catalog="mdm_catalog", schema="quality"):
        """
        Identify and log specific data quality issues for remediation
        """
        issues_table = f"{catalog}.{schema}.dq_issues"

        # Find records with quality issues
        quality_issues = df.filter(F.col("dq_score") < 0.9)

        # Explode failed checks
        issues_detailed = quality_issues.selectExpr(
            "master_entity_id",
            "source_system",
            "source_record_id",
            "dq_score",
            "failed_checks",
            "dq_check_timestamp"
        )

        # Add issue metadata
        issues_detailed = (issues_detailed
                          .withColumn("issue_id", F.monotonically_increasing_id())
                          .withColumn("issue_status", F.lit("OPEN"))
                          .withColumn("issue_severity",
                                    F.when(F.col("dq_score") < 0.5, "CRITICAL")
                                    .when(F.col("dq_score") < 0.7, "HIGH")
                                    .otherwise("MEDIUM"))
                          .withColumn("assigned_to", F.lit(None).cast(StringType()))
                          .withColumn("resolution_notes", F.lit(None).cast(StringType())))

        # Save issues
        issues_detailed.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(issues_table)

        return issues_detailed

    def create_remediation_workflow(self, issue_id, assigned_to, catalog="mdm_catalog", schema="quality"):
        """
        Assign data quality issue for remediation
        """
        issues_table = f"{catalog}.{schema}.dq_issues"

        delta_table = DeltaTable.forName(self.spark, issues_table)

        delta_table.update(
            condition=F.col("issue_id") == issue_id,
            set={
                "assigned_to": F.lit(assigned_to),
                "issue_status": F.lit("IN_PROGRESS"),
                "assigned_at": F.current_timestamp()
            }
        )

        print(f"Assigned issue {issue_id} to {assigned_to}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Real-Time Data Quality Monitoring

# COMMAND ----------

class RealTimeDQMonitor:
    """
    Real-time data quality monitoring for streaming data
    """

    def __init__(self, spark, catalog="mdm_catalog", schema="quality"):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema

    def monitor_stream(self, streaming_df, entity_type, quality_rules):
        """
        Apply quality checks to streaming data
        """
        # Apply quality rules
        monitored_df = streaming_df

        for rule in quality_rules:
            monitored_df = self._apply_quality_rule(monitored_df, rule)

        # Calculate quality score
        rule_cols = [f"rule_{i}_passed" for i in range(len(quality_rules))]

        monitored_df = monitored_df.withColumn(
            "dq_score",
            sum(F.when(F.col(c), 1).otherwise(0) for c in rule_cols) / len(quality_rules)
        )

        # Add quality status
        monitored_df = monitored_df.withColumn(
            "dq_status",
            F.when(F.col("dq_score") >= 0.9, "GOLD")
            .when(F.col("dq_score") >= 0.7, "SILVER")
            .otherwise("BRONZE")
        )

        return monitored_df

    def _apply_quality_rule(self, df, rule):
        """
        Apply a single quality rule
        """
        rule_type = rule['type']
        field = rule.get('field')

        if rule_type == 'not_null':
            df = df.withColumn(f"rule_{rule['id']}_passed", F.col(field).isNotNull())

        elif rule_type == 'regex':
            pattern = rule['pattern']
            df = df.withColumn(f"rule_{rule['id']}_passed",
                             F.col(field).rlike(pattern))

        elif rule_type == 'range':
            min_val = rule['min']
            max_val = rule['max']
            df = df.withColumn(f"rule_{rule['id']}_passed",
                             (F.col(field) >= min_val) & (F.col(field) <= max_val))

        elif rule_type == 'value_set':
            allowed_values = rule['values']
            df = df.withColumn(f"rule_{rule['id']}_passed",
                             F.col(field).isin(allowed_values))

        return df

    def write_quality_metrics(self, monitored_df, entity_type):
        """
        Write streaming quality metrics to Delta table
        """
        metrics_table = f"{self.catalog}.{self.schema}.dq_streaming_metrics"
        checkpoint_path = f"/dbfs/mdm/checkpoints/dq_streaming_{entity_type}"

        # Aggregate metrics by window
        windowed_metrics = (monitored_df
                           .withWatermark("ingestion_timestamp", "10 minutes")
                           .groupBy(
                               F.window("ingestion_timestamp", "5 minutes"),
                               "dq_status"
                           )
                           .agg(
                               F.count("*").alias("record_count"),
                               F.avg("dq_score").alias("avg_quality_score")
                           )
                           .withColumn("entity_type", F.lit(entity_type)))

        # Write stream
        query = (windowed_metrics.writeStream
                .format("delta")
                .outputMode("append")
                .option("checkpointLocation", checkpoint_path)
                .toTable(metrics_table))

        return query


# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Dashboard

# COMMAND ----------

class DataQualityDashboard:
    """
    Generate data quality dashboards and reports
    """

    def __init__(self, spark, catalog="mdm_catalog", schema="quality"):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema

    def get_overall_quality_score(self, entity_type=None):
        """
        Get overall data quality score
        """
        metrics_table = f"{self.catalog}.{self.schema}.dq_metrics"

        query = f"""
        SELECT
            entity_type,
            AVG(avg_quality_score) as overall_quality_score,
            SUM(total_records) as total_records,
            SUM(gold_records) / SUM(total_records) * 100 as gold_percentage,
            SUM(silver_records) / SUM(total_records) * 100 as silver_percentage,
            SUM(bronze_records) / SUM(total_records) * 100 as bronze_percentage
        FROM {metrics_table}
        WHERE metric_timestamp >= CURRENT_DATE - INTERVAL '7' DAY
        """

        if entity_type:
            query += f" AND entity_type = '{entity_type}'"

        query += " GROUP BY entity_type"

        return self.spark.sql(query)

    def get_quality_trends(self, entity_type, days=30):
        """
        Get quality score trends over time
        """
        metrics_table = f"{self.catalog}.{self.schema}.dq_metrics"

        query = f"""
        SELECT
            DATE(metric_timestamp) as date,
            AVG(avg_quality_score) as daily_quality_score,
            SUM(total_records) as daily_record_count
        FROM {metrics_table}
        WHERE entity_type = '{entity_type}'
          AND metric_timestamp >= CURRENT_DATE - INTERVAL '{days}' DAY
        GROUP BY DATE(metric_timestamp)
        ORDER BY date
        """

        return self.spark.sql(query)

    def get_top_quality_issues(self, limit=10):
        """
        Get most common quality issues
        """
        issues_table = f"{self.catalog}.{self.schema}.dq_issues"

        query = f"""
        SELECT
            failed_checks,
            COUNT(*) as issue_count,
            AVG(dq_score) as avg_score,
            issue_severity
        FROM {issues_table}
        WHERE issue_status = 'OPEN'
        GROUP BY failed_checks, issue_severity
        ORDER BY issue_count DESC
        LIMIT {limit}
        """

        return self.spark.sql(query)

    def get_remediation_metrics(self):
        """
        Get data quality remediation metrics
        """
        issues_table = f"{self.catalog}.{self.schema}.dq_issues"

        query = f"""
        SELECT
            issue_status,
            issue_severity,
            COUNT(*) as count,
            AVG(DATEDIFF(CURRENT_DATE, DATE(dq_check_timestamp))) as avg_age_days
        FROM {issues_table}
        GROUP BY issue_status, issue_severity
        ORDER BY issue_severity, issue_status
        """

        return self.spark.sql(query)

    def export_dashboard_data(self, output_path="/dbfs/mdm/dashboards"):
        """
        Export dashboard data for visualization tools (Tableau, PowerBI, etc.)
        """
        # Overall scores
        overall = self.get_overall_quality_score()
        overall.write.mode("overwrite").parquet(f"{output_path}/overall_quality")

        # Top issues
        issues = self.get_top_quality_issues(limit=50)
        issues.write.mode("overwrite").parquet(f"{output_path}/top_issues")

        # Remediation metrics
        remediation = self.get_remediation_metrics()
        remediation.write.mode("overwrite").parquet(f"{output_path}/remediation_metrics")

        print(f"Dashboard data exported to {output_path}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Automated Quality Alerts

# COMMAND ----------

class QualityAlertManager:
    """
    Manage automated alerts for data quality issues
    """

    def __init__(self, spark, alert_thresholds):
        self.spark = spark
        self.alert_thresholds = alert_thresholds

    def check_quality_thresholds(self, metrics_df):
        """
        Check if quality metrics breach thresholds
        """
        alerts = []

        for metric_row in metrics_df.collect():
            quality_score = metric_row['avg_quality_score']
            entity_type = metric_row.get('entity_type', 'unknown')

            threshold = self.alert_thresholds.get(entity_type, 0.8)

            if quality_score < threshold:
                alert = {
                    "alert_type": "QUALITY_BREACH",
                    "entity_type": entity_type,
                    "current_score": quality_score,
                    "threshold": threshold,
                    "severity": "CRITICAL" if quality_score < 0.5 else "HIGH",
                    "timestamp": datetime.now()
                }
                alerts.append(alert)

        return alerts

    def send_alert(self, alert):
        """
        Send alert notification
        (Integration with email, Slack, PagerDuty, etc.)
        """
        print(f"ALERT: {alert['alert_type']} - {alert['entity_type']}")
        print(f"Current Score: {alert['current_score']:.2f}, Threshold: {alert['threshold']:.2f}")

        # TODO: Integrate with notification systems
        # - Email via SendGrid/SMTP
        # - Slack webhook
        # - PagerDuty API
        # - Teams webhook