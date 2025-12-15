# notebooks/01_ingestion/multi_source_connector.py

from databricks.sdk import WorkspaceClient
from pyspark.sql import functions as F
from delta.tables import DeltaTable


class MDMSourceConnector:
    """
    Universal connector for MDM source systems
    """

    def __init__(self, config):
        self.spark = SparkSession.builder.getOrCreate()
        self.config = config

    def ingest_from_source(self, source_type, source_config):
        """
        Unified ingestion framework supporting multiple sources
        """
        if source_type == "SAP":
            return self._ingest_sap(source_config)
        elif source_type == "Salesforce":
            return self._ingest_salesforce(source_config)
        elif source_type == "ODOO":
            return self._ingest_odoo(source_config)
        # Add more sources

    def _ingest_sap(self, config):
        """SAP data extraction using RFC or OData"""
        df = (self.spark.read
              .format("jdbc")
              .option("url", config['jdbc_url'])
              .option("dbtable", config['table'])
              .option("user", config['user'])
              .option("password", config['password'])
              .load())

        # Add metadata
        df = df.withColumn("source_system", F.lit("SAP"))
        df = df.withColumn("ingestion_timestamp", F.current_timestamp())
        df = df.withColumn("source_record_id", F.col(config['primary_key']))

        return df

    def write_to_bronze(self, df, entity_type):
        """
        Write to Bronze layer with CDC support
        """
        bronze_path = f"{self.config['bronze_path']}/{entity_type}"

        (df.write
         .format("delta")
         .mode("append")
         .option("mergeSchema", "true")
         .save(bronze_path))