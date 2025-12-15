# Databricks notebook source
# MAGIC %md
# MAGIC # Multi-Source MDM Connector
# MAGIC This notebook provides pre-built connectors for SAP, Salesforce, Oracle, and Odoo
# MAGIC Supports both batch and real-time ingestion

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession, functions as F
from delta.tables import DeltaTable
import requests
import json
import xmlrpc.client
from simple_salesforce import Salesforce
import oracledb
from datetime import datetime, timedelta


class MDMSourceConnector:
    """
    Universal connector for MDM source systems
    Supports: SAP, Salesforce, Oracle, Odoo
    """

    def __init__(self, config):
        self.spark = SparkSession.builder.getOrCreate()
        self.config = config
        self.catalog = config.get('catalog_name', 'mdm_catalog')
        self.schema = config.get('schema_name', 'bronze')

    def ingest_from_source(self, source_type, source_config, mode='batch'):
        """
        Unified ingestion framework supporting multiple sources

        Args:
            source_type: SAP, Salesforce, Oracle, Odoo
            source_config: Connection and table configuration
            mode: 'batch' or 'streaming'
        """
        if source_type.upper() == "SAP":
            return self._ingest_sap(source_config, mode)
        elif source_type.upper() == "SALESFORCE":
            return self._ingest_salesforce(source_config, mode)
        elif source_type.upper() == "ORACLE":
            return self._ingest_oracle(source_config, mode)
        elif source_type.upper() == "ODOO":
            return self._ingest_odoo(source_config, mode)
        else:
            raise ValueError(f"Unsupported source type: {source_type}")

    def _ingest_sap(self, config, mode='batch'):
        """
        SAP data extraction using JDBC
        Supports KNA1 (Customer), LFA1 (Vendor), MARA (Material) tables
        """
        if mode == 'batch':
            df = (self.spark.read
                  .format("jdbc")
                  .option("url", config['jdbc_url'])
                  .option("dbtable", config['table'])
                  .option("user", config['user'])
                  .option("password", config['password'])
                  .option("driver", "com.sap.db.jdbc.Driver")
                  .option("fetchsize", "10000")
                  .load())
        else:
            # Streaming mode requires external CDC tool (Debezium -> Kafka)
            # JDBC does not support readStream in Spark
            raise NotImplementedError(
                "SAP streaming requires CDC via Kafka/Debezium. "
                "Use batch mode with incremental_column for polling-based updates, "
                "or set up Debezium to capture SAP changes and stream via Kafka."
            )

        # Add metadata
        df = (df.withColumn("source_system", F.lit("SAP"))
                .withColumn("ingestion_timestamp", F.current_timestamp())
                .withColumn("source_record_id", F.col(config.get('primary_key', 'MANDT')))
                .withColumn("ingestion_mode", F.lit(mode)))

        return df

    def _ingest_salesforce(self, config, mode='batch'):
        """
        Salesforce data extraction using REST API
        Supports Account, Contact, Lead objects
        """
        if mode == 'batch':
            # Use Salesforce Bulk API for batch
            sf = Salesforce(
                username=config['username'],
                password=config['password'],
                security_token=config['security_token'],
                instance_url=config.get('instance_url')
            )

            # Query data - use safe field-based query building
            # Only allow soql_query if explicitly enabled, otherwise build from fields
            record_id_field = config.get('record_id_field', 'Id')

            if config.get('allow_custom_soql', False) and 'soql_query' in config:
                soql = config['soql_query']
                # Validate SOQL to prevent injection
                self._validate_soql(soql)
            else:
                # Build safe SOQL from fields list
                fields = config.get('fields', ['Id', 'Name'])
                # Ensure record ID field (usually 'Id') is always included
                if record_id_field not in fields:
                    fields = [record_id_field] + list(fields)

                # Validate field names (alphanumeric, underscore, period only)
                for field in fields:
                    if not all(c.isalnum() or c in ('_', '.') for c in field):
                        raise ValueError(f"Invalid field name: {field}")
                fields_str = ', '.join(fields)
                # Validate object name
                obj = config['object']
                if not obj.replace('_', '').isalnum():
                    raise ValueError(f"Invalid object name: {obj}")
                soql = f"SELECT {fields_str} FROM {obj}"
                # Add WHERE clause if provided
                if 'where_clause' in config:
                    where = config['where_clause']
                    # Basic validation - no SOQL injection patterns
                    self._validate_soql_fragment(where)
                    soql += f" WHERE {where}"

            data = sf.query_all(soql)
            records = data['records']

            # Handle empty results - need schema for createDataFrame
            if records:
                df = self.spark.createDataFrame(records)
            else:
                # No records returned - create empty DataFrame with schema if provided
                from pyspark.sql.types import StructType
                schema = config.get('schema')
                if schema is None:
                    raise ValueError(
                        f"Salesforce query returned no records for {config['object']}. "
                        "Provide 'schema' in config to handle empty results."
                    )
                df = self.spark.createDataFrame(
                    self.spark.sparkContext.emptyRDD(),
                    schema=schema
                )

            # Add metadata
            df = (df.withColumn("source_system", F.lit("Salesforce"))
                    .withColumn("ingestion_timestamp", F.current_timestamp())
                    .withColumn("source_record_id", F.col(record_id_field))
                    .withColumn("ingestion_mode", F.lit(mode)))

            return df

        else:
            # Streaming mode requires external connector
            # format("salesforce") is not a built-in Spark source
            raise NotImplementedError(
                "Salesforce streaming requires a custom connector or CDC tool. "
                "Use batch mode with polling-based incremental updates, "
                "or set up Salesforce Change Data Capture -> Kafka and stream via ingest_from_kafka()."
            )

    def _ingest_oracle(self, config, mode='batch'):
        """
        Oracle database extraction using JDBC
        """
        if mode == 'batch':
            reader = (self.spark.read
                      .format("jdbc")
                      .option("url", config['jdbc_url'])
                      .option("dbtable", config['table'])
                      .option("user", config['user'])
                      .option("password", config['password'])
                      .option("driver", "oracle.jdbc.driver.OracleDriver")
                      .option("fetchsize", "10000"))

            # Only enable JDBC partitioning if ALL required parameters are provided
            partition_column = config.get("partition_column")
            lower_bound = config.get("lower_bound")
            upper_bound = config.get("upper_bound")
            num_partitions = config.get("num_partitions")

            if all(v is not None for v in (partition_column, lower_bound, upper_bound, num_partitions)):
                reader = (reader
                          .option("partitionColumn", partition_column)
                          .option("lowerBound", str(lower_bound))
                          .option("upperBound", str(upper_bound))
                          .option("numPartitions", str(num_partitions)))
            elif any(v is not None for v in (partition_column, lower_bound, upper_bound, num_partitions)):
                raise ValueError(
                    "To enable Oracle JDBC partitioning, provide all of: "
                    "partition_column, lower_bound, upper_bound, and num_partitions in config."
                )

            df = reader.load()

            # Add metadata
            df = (df.withColumn("source_system", F.lit("Oracle"))
                    .withColumn("ingestion_timestamp", F.current_timestamp())
                    .withColumn("source_record_id", F.col(config.get('primary_key', 'ID')))
                    .withColumn("ingestion_mode", F.lit(mode)))

            return df

        else:
            # Streaming over JDBC is not supported in Spark/Databricks
            # Use CDC (e.g., Debezium -> Kafka) or an incremental batch pattern instead
            raise NotImplementedError(
                "Oracle streaming via JDBC is not supported. "
                "Use batch mode with an incremental_column or stream from a CDC source (e.g., Kafka)."
            )

    def _ingest_odoo(self, config, mode='batch'):
        """
        Odoo ERP extraction using XML-RPC API
        Supports res.partner, product.product models
        """
        if mode == 'batch':
            # Connect to Odoo
            common = xmlrpc.client.ServerProxy(f"{config['url']}/xmlrpc/2/common")
            uid = common.authenticate(
                config['database'],
                config['username'],
                config['password'],
                {}
            )

            # Check authentication success
            if not uid:
                raise ConnectionError(
                    f"Failed to authenticate with Odoo at {config['url']}. "
                    "Check database, username, and password."
                )

            models = xmlrpc.client.ServerProxy(f"{config['url']}/xmlrpc/2/object")

            # Search and read records
            model = config['model']
            domain = config.get('domain', [])
            fields = config.get('fields', [])

            record_ids = models.execute_kw(
                config['database'], uid, config['password'],
                model, 'search', [domain]
            )

            # Handle empty results
            if record_ids:
                records = models.execute_kw(
                    config['database'], uid, config['password'],
                    model, 'read', [record_ids], {'fields': fields}
                )
                df = self.spark.createDataFrame(records)
            else:
                # No matching records - create empty DataFrame with schema if provided
                from pyspark.sql.types import StructType
                schema = config.get('schema')
                if schema is None:
                    raise ValueError(
                        f"Odoo query returned no records for model {model}. "
                        "Provide 'schema' in config to handle empty results."
                    )
                df = self.spark.createDataFrame(
                    self.spark.sparkContext.emptyRDD(),
                    schema=schema
                )

            # Add metadata
            df = (df.withColumn("source_system", F.lit("Odoo"))
                    .withColumn("ingestion_timestamp", F.current_timestamp())
                    .withColumn("source_record_id", F.col("id"))
                    .withColumn("ingestion_mode", F.lit(mode)))

            return df

        else:
            # For streaming, poll API periodically (Odoo doesn't have native streaming)
            raise NotImplementedError("Odoo streaming mode requires custom implementation")

    def ingest_from_kafka(self, topic, kafka_config):
        """
        Real-time ingestion from Kafka topics
        Only sets security options when explicitly provided in config
        """
        reader = (self.spark.readStream
                  .format("kafka")
                  .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'])
                  .option("subscribe", topic)
                  .option("startingOffsets", kafka_config.get('starting_offsets', 'latest')))

        # Only set security options if provided (avoid breaking PLAINTEXT clusters)
        security_protocol = kafka_config.get('security_protocol')
        if security_protocol:
            reader = reader.option("kafka.security.protocol", security_protocol)

        sasl_mechanism = kafka_config.get('sasl_mechanism')
        if sasl_mechanism:
            reader = reader.option("kafka.sasl.mechanism", sasl_mechanism)

        sasl_jaas = kafka_config.get('sasl_jaas_config')
        if sasl_jaas:
            reader = reader.option("kafka.sasl.jaas.config", sasl_jaas)

        df = reader.load()

        # Parse JSON messages
        schema = kafka_config.get('value_schema')
        df = (df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")
                .select(F.from_json(F.col("value"), schema).alias("data"), "timestamp")
                .select("data.*", F.col("timestamp").alias("kafka_timestamp"))
                .withColumn("ingestion_timestamp", F.current_timestamp())
                .withColumn("ingestion_mode", F.lit("streaming")))

        return df

    def ingest_from_kinesis(self, stream_name, kinesis_config):
        """
        Real-time ingestion from AWS Kinesis
        Prefer using AWS credential provider chain or Databricks secrets for production
        """
        reader = (self.spark.readStream
                  .format("kinesis")
                  .option("streamName", stream_name)
                  .option("region", kinesis_config.get('region', 'us-east-1'))
                  .option("initialPosition", "latest"))

        # Only set AWS credentials if explicitly provided
        # Recommended: use instance profiles or Databricks secrets instead
        access_key_id = kinesis_config.get('access_key_id')
        secret_access_key = kinesis_config.get('secret_access_key')

        if access_key_id and secret_access_key:
            reader = (reader
                      .option("awsAccessKeyId", access_key_id)
                      .option("awsSecretKey", secret_access_key))

        df = reader.load()

        # Parse records
        schema = kinesis_config.get('data_schema')
        df = (df.selectExpr("CAST(data AS STRING)")
                .select(F.from_json(F.col("data"), schema).alias("record"))
                .select("record.*")
                .withColumn("ingestion_timestamp", F.current_timestamp())
                .withColumn("ingestion_mode", F.lit("streaming")))

        return df

    def write_to_bronze(self, df, entity_type, mode='batch'):
        """
        Write to Bronze layer with CDC support using Unity Catalog
        """
        table_name = f"{self.catalog}.{self.schema}.{entity_type}_bronze"

        if mode == 'batch':
            # Batch write with merge
            df.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .option("delta.enableChangeDataFeed", "true") \
                .saveAsTable(table_name)

        else:
            # Streaming write - requires checkpoint_path in config
            checkpoint_base = self.config.get('checkpoint_path')
            if not checkpoint_base:
                raise ValueError(
                    f"Streaming bronze writes for '{entity_type}' require 'checkpoint_path' "
                    "in the connector config."
                )
            checkpoint_path = f"{checkpoint_base}/{entity_type}_bronze"

            query = (df.writeStream
                       .format("delta")
                       .outputMode("append")
                       .option("checkpointLocation", checkpoint_path)
                       .option("mergeSchema", "true")
                       .option("delta.enableChangeDataFeed", "true")
                       .toTable(table_name))

            return query

    def create_custom_connector(self, connector_class):
        """
        Framework for custom connectors
        Custom connector must implement: connect(), extract(), transform()
        """
        connector = connector_class(self.spark, self.config)

        # Connect to source
        connector.connect()

        # Extract data
        raw_data = connector.extract()

        # Transform to standard schema
        df = connector.transform(raw_data)

        # Add standard metadata
        df = (df.withColumn("source_system", F.lit(connector.source_name))
                .withColumn("ingestion_timestamp", F.current_timestamp()))

        return df

    def _validate_soql(self, soql):
        """
        Validate SOQL query to prevent injection attacks
        Checks for dangerous patterns and syntax
        """
        soql_upper = soql.upper()

        # Check for dangerous keywords that shouldn't be in SOQL
        dangerous_patterns = [
            'DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'CREATE',
            'EXEC', 'EXECUTE', 'SCRIPT', '--', '/*', '*/',
            'UNION', 'INSERT', 'UPDATE'
        ]

        for pattern in dangerous_patterns:
            if pattern in soql_upper:
                raise ValueError(f"SOQL query contains forbidden keyword: {pattern}")

        # Must start with SELECT
        if not soql_upper.strip().startswith('SELECT'):
            raise ValueError("SOQL query must start with SELECT")

        # Validate it contains FROM
        if ' FROM ' not in soql_upper:
            raise ValueError("SOQL query must contain FROM clause")

    def _validate_soql_fragment(self, fragment):
        """
        Validate SOQL fragment (like WHERE clause) for injection
        """
        fragment_upper = fragment.upper()

        # Check for dangerous patterns
        dangerous_patterns = [
            'DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'CREATE',
            'EXEC', 'EXECUTE', 'SCRIPT', '--', '/*', '*/'
        ]

        for pattern in dangerous_patterns:
            if pattern in fragment_upper:
                raise ValueError(f"SOQL fragment contains forbidden keyword: {pattern}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Custom Connector Base Class

# COMMAND ----------

class CustomConnectorBase:
    """
    Base class for building custom source connectors
    """

    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
        self.source_name = "CustomSource"

    def connect(self):
        """Establish connection to source system"""
        raise NotImplementedError("Subclass must implement connect()")

    def extract(self):
        """Extract data from source"""
        raise NotImplementedError("Subclass must implement extract()")

    def transform(self, raw_data):
        """Transform to standardized schema"""
        raise NotImplementedError("Subclass must implement transform()")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Example: Custom REST API Connector

# COMMAND ----------

class RestAPIConnector(CustomConnectorBase):
    """
    Example custom connector for generic REST APIs
    """

    def __init__(self, spark, config):
        super().__init__(spark, config)
        self.source_name = config.get('source_name', 'RestAPI')
        self.api_url = config['api_url']
        self.headers = config.get('headers', {})
        self.timeout = config.get('timeout', (10, 30))  # (connect, read) timeout

    def connect(self):
        """Test API connection"""
        response = requests.get(f"{self.api_url}/health", headers=self.headers, timeout=self.timeout)
        if response.status_code != 200:
            raise ConnectionError(f"Failed to connect to {self.api_url}")

    def extract(self):
        """Extract data from REST API"""
        endpoint = self.config['endpoint']
        response = requests.get(f"{self.api_url}/{endpoint}", headers=self.headers, timeout=self.timeout)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"API call failed: {response.status_code}")

    def transform(self, raw_data):
        """Convert JSON to DataFrame"""
        if isinstance(raw_data, list):
            df = self.spark.createDataFrame(raw_data)
        else:
            df = self.spark.createDataFrame([raw_data])

        return df