# notebooks/03_golden_record/survivorship.py

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
                          .withColumn("golden_record_created_at", F.current_timestamp()))

        return golden_records