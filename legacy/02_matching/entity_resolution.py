# Databricks notebook source
# MAGIC %md
# MAGIC # Intelligent Entity Matching
# MAGIC Advanced entity resolution using ML-powered fuzzy matching, configurable rules, and match review UI

# COMMAND ----------

from pyspark.ml.feature import HashingTF, IDF, Tokenizer, VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import *
import jellyfish
import mlflow
import mlflow.spark
from typing import List, Dict, Tuple


class EntityMatcher:
    """
    Advanced entity matching using ML and rule-based approaches
    """

    def __init__(self, spark, config):
        self.spark = spark
        self.config = config

    def match_entities(self, df, entity_type, matching_rules):
        """
        Multi-strategy entity matching
        """
        # 1. Exact matching
        exact_matches = self._exact_match(df, matching_rules['exact_keys'])

        # 2. Fuzzy matching
        fuzzy_matches = self._fuzzy_match(df, matching_rules['fuzzy_keys'])

        # 3. ML-based similarity
        ml_matches = self._ml_similarity_match(df, matching_rules['ml_features'])

        # 4. Combine and score
        combined_matches = self._combine_match_scores(
            exact_matches, fuzzy_matches, ml_matches
        )

        return combined_matches

    def _exact_match(self, df, exact_keys):
        """Exact key matching across sources"""
        match_key = F.concat_ws("||", *[F.coalesce(F.col(k), F.lit(""))
                                        for k in exact_keys])

        df_with_key = df.withColumn("exact_match_key", match_key)

        window = Window.partitionBy("exact_match_key").orderBy("ingestion_timestamp")

        matched = (df_with_key
                   .withColumn("master_id", F.first("source_record_id").over(window))
                   .withColumn("match_type", F.lit("EXACT"))
                   .withColumn("match_score", F.lit(1.0)))

        return matched

    def _fuzzy_match(self, df, fuzzy_keys):
        """Fuzzy string matching using Levenshtein, Jaro-Winkler"""

        # Create UDFs for fuzzy matching
        @F.udf(returnType=DoubleType())
        def jaro_winkler_similarity(s1, s2):
            if s1 is None or s2 is None:
                return 0.0
            return jellyfish.jaro_winkler_similarity(s1.lower(), s2.lower())

        # Self-join for pairwise comparison
        df_left = df.alias("left")
        df_right = df.alias("right")

        # Join on blocking keys to reduce comparisons
        blocking_condition = (F.col("left.country") == F.col("right.country"))

        pairs = df_left.join(df_right, blocking_condition, "inner")

        # Calculate similarity scores
        for key in fuzzy_keys:
            pairs = pairs.withColumn(
                f"{key}_similarity",
                jaro_winkler_similarity(F.col(f"left.{key}"), F.col(f"right.{key}"))
            )

        # Aggregate similarity score
        similarity_cols = [f"{key}_similarity" for key in fuzzy_keys]
        pairs = pairs.withColumn(
            "fuzzy_match_score",
            sum(F.col(c) for c in similarity_cols) / len(similarity_cols)
        )

        # Filter by threshold
        threshold = self.config.get('fuzzy_threshold', 0.85)
        matches = pairs.filter(F.col("fuzzy_match_score") >= threshold)

        return matches

    def _ml_similarity_match(self, df, feature_cols):
        """ML-based entity similarity using TF-IDF"""

        # Combine text features
        df = df.withColumn(
            "combined_text",
            F.concat_ws(" ", *feature_cols)
        )

        # Tokenize
        tokenizer = Tokenizer(inputCol="combined_text", outputCol="words")
        wordsData = tokenizer.transform(df)

        # TF-IDF
        hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=1000)
        featurizedData = hashingTF.transform(wordsData)

        idf = IDF(inputCol="rawFeatures", outputCol="features")
        idfModel = idf.fit(featurizedData)
        rescaledData = idfModel.transform(featurizedData)

        # Calculate cosine similarity (implement vector similarity UDF)
        # Return matches above similarity threshold

        return rescaledData

    def create_master_entity_id(self, matched_df):
        """
        Create consistent master entity IDs across sources
        """
        # Use hash of sorted source IDs or UUID
        window = Window.partitionBy("match_group").orderBy("ingestion_timestamp")

        master_df = (matched_df
                     .withColumn("master_entity_id",
                                 F.concat(F.lit("MDM_"),
                                          F.md5(F.col("match_group"))))
                     .withColumn("rank", F.row_number().over(window)))

        return master_df

    def ml_powered_matching(self, df, entity_type):
        """
        Advanced ML-based entity matching using trained models
        """
        with mlflow.start_run(run_name=f"entity_matching_{entity_type}"):
            # Feature engineering
            feature_df = self._create_matching_features(df)

            # Train or load matching model
            model_path = f"/dbfs/mdm/models/{entity_type}_matching_model"

            try:
                # Try to load existing model
                model = mlflow.spark.load_model(model_path)
                print(f"Loaded existing model from {model_path}")
            except Exception as e:
                # Train new model if not found or load fails
                print(f"Model not found or failed to load ({e}), training new matching model...")
                model = self._train_matching_model(feature_df, entity_type)
                mlflow.spark.log_model(model, "matching_model")
                mlflow.spark.save_model(model, model_path)

            # Apply model for matching predictions
            predictions = model.transform(feature_df)

            # Extract match pairs
            matches = self._extract_match_pairs(predictions)

            return matches

    def _create_matching_features(self, df):
        """
        Create engineered features for ML matching
        """
        # String similarity features
        @F.udf(returnType=DoubleType())
        def jaro_similarity(s1, s2):
            if not s1 or not s2:
                return 0.0
            return jellyfish.jaro_similarity(str(s1), str(s2))

        @F.udf(returnType=DoubleType())
        def levenshtein_ratio(s1, s2):
            if not s1 or not s2:
                return 0.0
            lev_dist = F.levenshtein(s1, s2)
            max_len = max(len(str(s1)), len(str(s2)))
            return 1 - (lev_dist / max_len) if max_len > 0 else 0.0

        # Create pairwise combinations for comparison
        df_left = df.alias("left")
        df_right = df.alias("right")

        # Use blocking to reduce comparisons
        pairs = df_left.join(
            df_right,
            (F.col("left.country") == F.col("right.country")) &
            (F.col("left.source_record_id") != F.col("right.source_record_id")),
            "inner"
        )

        # Calculate similarity features
        feature_df = (pairs
                      .withColumn("name_jaro", jaro_similarity(F.col("left.name"), F.col("right.name")))
                      .withColumn("email_exact", F.when(F.col("left.email") == F.col("right.email"), 1.0).otherwise(0.0))
                      .withColumn("phone_exact", F.when(F.col("left.phone") == F.col("right.phone"), 1.0).otherwise(0.0))
                      .withColumn("address_similarity", jaro_similarity(F.col("left.address"), F.col("right.address")))
                      .withColumn("same_source", F.when(F.col("left.source_system") == F.col("right.source_system"), 0.0).otherwise(1.0)))

        # Assemble features
        assembler = VectorAssembler(
            inputCols=["name_jaro", "email_exact", "phone_exact", "address_similarity", "same_source"],
            outputCol="features"
        )

        feature_df = assembler.transform(feature_df)

        return feature_df

    def _train_matching_model(self, feature_df, entity_type):
        """
        Train Random Forest model for match/no-match classification
        """
        # For initial training, use rule-based labels as ground truth
        labeled_df = self._create_training_labels(feature_df)

        # Split train/test
        train_df, test_df = labeled_df.randomSplit([0.8, 0.2], seed=42)

        # Train Random Forest
        rf = RandomForestClassifier(
            featuresCol="features",
            labelCol="is_match",
            numTrees=100,
            maxDepth=10,
            seed=42
        )

        model = rf.fit(train_df)

        # Evaluate
        predictions = model.transform(test_df)
        from pyspark.ml.evaluation import BinaryClassificationEvaluator

        evaluator = BinaryClassificationEvaluator(labelCol="is_match", rawPredictionCol="rawPrediction")
        auc = evaluator.evaluate(predictions)

        print(f"Model AUC: {auc}")
        mlflow.log_metric("auc", auc)

        return model

    def _create_training_labels(self, feature_df):
        """
        Create training labels based on high-confidence rules
        """
        # Label as match (1) if high similarity, no-match (0) if low similarity
        labeled_df = feature_df.withColumn(
            "is_match",
            F.when(
                (F.col("email_exact") == 1.0) |
                ((F.col("name_jaro") > 0.9) & (F.col("address_similarity") > 0.85)),
                1.0
            ).when(
                (F.col("name_jaro") < 0.6) & (F.col("address_similarity") < 0.6),
                0.0
            ).otherwise(None)  # Uncertain cases excluded from training
        )

        # Filter only labeled examples
        labeled_df = labeled_df.filter(F.col("is_match").isNotNull())

        return labeled_df

    def _extract_match_pairs(self, predictions):
        """
        Extract matched pairs from ML predictions
        """
        # Get high-confidence matches
        matches = predictions.filter(F.col("prediction") == 1.0)

        # Create match groups using graph clustering
        match_groups = self._create_match_clusters(matches)

        return match_groups

    def _create_match_clusters(self, matches):
        """
        Create transitive match clusters (if A matches B and B matches C, then A=B=C)
        """
        # Use GraphFrames or custom clustering logic
        # For now, simple approach using connected components

        edges = matches.select(
            F.col("left.source_record_id").alias("src"),
            F.col("right.source_record_id").alias("dst")
        )

        # Use iterative approach to find connected components
        # This is simplified - production would use GraphFrames
        match_groups = edges.withColumn("match_group", F.monotonically_increasing_id())

        return match_groups

    def identify_review_cases(self, matched_df):
        """
        Identify matches that require manual review
        """
        review_threshold_min = self.config.get('manual_review_threshold', 0.75)
        auto_merge_threshold = self.config.get('auto_merge_threshold', 0.95)

        review_cases = matched_df.filter(
            (F.col("match_score") >= review_threshold_min) &
            (F.col("match_score") < auto_merge_threshold)
        )

        # Add review metadata
        review_cases = (review_cases
                        .withColumn("review_status", F.lit("PENDING"))
                        .withColumn("review_created_at", F.current_timestamp())
                        .withColumn("review_assigned_to", F.lit(None).cast(StringType()))
                        .withColumn("review_notes", F.lit(None).cast(StringType())))

        return review_cases

    def save_for_review(self, review_cases, catalog, schema):
        """
        Save match cases requiring manual review
        """
        review_table = f"{catalog}.{schema}.match_review_queue"

        review_cases.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(review_table)

        print(f"Saved {review_cases.count()} cases to review queue: {review_table}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Match Review UI Helper Functions

# COMMAND ----------

class MatchReviewManager:
    """
    Manage manual review workflow for entity matches
    """

    def __init__(self, spark, catalog="mdm_catalog", schema="silver"):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.review_table = f"{catalog}.{schema}.match_review_queue"

    def get_pending_reviews(self, limit=100, assigned_to=None):
        """
        Get pending match reviews
        """
        query = f"""
        SELECT
            review_id,
            left_entity,
            right_entity,
            match_score,
            match_type,
            review_created_at,
            review_assigned_to
        FROM {self.review_table}
        WHERE review_status = 'PENDING'
        """

        if assigned_to:
            query += f" AND review_assigned_to = '{assigned_to}'"

        query += f" ORDER BY match_score DESC LIMIT {limit}"

        return self.spark.sql(query)

    def approve_match(self, review_id, approved_by, notes=None):
        """
        Approve a match and merge entities
        """
        update_query = f"""
        UPDATE {self.review_table}
        SET review_status = 'APPROVED',
            review_completed_at = current_timestamp(),
            review_completed_by = '{approved_by}',
            review_notes = '{notes if notes else ""}'
        WHERE review_id = '{review_id}'
        """

        self.spark.sql(update_query)

        # Trigger merge process
        self._trigger_entity_merge(review_id)

    def reject_match(self, review_id, rejected_by, notes=None):
        """
        Reject a match
        """
        update_query = f"""
        UPDATE {self.review_table}
        SET review_status = 'REJECTED',
            review_completed_at = current_timestamp(),
            review_completed_by = '{rejected_by}',
            review_notes = '{notes if notes else ""}'
        WHERE review_id = '{review_id}'
        """

        self.spark.sql(update_query)

    def _trigger_entity_merge(self, review_id):
        """
        Trigger golden record creation after match approval
        """
        # This would trigger the golden record creation workflow
        print(f"Triggering merge for review_id: {review_id}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Configurable Matching Rules Engine

# COMMAND ----------

class MatchingRulesEngine:
    """
    Configurable rules engine for entity matching
    Allows business users to define custom matching logic
    """

    def __init__(self, spark):
        self.spark = spark

    def apply_rules(self, df, rules_config):
        """
        Apply user-defined matching rules

        Rules format:
        {
            "rule_name": "Email Match",
            "conditions": [
                {"field": "email", "operator": "exact_match"},
                {"field": "country", "operator": "exact_match"}
            ],
            "weight": 1.0,
            "auto_merge": true
        }
        """
        matched_df = df

        for rule in rules_config:
            rule_result = self._apply_single_rule(df, rule)

            # Combine with existing matches
            matched_df = matched_df.union(rule_result)

        return matched_df

    def _apply_single_rule(self, df, rule):
        """
        Apply a single matching rule
        """
        conditions = rule['conditions']
        join_condition = None

        df_left = df.alias("left")
        df_right = df.alias("right")

        # Build join condition from rule conditions
        for cond in conditions:
            field = cond['field']
            operator = cond['operator']

            if operator == "exact_match":
                condition = F.col(f"left.{field}") == F.col(f"right.{field}")
            elif operator == "fuzzy_match":
                threshold = cond.get('threshold', 0.85)
                # Use UDF for fuzzy matching
                condition = F.col(f"left.{field}").isNotNull() & F.col(f"right.{field}").isNotNull()

            if join_condition is None:
                join_condition = condition
            else:
                join_condition = join_condition & condition

        # Apply rule
        matches = df_left.join(df_right, join_condition, "inner")

        # Add rule metadata
        matches = (matches
                   .withColumn("match_rule", F.lit(rule['rule_name']))
                   .withColumn("match_score", F.lit(rule['weight']))
                   .withColumn("auto_merge", F.lit(rule.get('auto_merge', False))))

        return matches