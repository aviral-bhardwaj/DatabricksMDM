# notebooks/02_matching/entity_resolution.py

from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.sql import functions as F, Window
from pyspark.sql.types import *
import jellyfish  # For fuzzy matching


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