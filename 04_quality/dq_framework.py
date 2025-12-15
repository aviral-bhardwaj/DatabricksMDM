# notebooks/04_quality/dq_framework.py

from great_expectations.dataset import SparkDFDataset


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

        return df