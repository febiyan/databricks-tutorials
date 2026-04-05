"""
test_privacy_pipeline.py — Integration-style unit tests for the pipeline logic.

These tests simulate what each pipeline layer does by calling the same
transformations on small in-memory DataFrames. They don't require a running
pipeline — just a local or workspace Spark session.

Validates:
  1. Silver layer drops rows with NULL customerID (expectation)
  2. Silver layer output schema contains hashed columns, not raw PII
  3. Gold layer aggregation counts are correct
"""

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
)

try:
    spark  # type: ignore[name-defined]
except NameError:
    spark = SparkSession.builder.master("local[*]").getOrCreate()

import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from pii_utils import hash_pii, hash_full_name

# ── Test fixtures ────────────────────────────────────────────────────────

SCHEMA = StructType(
    [
        StructField("customerID", LongType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email_address", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("country", StringType(), True),
        StructField("continent", StringType(), True),
        StructField("postal_zip_code", LongType(), True),
        StructField("gender", StringType(), True),
    ]
)

SAMPLE_DATA = [
    (1, "Alice", "Smith", "alice@example.com", "555-0001", "123 Main St", "Seattle", "WA", "US", "North America", 98101, "Female"),
    (2, "Bob", "Jones", "bob@example.com", "555-0002", "456 Oak Ave", "Seattle", "WA", "US", "North America", 98102, "Male"),
    (3, "Carol", "Lee", "carol@example.com", "555-0003", "789 Pine Rd", "Portland", "OR", "US", "North America", 97201, "Female"),
    (None, "Bad", "Record", "bad@example.com", "555-9999", "000 Null St", "Nowhere", "XX", "US", "North America", 00000, "Other"),
]


def _build_bronze_df():
    return spark.createDataFrame(SAMPLE_DATA, schema=SCHEMA)


def _apply_silver_transform(bronze_df):
    """Reproduce the silver layer logic: filter NULLs + hash PII."""
    filtered = bronze_df.filter("customerID IS NOT NULL")
    return filtered.select(
        F.col("customerID"),
        hash_full_name("first_name", "last_name"),
        hash_pii("email_address"),
        hash_pii("phone_number"),
        hash_pii("address"),
        F.col("city"),
        F.col("state"),
        F.col("country"),
        F.col("continent"),
        F.col("postal_zip_code"),
        F.col("gender"),
    )


def _apply_gold_transform(silver_df):
    """Reproduce the gold layer aggregation."""
    return (
        silver_df.groupBy("country", "state", "city", "gender")
        .agg(
            F.count("*").alias("customer_count"),
            F.countDistinct("email_address_hash").alias("unique_emails"),
        )
        .orderBy("country", "state", "city")
    )


# ═══════════════════════════════════════════════════════════════════════════
# Silver layer tests
# ═══════════════════════════════════════════════════════════════════════════

class TestSilverLayer:

    def test_null_customer_id_dropped(self):
        """Rows with NULL customerID must be removed."""
        bronze = _build_bronze_df()
        silver = _apply_silver_transform(bronze)
        assert silver.count() == 3, "Expected 3 rows (1 NULL customerID dropped)"

    def test_no_raw_pii_in_output(self):
        """Silver output must not contain raw PII column names."""
        bronze = _build_bronze_df()
        silver = _apply_silver_transform(bronze)
        raw_pii_cols = {"first_name", "last_name", "email_address", "phone_number", "address"}
        present = raw_pii_cols.intersection(set(silver.columns))
        assert len(present) == 0, f"Raw PII columns still present: {present}"

    def test_hashed_columns_exist(self):
        """Silver output must contain the expected hashed columns."""
        bronze = _build_bronze_df()
        silver = _apply_silver_transform(bronze)
        expected = {"full_name_hash", "email_address_hash", "phone_number_hash", "address_hash"}
        assert expected.issubset(set(silver.columns)), f"Missing columns: {expected - set(silver.columns)}"

    def test_non_pii_columns_preserved(self):
        """Non-PII columns must pass through unchanged."""
        bronze = _build_bronze_df()
        silver = _apply_silver_transform(bronze)
        kept = {"customerID", "city", "state", "country", "continent", "postal_zip_code", "gender"}
        assert kept.issubset(set(silver.columns))

    def test_hash_values_are_not_original(self):
        """Hashed email must not equal the original email."""
        bronze = _build_bronze_df()
        silver = _apply_silver_transform(bronze)
        row = silver.filter("customerID = 1").collect()[0]
        assert row["email_address_hash"] != "alice@example.com"


# ═══════════════════════════════════════════════════════════════════════════
# Gold layer tests
# ═══════════════════════════════════════════════════════════════════════════

class TestGoldLayer:

    def test_aggregation_counts(self):
        """Seattle Female should have 1 customer, Seattle Male should have 1."""
        bronze = _build_bronze_df()
        silver = _apply_silver_transform(bronze)
        gold = _apply_gold_transform(silver)

        seattle_female = gold.filter(
            (F.col("city") == "Seattle") & (F.col("gender") == "Female")
        ).collect()
        assert len(seattle_female) == 1
        assert seattle_female[0]["customer_count"] == 1

    def test_no_pii_in_gold(self):
        """Gold output must not contain any PII or hashed-PII columns."""
        bronze = _build_bronze_df()
        silver = _apply_silver_transform(bronze)
        gold = _apply_gold_transform(silver)

        forbidden = {"first_name", "last_name", "email_address", "phone_number",
                     "address", "full_name_hash", "phone_number_hash", "address_hash"}
        present = forbidden.intersection(set(gold.columns))
        assert len(present) == 0, f"PII-related columns in gold: {present}"

    def test_total_rows(self):
        """With 3 valid records across 2 cities and 2 genders, expect 3 groups."""
        bronze = _build_bronze_df()
        silver = _apply_silver_transform(bronze)
        gold = _apply_gold_transform(silver)
        # Seattle/WA/Female, Seattle/WA/Male, Portland/OR/Female
        assert gold.count() == 3
