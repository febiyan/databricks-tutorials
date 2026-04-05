"""
privacy_pipeline.py — Spark Declarative Pipeline for privacy-compliant
processing of the samples.bakehouse.sales_customers dataset.

Medallion architecture:
  bronze  →  raw ingestion  (materialized view, schema preserved)
  silver  →  PII hashed     (materialized view, PII replaced with SHA-256)
  gold    →  analytics-ready aggregation (materialized view, no PII at all)
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from pii_utils import hash_pii, hash_full_name

# ═══════════════════════════════════════════════════════════════════════════
# BRONZE — raw ingestion
# ═══════════════════════════════════════════════════════════════════════════

@dp.materialized_view(
    name="bronze_customers",
    comment="Raw customer data from samples.bakehouse.sales_customers",
)
def bronze_customers():
    """Ingest raw data with no transformations."""
    return spark.read.table("samples.bakehouse.sales_customers")


# ═══════════════════════════════════════════════════════════════════════════
# SILVER — PII hashed
# ═══════════════════════════════════════════════════════════════════════════

@dp.materialized_view(
    name="silver_customers_hashed",
    comment="Customer data with PII fields replaced by SHA-256 hashes",
)
@dp.expect_or_drop(
    "valid_customer_id", "customerID IS NOT NULL"
)
def silver_customers_hashed():
    """
    Replace PII columns with deterministic SHA-256 hashes.
    Non-PII columns (city, state, country, continent, gender) are kept as-is
    for analytics.
    """
    df = spark.read.table("bronze_customers")

    return df.select(
        # ── Identifiers ──────────────────────────────────────────────
        F.col("customerID"),
        # ── Hashed PII ───────────────────────────────────────────────
        hash_full_name("first_name", "last_name"),
        hash_pii("email_address"),
        hash_pii("phone_number"),
        hash_pii("address"),
        # ── Non-PII kept for analytics ───────────────────────────────
        F.col("city"),
        F.col("state"),
        F.col("country"),
        F.col("continent"),
        F.col("postal_zip_code"),
        F.col("gender"),
    )


# ═══════════════════════════════════════════════════════════════════════════
# GOLD — analytics-ready aggregation
# ═══════════════════════════════════════════════════════════════════════════

@dp.materialized_view(
    name="gold_customers_by_region",
    comment="Customer counts aggregated by region — fully anonymized",
)
def gold_customers_by_region():
    """Aggregate customer counts by geography and gender. No PII present."""
    df = spark.read.table("silver_customers_hashed")

    return (
        df.groupBy("country", "state", "city", "gender")
        .agg(
            F.count("*").alias("customer_count"),
            F.countDistinct("email_address_hash").alias("unique_emails"),
        )
        .orderBy("country", "state", "city")
    )
