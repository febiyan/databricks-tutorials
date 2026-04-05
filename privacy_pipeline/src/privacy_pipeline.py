"""
privacy_pipeline.py — Spark Declarative Pipeline implementing GDPR-compliant
crypto-shredding for the samples.bakehouse.sales_customers dataset.

Two silver tables
-----------------
silver_customer_keys
    One row per customer: customerID + a 256-bit AES key derived from the
    master secret. This is the only place where the key lives.

    GDPR "right to erasure":
        DELETE FROM silver_customer_keys WHERE customerID = '<id>';
    Then refresh the pipeline. The corresponding customer's encrypted PII
    in silver_customers will become NULL — permanently unrecoverable.

silver_customers
    Customer data with all PII fields AES-256 encrypted using the customer's
    key. Non-PII columns (geography, gender) are kept in plaintext for
    analytics. When a customer's key is absent (deleted), their PII columns
    are NULL.

Production note
---------------
silver_customer_keys is defined here as a materialized view, which means a
full pipeline refresh re-derives all keys. In production, make it a streaming
table (or manage it as an external Delta table) so that manual key deletions
persist across pipeline runs.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from pii_utils import derive_customer_key, encrypt_pii


# ═══════════════════════════════════════════════════════════════════════════
# SILVER — per-customer AES keys
# ═══════════════════════════════════════════════════════════════════════════

@dp.materialized_view(
    name="silver_customer_keys",
    comment=(
        "Per-customer AES-256 keys for PII encryption. "
        "Delete a customer's row here to satisfy a GDPR right-to-erasure request."
    ),
)
@dp.expect_or_drop("valid_customer_id", "customerID IS NOT NULL")
def silver_customer_keys():
    """Derive one stable AES-256 key per customer from their ID."""
    return (
        spark.read.table("samples.bakehouse.sales_customers")
        .select(
            F.col("customerID"),
            derive_customer_key("customerID"),
        )
    )


# ═══════════════════════════════════════════════════════════════════════════
# SILVER — customer data with encrypted PII
# ═══════════════════════════════════════════════════════════════════════════

@dp.materialized_view(
    name="silver_customers",
    comment=(
        "Customer data with PII fields AES-256 encrypted using per-customer keys "
        "from silver_customer_keys. Non-PII columns are kept in plaintext."
    ),
)
@dp.expect_or_drop("valid_customer_id", "customerID IS NOT NULL")
def silver_customers():
    """
    Join customers with their key and encrypt every PII field.
    If a customer's key has been deleted (GDPR erasure), the LEFT JOIN
    returns NULL for pii_key, and all encrypted columns will be NULL.
    """
    customers = spark.read.table("samples.bakehouse.sales_customers")
    keys = spark.read.table("silver_customer_keys")

    return (
        customers.join(keys, on="customerID", how="left")
        .select(
            # ── Identity ──────────────────────────────────────────────────
            F.col("customerID"),
            # ── Encrypted PII ─────────────────────────────────────────────
            encrypt_pii("first_name", "pii_key"),
            encrypt_pii("last_name", "pii_key"),
            encrypt_pii("email_address", "pii_key"),
            encrypt_pii("phone_number", "pii_key"),
            encrypt_pii("address", "pii_key"),
            # ── Non-PII kept for analytics ────────────────────────────────
            F.col("city"),
            F.col("state"),
            F.col("country"),
            F.col("continent"),
            F.col("postal_zip_code"),
            F.col("gender"),
        )
    )
