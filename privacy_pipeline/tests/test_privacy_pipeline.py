"""
Integration-style unit tests for the pipeline logic.

These tests simulate what each pipeline table does by calling the same
transformations on small in-memory DataFrames. They don't require a running
pipeline — just a local or workspace Spark session.

Validates:
  1. silver_customer_keys — one key per customer, NULL customerID dropped
  2. silver_customers     — PII encrypted, non-PII preserved, NULL key shreds data
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, StringType

try:
    spark  # type: ignore[name-defined]
except NameError:
    spark = SparkSession.builder.master("local[*]").getOrCreate()

import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from pii_utils import derive_customer_key, encrypt_pii

# Test fixtures ─────────────────────────────────────────────────────────

SCHEMA = StructType([
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
])

SAMPLE_DATA = [
    (1, "Alice", "Smith", "alice@example.com", "555-0001", "123 Main St", "Seattle", "WA", "US", "North America", 98101, "Female"),
    (2, "Bob",   "Jones", "bob@example.com",   "555-0002", "456 Oak Ave", "Seattle", "WA", "US", "North America", 98102, "Male"),
    (3, "Carol", "Lee",   "carol@example.com", "555-0003", "789 Pine Rd", "Portland", "OR", "US", "North America", 97201, "Female"),
    (None, "Bad", "Record", "bad@example.com", "555-9999", "000 Null St", "Nowhere", "XX", "US", "North America", 0, "Other"),
]


def _build_source_df():
    return spark.createDataFrame(SAMPLE_DATA, schema=SCHEMA)


def _apply_keys_transform(source_df):
    """Reproduce the silver_customer_keys logic."""
    return (
        source_df
        .filter("customerID IS NOT NULL")
        .select(
            F.col("customerID"),
            derive_customer_key("customerID"),
        )
    )


def _apply_customers_transform(source_df, keys_df):
    """Reproduce the silver_customers logic."""
    return (
        source_df
        .filter("customerID IS NOT NULL")
        .join(keys_df, on="customerID", how="left")
        .select(
            F.col("customerID"),
            encrypt_pii("first_name", "pii_key"),
            encrypt_pii("last_name", "pii_key"),
            encrypt_pii("email_address", "pii_key"),
            encrypt_pii("phone_number", "pii_key"),
            encrypt_pii("address", "pii_key"),
            F.col("city"),
            F.col("state"),
            F.col("country"),
            F.col("continent"),
            F.col("postal_zip_code"),
            F.col("gender"),
        )
    )


# ═══════════════════════════════════════════════════════════════════════════
# silver_customer_keys tests
# ═══════════════════════════════════════════════════════════════════════════

class TestSilverCustomerKeys:

    def test_null_customer_id_dropped(self):
        """Rows with NULL customerID must be removed."""
        keys = _apply_keys_transform(_build_source_df())
        assert keys.count() == 3, "Expected 3 rows (1 NULL customerID dropped)"

    def test_one_key_per_customer(self):
        """Each customer must have exactly one key row."""
        keys = _apply_keys_transform(_build_source_df())
        assert keys.select("customerID").distinct().count() == keys.count()

    def test_output_columns(self):
        """Output must contain exactly customerID and pii_key."""
        keys = _apply_keys_transform(_build_source_df())
        assert set(keys.columns) == {"customerID", "pii_key"}

    def test_key_is_hex_string(self):
        """pii_key must be a 64-character hex string."""
        keys = _apply_keys_transform(_build_source_df())
        key = keys.filter("customerID = 1").collect()[0]["pii_key"]
        assert len(key) == 64
        assert all(c in "0123456789abcdef" for c in key)


# ═══════════════════════════════════════════════════════════════════════════
# silver_customers tests
# ═══════════════════════════════════════════════════════════════════════════

class TestSilverCustomers:

    def _build(self):
        source = _build_source_df()
        keys = _apply_keys_transform(source)
        return _apply_customers_transform(source, keys)

    def test_null_customer_id_dropped(self):
        """Rows with NULL customerID must be removed."""
        customers = self._build()
        assert customers.count() == 3

    def test_no_raw_pii_columns(self):
        """Raw PII column names must not appear in the output."""
        customers = self._build()
        raw_pii = {"first_name", "last_name", "email_address", "phone_number", "address"}
        present = raw_pii & set(customers.columns)
        assert len(present) == 0, f"Raw PII columns still present: {present}"

    def test_encrypted_columns_exist(self):
        """All five PII columns must have an encrypted counterpart."""
        customers = self._build()
        expected = {
            "first_name_encrypted", "last_name_encrypted",
            "email_address_encrypted", "phone_number_encrypted", "address_encrypted",
        }
        assert expected.issubset(set(customers.columns))

    def test_non_pii_columns_preserved(self):
        """Non-PII columns must pass through unchanged."""
        customers = self._build()
        kept = {"customerID", "city", "state", "country", "continent", "postal_zip_code", "gender"}
        assert kept.issubset(set(customers.columns))

    def test_encrypted_value_differs_from_original(self):
        """Encrypted email must not equal the original plaintext."""
        customers = self._build()
        row = customers.filter("customerID = 1").collect()[0]
        assert row["email_address_encrypted"] != "alice@example.com"

    def test_gdpr_erasure_nulls_pii(self):
        """Deleting a customer's key must make all their encrypted columns NULL."""
        source = _build_source_df()
        # Simulate key deletion: provide keys for everyone except customer 1
        keys = _apply_keys_transform(source).filter("customerID != 1")
        customers = _apply_customers_transform(source, keys)

        row = customers.filter("customerID = 1").collect()[0]
        assert row["first_name_encrypted"] is None
        assert row["email_address_encrypted"] is None
        assert row["address_encrypted"] is None

    def test_gdpr_erasure_preserves_non_pii(self):
        """After key deletion, non-PII columns for that customer must still be present."""
        source = _build_source_df()
        keys = _apply_keys_transform(source).filter("customerID != 1")
        customers = _apply_customers_transform(source, keys)

        row = customers.filter("customerID = 1").collect()[0]
        assert row["city"] == "Seattle"
        assert row["country"] == "US"

    def test_erasure_does_not_affect_other_customers(self):
        """Deleting one customer's key must not affect other customers' data."""
        source = _build_source_df()
        keys = _apply_keys_transform(source).filter("customerID != 1")
        customers = _apply_customers_transform(source, keys)

        row = customers.filter("customerID = 2").collect()[0]
        assert row["email_address_encrypted"] is not None
