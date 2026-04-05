"""
Unit tests for the PII encryption utilities.

These tests follow pytest conventions and can be run:
  • In the Databricks workspace (Testing sidebar → Run All Tests)
  • Locally with `pytest tests/` (requires pyspark installed)

These tests validate:
  1. Key determinism:  same customerID always produces the same key
  2. Key uniqueness: different customerIDs produce different keys
  3. Key format: output is a 64-char hex string (256-bit)
  4. Encrypt non-null: encrypted output is a non-empty base64 string
  5. Encrypt determinism: same plaintext + same key → same ciphertext (ECB)
  6. NULL passthrough: NULL input produces NULL output (not an error)
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ── Spark session for local / workspace testing ──────────────────────────
try:
    spark  # type: ignore[name-defined]
except NameError:
    spark = SparkSession.builder.master("local[*]").getOrCreate()

# ── Import the module under test ─────────────────────────────────────────
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pii_utils import derive_customer_key, encrypt_pii, MASTER_SECRET

# ═══════════════════════════════════════════════════════════════════════════
# derive_customer_key tests
# ═══════════════════════════════════════════════════════════════════════════

class TestDeriveCustomerKey:
    """Tests for the per-customer key derivation function."""

    def test_deterministic(self):
        """Same customerID must always produce the same key."""
        df = spark.createDataFrame([("C001",)], ["customerID"])
        key_a = df.select(derive_customer_key("customerID")).collect()[0][0]
        key_b = df.select(derive_customer_key("customerID")).collect()[0][0]
        assert key_a == key_b, "Key derivation must be deterministic"

    def test_output_is_256bit_hex(self):
        """Key must be a 64-character lowercase hex string (256-bit)."""
        df = spark.createDataFrame([("C001",)], ["customerID"])
        key = df.select(derive_customer_key("customerID")).collect()[0][0]
        assert len(key) == 64, f"Expected 64 hex chars, got {len(key)}"
        assert all(c in "0123456789abcdef" for c in key), "Key must be hex"

    def test_unique_per_customer(self):
        """Different customerIDs must produce different keys."""
        df = spark.createDataFrame([("C001",), ("C002",)], ["customerID"])
        rows = df.select(derive_customer_key("customerID")).collect()
        assert rows[0][0] != rows[1][0], "Each customer must get a unique key"

    def test_output_alias(self):
        """Output column must be named 'pii_key'."""
        df = spark.createDataFrame([("C001",)], ["customerID"])
        result = df.select(derive_customer_key("customerID"))
        assert result.columns == ["pii_key"]


# ═══════════════════════════════════════════════════════════════════════════
# encrypt_pii tests
# ═══════════════════════════════════════════════════════════════════════════

class TestEncryptPii:
    """Tests for the per-customer AES-256 encryption function."""

    def _make_df(self, value, customer_id="C001"):
        """Helper: create a one-row dataframe with a derived key."""
        return (
            spark.createDataFrame([(customer_id, value)], ["customerID", "email_address"])
            .withColumn("pii_key", derive_customer_key("customerID"))
        )

    def test_output_is_base64_string(self):
        """Encrypted output must be a non-empty base64 string."""
        import base64
        df = self._make_df("alice@example.com")
        result = df.select(encrypt_pii("email_address", "pii_key")).collect()[0][0]
        assert result is not None and len(result) > 0
        # Must be valid base64
        base64.b64decode(result)  # raises if invalid

    def test_deterministic(self):
        """Same plaintext + same key must produce the same ciphertext (ECB)."""
        df = self._make_df("alice@example.com")
        c1 = df.select(encrypt_pii("email_address", "pii_key")).collect()[0][0]
        c2 = df.select(encrypt_pii("email_address", "pii_key")).collect()[0][0]
        assert c1 == c2, "Encryption must be deterministic for the same key"

    def test_different_customers_different_ciphertext(self):
        """Same plaintext encrypted with different keys must differ."""
        df1 = self._make_df("alice@example.com", customer_id="C001")
        df2 = self._make_df("alice@example.com", customer_id="C002")
        c1 = df1.select(encrypt_pii("email_address", "pii_key")).collect()[0][0]
        c2 = df2.select(encrypt_pii("email_address", "pii_key")).collect()[0][0]
        assert c1 != c2, "Different keys must produce different ciphertext"

    def test_null_key_returns_null(self):
        """NULL key (deleted customer) must yield NULL output, not an error."""
        df = (
            spark.createDataFrame([("alice@example.com",)], ["email_address"])
            .withColumn("pii_key", F.lit(None).cast("string"))
        )
        result = df.select(encrypt_pii("email_address", "pii_key")).collect()[0][0]
        assert result is None, "NULL key should yield NULL ciphertext"

    def test_null_value_returns_null(self):
        """NULL PII value must yield NULL output."""
        df = (
            spark.createDataFrame([("C001", None)], ["customerID", "email_address"])
            .withColumn("pii_key", derive_customer_key("customerID"))
        )
        result = df.select(encrypt_pii("email_address", "pii_key")).collect()[0][0]
        assert result is None, "NULL input should yield NULL ciphertext"

    def test_output_alias(self):
        """Output column must be named '<col>_encrypted'."""
        df = self._make_df("alice@example.com")
        result = df.select(encrypt_pii("email_address", "pii_key"))
        assert result.columns == ["email_address_encrypted"]
