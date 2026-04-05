"""
test_pii_utils.py — Unit tests for the PII hashing utilities.

These tests follow pytest conventions and can be run:
  • In the Databricks workspace (Testing sidebar → Run All Tests)
  • Locally with `pytest tests/` (requires pyspark installed)

The tests validate:
  1. Determinism    — same input always produces the same hash
  2. Salting        — plain SHA-256 differs from our salted hash
  3. Irreversibility check — output is a 64-char hex string (SHA-256)
  4. Full-name normalisation — case and whitespace are handled
  5. NULL handling  — NULL inputs produce NULL output (not an error)
"""

import hashlib
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F

# ── Spark session for local / workspace testing ──────────────────────────
# In the workspace the global `spark` is injected automatically;
# we create one only if it doesn't exist yet.
try:
    spark  # type: ignore[name-defined]
except NameError:
    spark = SparkSession.builder.master("local[*]").getOrCreate()

# ── Import the module under test ─────────────────────────────────────────
import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from ..src.pii_utils import hash_pii, hash_full_name, PII_SALT


# ═══════════════════════════════════════════════════════════════════════════
# hash_pii tests
# ═══════════════════════════════════════════════════════════════════════════

class TestHashPii:
    """Tests for the single-column hash_pii() function."""

    def test_deterministic(self):
        """Same value hashed twice must return the same result."""
        df = spark.createDataFrame([("alice@example.com",)], ["email_address"])
        result = df.select(hash_pii("email_address")).collect()
        hash_a = result[0][0]

        result2 = df.select(hash_pii("email_address")).collect()
        hash_b = result2[0][0]

        assert hash_a == hash_b, "hash_pii must be deterministic"

    def test_output_is_sha256_hex(self):
        """Output should be a 64-character lowercase hex string."""
        df = spark.createDataFrame([("test@test.com",)], ["email_address"])
        result = df.select(hash_pii("email_address")).collect()
        h = result[0][0]

        assert len(h) == 64, f"Expected 64 chars, got {len(h)}"
        assert all(c in "0123456789abcdef" for c in h), "Must be hex"

    def test_salted_differs_from_plain(self):
        """Our salted hash must NOT match an unsalted SHA-256."""
        value = "alice@example.com"
        plain_sha = hashlib.sha256(value.encode()).hexdigest()

        df = spark.createDataFrame([(value,)], ["email_address"])
        salted = df.select(hash_pii("email_address")).collect()[0][0]

        assert salted != plain_sha, "Salted hash must differ from plain SHA-256"

    def test_different_values_produce_different_hashes(self):
        """Two different emails must not collide (probabilistically)."""
        df = spark.createDataFrame(
            [("a@a.com",), ("b@b.com",)], ["email_address"]
        )
        rows = df.select(hash_pii("email_address")).collect()
        assert rows[0][0] != rows[1][0], "Different values should hash differently"

    def test_null_input_returns_null(self):
        """NULL input should produce NULL, not an exception."""
        df = spark.createDataFrame([(None,)], ["email_address"])
        result = df.select(hash_pii("email_address")).collect()
        assert result[0][0] is None, "NULL input should yield NULL output"


# ═══════════════════════════════════════════════════════════════════════════
# hash_full_name tests
# ═══════════════════════════════════════════════════════════════════════════

class TestHashFullName:
    """Tests for the two-column hash_full_name() function."""

    def test_case_insensitive(self):
        """'Alice Smith' and 'alice smith' must produce the same hash."""
        df1 = spark.createDataFrame([("Alice", "Smith")], ["first_name", "last_name"])
        df2 = spark.createDataFrame([("alice", "smith")], ["first_name", "last_name"])

        h1 = df1.select(hash_full_name("first_name", "last_name")).collect()[0][0]
        h2 = df2.select(hash_full_name("first_name", "last_name")).collect()[0][0]

        assert h1 == h2, "hash_full_name should be case-insensitive"

    def test_whitespace_trimmed(self):
        """Leading / trailing spaces must not affect the hash."""
        df1 = spark.createDataFrame([("  Alice  ", "  Smith  ")], ["first_name", "last_name"])
        df2 = spark.createDataFrame([("Alice", "Smith")], ["first_name", "last_name"])

        h1 = df1.select(hash_full_name("first_name", "last_name")).collect()[0][0]
        h2 = df2.select(hash_full_name("first_name", "last_name")).collect()[0][0]

        assert h1 == h2, "Whitespace should be trimmed before hashing"

    def test_different_names_differ(self):
        """Different names must produce different hashes."""
        df = spark.createDataFrame(
            [("Alice", "Smith"), ("Bob", "Jones")],
            ["first_name", "last_name"],
        )
        rows = df.select(hash_full_name("first_name", "last_name")).collect()
        assert rows[0][0] != rows[1][0]

    def test_output_alias(self):
        """The output column must be named 'full_name_hash'."""
        df = spark.createDataFrame([("A", "B")], ["first_name", "last_name"])
        result = df.select(hash_full_name("first_name", "last_name"))
        assert result.columns == ["full_name_hash"]
