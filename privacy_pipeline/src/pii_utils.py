"""
pii_utils.py — Reusable PII-hashing helpers for Spark Declarative Pipelines.

Uses SHA-256 to produce deterministic, irreversible hashes of PII fields.
A shared salt is mixed in so that raw values cannot be rainbow-tabled.

Usage
-----
    from pii_utils import hash_pii, hash_full_name

    df = df.withColumn("email_hash", hash_pii("email_address"))
"""

from pyspark.sql import functions as F
from pyspark.sql import Column

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
# In production, store this in Databricks Secrets and retrieve with
#   dbutils.secrets.get(scope="pii", key="hash_salt")
# For this tutorial we hard-code a placeholder salt.
PII_SALT = "databricks_tutorial_salt_2025"


def hash_pii(col_name: str) -> Column:
    """Return a SHA-256 hash of a single column value, salted."""
    return F.sha2(F.concat(F.col(col_name), F.lit(PII_SALT)), 256).alias(
        f"{col_name}_hash"
    )


def hash_full_name(first_col: str, last_col: str) -> Column:
    """Hash the concatenation of first + last name (lower-cased, trimmed)."""
    normalized = F.lower(
        F.concat(F.trim(F.col(first_col)), F.lit(" "), F.trim(F.col(last_col)))
    )
    return F.sha2(F.concat(normalized, F.lit(PII_SALT)), 256).alias("full_name_hash")
