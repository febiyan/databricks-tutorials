"""
PII encryption helpers for Spark Declarative Pipelines.

This module implements the crypto-shredding pattern for GDPR compliance:
  1. A per-customer AES-256 key is derived and stored in silver_customer_keys.
  2. PII fields are encrypted with that key and stored in silver_customers.
  3. To honour a "right to erasure" request, delete the customer's row from
     silver_customer_keys. On the next pipeline refresh, the join returns NULL
     for that customer's key, making every encrypted PII column NULL —
     permanently unrecoverable without re-ingesting the source.

Usage
-----
    from pii_utils import derive_customer_key, encrypt_pii

    # Build the keys table
    df = df.withColumn("pii_key", derive_customer_key("customerID"))

    # Build the customers table
    df = df.withColumn("email_encrypted", encrypt_pii("email_address", "pii_key"))
"""

from pyspark.sql import functions as F
from pyspark.sql import Column

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
# In production, retrieve from Databricks Secrets:
#   dbutils.secrets.get(scope="pii", key="master_secret")
MASTER_SECRET = "databricks_tutorial_master_2025"


def derive_customer_key(customer_id_col: str) -> Column:
    """Derive a deterministic 256-bit AES key for a customer.

    Combines the customer ID with a master secret via SHA-256 to produce a
    stable, unique 64-character hex key per customer.

    Production note: use a truly random key per customer stored in a secret
    manager (e.g. Azure Key Vault) and only keep the key reference here —
    derivation from a master secret means rotating the master secret would
    invalidate all keys at once.
    """
    return F.sha2(
        F.concat(F.col(customer_id_col), F.lit(MASTER_SECRET)), 256
    ).alias("pii_key")


def encrypt_pii(col_name: str, key_col: str) -> Column:
    """AES-256-ECB encrypt a single PII column using a per-customer key.

    The key column must contain a 64-character hex string (output of SHA-256),
    which is decoded to 32 bytes before use.

    ECB mode is chosen here because it is deterministic (same plaintext +
    same key → same ciphertext), preserving the ability to COUNT DISTINCT
    encrypted values for analytics without exposing the raw data.

    Production note: for stronger confidentiality guarantees (e.g. when the
    same customer may have the same phone number as another), prefer GCM mode
    with a stored IV at the cost of non-deterministic output.
    """
    return F.base64(
        F.aes_encrypt(
            F.col(col_name),
            F.unhex(F.col(key_col)),
            F.lit("ECB"),
            F.lit("PKCS"),
        )
    ).alias(f"{col_name}_encrypted")
