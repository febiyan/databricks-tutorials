# Let's Build a GDPR-Compliant Data Pipeline on Databricks

You've got customer data. It has emails, phone numbers, addresses — the works. You need to make it usable for analytics. And when a customer asks to be forgotten, you need to actually forget them.

Hashing helps. But it's not enough. A SHA-256 hash of someone's email is irreversible — but it's still their data, sitting in your table, forever. Under GDPR's right to erasure, that's a problem.

This tutorial builds something better: a **crypto-shredding** pipeline. Each customer's PII gets encrypted with their own personal key. Delete the key, and the data becomes permanently unrecoverable. No key, no data. Simple.

No fluff. Let's go.

---


---

## What we're working with

Our source table is `samples.bakehouse.sales_customers`. It's a sample dataset that ships with every Databricks workspace — a fake bakery franchise customer list.

Here's what the columns look like:

| Column | Type | PII? |
|--------|------|------|
| `customerID` | bigint | No |
| `first_name` | string | Yes |
| `last_name` | string | Yes |
| `email_address` | string | Yes |
| `phone_number` | string | Yes |
| `address` | string | Yes |
| `city` | string | No |
| `state` | string | No |
| `country` | string | No |
| `continent` | string | No |
| `postal_zip_code` | bigint | No |
| `gender` | string | No |

Five PII columns. We're going to encrypt all of them.

---

## The game plan

Two silver tables. That's the whole architecture.


**`silver_customer_keys`** holds one row per customer: their `customerID` and a 256-bit AES key. This is the only place their key lives.

**`silver_customers`** holds the actual customer data. Every PII field is AES-encrypted using that customer's key from the keys table. Geography, gender — anything that's not personally identifiable stays in plaintext for analytics.

The GDPR workflow, in full:

```sql
-- Customer C123 wants to be forgotten
DELETE FROM silver_customer_keys WHERE customerID = 123;

-- Refresh the pipeline
-- On the next run, the LEFT JOIN finds no key for C123
-- → every encrypted column for C123 is NULL
-- → permanently unrecoverable
```

That's crypto-shredding. The customer's row still exists in `silver_customers` — but it's just noise. You've deleted their key, not their data, and the effect is the same.

---

## The tools

This tutorial uses three things that are relatively new to Databricks:

**Spark Declarative Pipelines** — the `pyspark.pipelines` module. You decorate a Python function, return a DataFrame, and the pipeline engine figures out the execution order. It's part of Apache Spark 4.1 and Databricks extends it.

**Workspace unit testing** — Databricks now discovers `test_*.py` files automatically and gives you a testing sidebar right in the workspace editor. Click play. See results. That's it.

**Declarative Automation Bundles (DABs)** — YAML files that define your pipeline, your job schedule, and your deployment targets. One `databricks bundle deploy` and you're live.

---

## Project structure

Here's what we're building:

```
privacy_pipeline/
├── databricks.yml                # Bundle config — targets, variables
├── resources/
│   └── privacy_pipeline.yml      # Pipeline + job definitions
├── src/
│   ├── pii_utils.py              # Encryption helpers (the reusable bits)
│   └── privacy_pipeline.py       # Two silver tables
└── tests/
    ├── test_pii_utils.py         # Tests for the encryption functions
    ├── test_privacy_pipeline.py  # Integration-style tests for pipeline logic
    └── run_tests_notebook.py     # Notebook wrapper for manual test runs
```

Seven files. That's the whole thing.

---

## Step 1 — The encryption helpers

Open `src/pii_utils.py`. Two functions, that's all.

### Key derivation

`derive_customer_key("customerID")` generates a stable, unique 256-bit AES key for each customer by hashing their ID against a master secret:

```python
MASTER_SECRET = "..."  # from Databricks Secrets in production

def derive_customer_key(customer_id_col: str) -> Column:
    return F.sha2(
        F.concat(F.col(customer_id_col), F.lit(MASTER_SECRET)), 256
    ).alias("pii_key")
```

Same customer ID + same master secret always produces the same key. That matters because the pipeline might run many times — you don't want a different key generated each run, which would make previously encrypted data unreadable.

In production, pull the master secret from Databricks Secrets:

```python
MASTER_SECRET = dbutils.secrets.get(scope="pii", key="master_secret")
```

### Encryption

`encrypt_pii("email_address", "pii_key")` takes a column name and the key column, and returns an AES-256 encrypted version of that column as a base64 string:

```python
def encrypt_pii(col_name: str, key_col: str) -> Column:
    return F.base64(
        F.aes_encrypt(
            F.col(col_name),
            F.unhex(F.col(key_col)),
            F.lit("ECB"),
            F.lit("PKCS"),
        )
    ).alias(f"{col_name}_encrypted")
```

A few things to notice.

We use `F.unhex()` to convert the 64-char hex key string into the 32-byte binary that `aes_encrypt` expects.

We use ECB mode, which is deterministic — same plaintext plus same key always gives the same ciphertext. That lets you do `COUNT DISTINCT email_address_encrypted` for analytics without ever decrypting anything.

The output column is automatically named `email_address_encrypted`, `phone_number_encrypted`, etc.

If `pii_key` is NULL — because you deleted that customer's key — `aes_encrypt` returns NULL. That's the shredding in action.

---

## Step 2 — The pipeline

Open `src/privacy_pipeline.py`. Two decorated functions. That's the whole pipeline.

### silver_customer_keys

```python
@dp.materialized_view(name="silver_customer_keys")
@dp.expect_or_drop("valid_customer_id", "customerID IS NOT NULL")
def silver_customer_keys():
    return (
        spark.read.table("samples.bakehouse.sales_customers")
        .select(
            F.col("customerID"),
            derive_customer_key("customerID"),
        )
    )
```

One row per customer. Two columns: their ID and their key.

The `@dp.expect_or_drop` decorator is a data quality gate. Any row with a NULL `customerID` gets quietly dropped before it reaches this table. It still shows up in the pipeline event log so you know it happened.

This table is the sensitive one. Access to it should be tightly controlled — it's the master key store. Anyone who can read this table can decrypt any customer's PII.


### silver_customers

```python
@dp.materialized_view(name="silver_customers")
@dp.expect_or_drop("valid_customer_id", "customerID IS NOT NULL")
def silver_customers():
    customers = spark.read.table("samples.bakehouse.sales_customers")
    keys = spark.read.table("silver_customer_keys")

    return (
        customers.join(keys, on="customerID", how="left")
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
```

A few things to notice.

We LEFT JOIN to the keys table. Not INNER JOIN. That's intentional — if a customer's key has been deleted, the join returns NULL for `pii_key`, and `encrypt_pii` returns NULL for every encrypted column. The customer's row still exists, but their PII is gone.

Raw PII columns — `first_name`, `last_name`, `email_address`, `phone_number`, `address` — never appear in the output. The `select()` replaces them with their `_encrypted` equivalents.

Geography and gender stay in plaintext. They're useful for analytics and they don't identify an individual on their own.

---

## Step 3 — The tests

This is the part people usually skip. Don't.

We test the encryption functions in isolation, then we test the pipeline logic against small in-memory DataFrames.

### What we test

**`test_pii_utils.py`** — the encryption helpers:

- Same customer ID → same key every time? (determinism)
- Different customer IDs → different keys? (uniqueness)
- Is the key a 64-char hex string? (format check)
- Encrypted output is valid base64? (format check)
- Same value + same key → same ciphertext? (ECB determinism, useful for analytics)
- Same value + different keys → different ciphertext? (isolation between customers)
- NULL key → NULL output, no crash? (the shredding case)
- NULL value → NULL output, no crash? (NULL safety)
- Output column named correctly? (alias check)

**`test_privacy_pipeline.py`** — the pipeline layers with a small test DataFrame:

- Does the keys table produce one key per customer?
- Does the customers table drop rows where customerID is NULL?
- Are raw PII column names gone from the output? (`first_name`, `email_address`, etc.)
- Are the `_encrypted` columns present?
- Do non-PII columns like `city` and `state` pass through untouched?
- Is the encrypted value actually different from the original?
- Does deleting a key make the corresponding PII NULL?

That last test is the important one. It verifies the actual GDPR behaviour, not just the plumbing.


### How to run them

**In the workspace:** Open any test file. The Tests sidebar appears automatically on the left. Hit the play-all button. That's it.

**Locally:**

```bash
cd privacy_pipeline
pip install pyspark pytest
pytest tests/ -v
```

---

## Step 4 — The bundle config

Two YAML files wire everything together.

### `databricks.yml`

This is the root. It gives the bundle a name, defines variables you can override per target, and lists three deployment targets:

- **dev** — your personal workspace. This is the default.
- **staging** — for pre-production validation.
- **prod** — runs under a service principal, not your personal identity.

The `catalog` and `schema` variables mean you don't hardcode table locations. Dev writes to `main.privacy_tutorial`, prod writes to `main.privacy_tutorial_prod`. Same code, different targets.

### `resources/privacy_pipeline.yml`

This defines two things:

A **pipeline** — points at the two source files, sets the target catalog and schema from variables, and enables Photon for faster execution.

A **job** that runs on a daily schedule and triggers a full pipeline refresh.


---

## Step 5 — Deploy it

### First time setup

Install the Databricks CLI and authenticate:

```bash
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
databricks auth login --host https://<your-workspace-url>
```

Then open `databricks.yml` and put your actual workspace URL in place of the placeholder.

### Ship it

```bash
cd privacy_pipeline

# Does everything look right?
databricks bundle validate

# Push to your dev workspace
databricks bundle deploy

# Run the pipeline
databricks bundle run customers_hashed_job
```

For production:

```bash
databricks bundle deploy -t prod
databricks bundle run -t prod customers_hashed_job
```

Three commands. You're live.

---

## Step 6 — Check your work

Once the pipeline finishes, open a SQL editor in your workspace:

```sql
-- Keys table: one row per customer, key is a hex string
SELECT * FROM main.privacy_tutorial.silver_customer_keys LIMIT 5;

-- Customers table: PII encrypted, geography in plaintext
SELECT customerID, first_name_encrypted, email_address_encrypted, city, country
FROM main.privacy_tutorial.silver_customers
LIMIT 5;

-- Confirm no raw PII in the schema
DESCRIBE main.privacy_tutorial.silver_customers;
```

If you see base64 strings where names used to be, you're good.

Now test the GDPR flow. Pick a customer:

```sql
-- Before: their data is encrypted but present
SELECT customerID, first_name_encrypted, email_address_encrypted
FROM main.privacy_tutorial.silver_customers
WHERE customerID = 1;

-- Forget them
DELETE FROM main.privacy_tutorial.silver_customer_keys WHERE customerID = 1;

-- Refresh the pipeline, then check again
-- After: all encrypted columns are NULL
SELECT customerID, first_name_encrypted, email_address_encrypted
FROM main.privacy_tutorial.silver_customers
WHERE customerID = 1;
```

NULL across the board. That customer is gone.

---

## Quick recap

Here's what just happened, in plain terms:

**Crypto-shredding** is the pattern where you encrypt data with a key stored separately, then delete the key to make the data permanently unrecoverable. It's a recognised approach to GDPR right-to-erasure compliance for analytical data — you don't need to physically delete every row from every table. Just delete the key.

**Per-customer keys** mean that erasing one customer doesn't affect anyone else. Each row in `silver_customer_keys` is independent.

**Spark Declarative Pipelines** let you define tables as decorated Python functions. The engine handles dependencies, ordering, and incremental refreshes.

**Expectations** like `@dp.expect_or_drop()` are built-in data quality checks. Bad rows get filtered and logged. No extra framework needed.

**DABs** turn your project into a deployable bundle. YAML defines the what and where. The CLI does the rest.

---

## What to do next

Move the master secret to Databricks Secrets. That's the first thing. A hardcoded secret in source code is not a secret.

Make `silver_customer_keys` a streaming table. Right now it's a materialized view, which means a full refresh re-derives all keys from scratch. That works fine until someone deletes a key and then runs a full refresh — the key gets re-created from the source data. A streaming table processes new customers incrementally and preserves manual deletions. That's what you want in production.

Add an audit table. When a key is deleted, log it: who requested it, when, which customer. You'll want that paper trail.

Set up CI/CD. Add `databricks bundle validate` and `databricks bundle deploy -t staging` to your GitHub Actions workflow. Now every PR validates the bundle before it merges.

You've got a solid foundation. Build on it.
