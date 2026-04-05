# Databricks Tutorials

This repo contains self-contained Databricks tutorials, each in its own folder with its own DAB (Declarative Automation Bundle).

## Current tutorials

- `privacy_pipeline/` — GDPR-compliant data pipeline using crypto-shredding (AES-256 per-customer encryption + key deletion for right-to-erasure)

## Privacy pipeline

### Architecture

Two silver tables, no bronze or gold layer:
- `silver_customer_keys` — one AES-256 key per customer, derived from `customerID + MASTER_SECRET`
- `silver_customers` — PII fields encrypted with the customer's key; LEFT JOIN means deleting a key row makes that customer's PII NULL

Source: `samples.bakehouse.sales_customers` (built-in Databricks sample dataset).

### Running tests locally

```bash
cd privacy_pipeline
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pytest tests/ -v
```

Tests use a local Spark session (`local[*]`) — no workspace needed.

### Deploying

```bash
cd privacy_pipeline
databricks bundle validate
databricks bundle deploy          # dev (default)
databricks bundle deploy -t prod  # production
databricks bundle run customers_hashed_job
```

### Key conventions

- `pii_utils.py` exports exactly two public functions: `derive_customer_key` and `encrypt_pii`. Keep it that way.
- Pipeline functions in `privacy_pipeline.py` use `spark` as an injected global — do not create a SparkSession there.
- Tests import from `src/` via `sys.path.insert` (not relative imports) so they work both locally and in the Databricks workspace testing sidebar.
- `MASTER_SECRET` in `pii_utils.py` is a placeholder — production value comes from Databricks Secrets.
- The README is the published tutorial (Substack). Keep language casual and direct. IMAGE PROMPT blocks are intentional — leave them in.
