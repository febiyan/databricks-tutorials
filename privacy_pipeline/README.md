# Let's Build a Privacy-Safe Data Pipeline on Databricks

You've got customer data. It has emails, phone numbers, addresses — the works. You need to make it usable for analytics without leaking anyone's personal info.

That's what we're building here. A full end-to-end pipeline that hashes PII with SHA-256, tested with real unit tests, and deployed with a single CLI command.

No fluff. Let's go.

---

> **IMAGE PROMPT — Hero / cover illustration:**
> A clean, modern isometric illustration of a data pipeline. On the left, a table icon labeled "Raw Customers" with rows showing blurred names and emails. In the middle, a shield icon with a lock, glowing softly. On the right, a clean table icon labeled "Hashed & Safe" where names are replaced with hexadecimal strings. Subtle arrows connect the three stages. Background: dark navy with a faint hexagonal grid pattern. Style: technical blog hero image, flat design with soft gradients, Databricks orange (#FF3621) as accent color.

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

Five PII columns. We're going to hash all of them.

---

## The game plan

We'll use the **medallion architecture** — three layers, each one cleaner than the last.

> **IMAGE PROMPT — Medallion architecture diagram:**
> A horizontal flow diagram with three rounded rectangles connected by arrows. Left: a bronze/copper-colored box labeled "Bronze — Raw Data" with a small table icon inside. Middle: a silver-colored box labeled "Silver — PII Hashed" with a lock icon. Right: a gold-colored box labeled "Gold — Aggregated" with a bar chart icon. Below each box, a one-liner: "All columns, as-is", "SHA-256 replaces names & emails", "Counts by region, fully anonymous". White background, clean lines, minimal style. The boxes have subtle metallic gradients matching their medallion color.

**Bronze** reads the raw table. Nothing fancy. Just a snapshot so we have a clean starting point.

**Silver** is where the magic happens. We swap out every PII column for a SHA-256 hash. Same input always gives the same hash, so you can still join tables downstream. But you can't reverse the hash to get the original value back.

**Gold** aggregates everything into counts by region and gender. No individual-level data at all. Safe for anyone to query.

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
│   ├── pii_utils.py              # Hash helpers (the reusable bits)
│   └── privacy_pipeline.py       # The actual pipeline (bronze/silver/gold)
└── tests/
    ├── test_pii_utils.py         # 9 tests for the hash functions
    ├── test_privacy_pipeline.py  # 8 tests for the pipeline logic
    └── run_tests_notebook.py     # Notebook wrapper so tests run as a job task
```

Seven files. That's the whole thing.

> **IMAGE PROMPT — Project structure visual:**
> A file tree visualization resembling a code editor sidebar. The root folder "privacy_pipeline" is expanded, showing databricks.yml at the top, then three sub-folders (resources, src, tests) each expanded with their files. Each file has a small icon: YAML files get a gear icon, Python files get the Python logo, test files get a green checkmark. Use a dark editor theme (VS Code-like) with syntax-colored filenames. Clean, crisp, developer-friendly aesthetic.

---

## Step 1 — The hashing helpers

Open `src/pii_utils.py`. Two functions, that's all.

`hash_pii("email_address")` takes a column name, appends a salt, and returns the SHA-256 hash. The output column gets named `email_address_hash` automatically.

`hash_full_name("first_name", "last_name")` is a bit smarter. It lowercases both names, trims whitespace, concatenates them, then hashes. So "Alice Smith" and "  ALICE   SMITH  " produce the same hash.

The salt is a string we mix into every value before hashing. It stops anyone from running a rainbow table attack — where they pre-hash common emails and match them against your output. In production, you'd pull this from Databricks Secrets:

```python
PII_SALT = dbutils.secrets.get(scope="pii", key="hash_salt")
```

For the tutorial, it's just a constant. Good enough to learn with.

> **IMAGE PROMPT — How salted hashing works:**
> A simple left-to-right diagram on a white background. On the left, a blue rounded box with "alice@example.com" inside. An arrow points right to a green box labeled "+ salt" where the email and a key icon merge. Another arrow points to an orange box showing "a3f8c1d9e2..." (a truncated hash). Below the diagram, two small comparison rows: Row 1 shows "Without salt: alice@example.com → 2c6ee..." with a red X (vulnerable to rainbow tables). Row 2 shows "With salt: alice@example.com + salt → a3f8c1..." with a green checkmark. Clean, educational, infographic style.

---

## Step 2 — The pipeline

Open `src/privacy_pipeline.py`. Three decorated functions. That's the whole pipeline.

### Bronze

```python
@dp.materialized_view(name="bronze_customers")
def bronze_customers():
    return spark.read.table("samples.bakehouse.sales_customers")
```

One read. One return. Done.

Why bother? Because you want a snapshot of the raw data inside your pipeline's catalog. If the source changes, you have a record. It's cheap insurance.

### Silver

This is where the PII goes away:

```python
@dp.materialized_view(name="silver_customers_hashed")
@dp.expect_or_drop("valid_customer_id", "customerID IS NOT NULL")
def silver_customers_hashed():
    df = spark.read.table("bronze_customers")
    return df.select(
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
```

A few things to notice.

The `@dp.expect_or_drop` decorator is a data quality gate. Any row with a NULL `customerID` gets quietly dropped. It still shows up in the pipeline event log so you know it happened, but it won't pollute your downstream tables.

We're not just dropping PII columns — we're *replacing* them. The `select()` call explicitly lists what makes it to the output. `first_name` and `last_name` are gone. `full_name_hash` takes their place. Same for email, phone, and address.

City, state, country, and the rest? They stay. They're useful for analytics and they're not personally identifiable on their own.

> **IMAGE PROMPT — Before and after table comparison:**
> Two side-by-side table mockups. Left table titled "Bronze (raw)" shows 3 rows with columns: customerID, first_name, last_name, email_address, phone_number, address — with realistic fake data visible (e.g., "Alice", "Smith", "alice@example.com"). Right table titled "Silver (hashed)" shows the same 3 rows but now has columns: customerID, full_name_hash, email_address_hash, phone_number_hash, address_hash — with long hex strings like "a3f8c1d9..." in each cell. The PII columns on the left have a semi-transparent red overlay. The hash columns on the right have a green overlay. A curved arrow connects the two tables. Clean, flat design with a light background.

### Gold

```python
@dp.materialized_view(name="gold_customers_by_region")
def gold_customers_by_region():
    df = spark.read.table("silver_customers_hashed")
    return df.groupBy("country", "state", "city", "gender").agg(
        F.count("*").alias("customer_count"),
        F.countDistinct("email_address_hash").alias("unique_emails"),
    )
```

Aggregate. Group. Count. No individual records survive.

This is the table you hand to analysts, dashboards, and stakeholders. They get "Seattle has 142 female customers" — not "Alice Smith lives at 123 Main St."

---

## Step 3 — The tests

This is the part people usually skip. Don't.

We have 13 tests across two files. They run in under a minute and they'll save you from shipping a pipeline that accidentally leaks PII in a column you forgot to hash.

### What we test

**`test_pii_utils.py`** — the hash functions in isolation:

- Same email in, same hash out? (determinism)
- Is the output actually 64 hex characters? (SHA-256 format)
- Does the salt change the output vs. plain SHA-256? (salting works)
- "Alice Smith" vs "alice smith" — same hash? (case insensitivity)
- "  Alice  " vs "Alice" — same hash? (whitespace trimming)
- NULL in → NULL out, no crash? (NULL safety)
- Two different emails → two different hashes? (no collisions)
- Output column named correctly? (alias check)

**`test_privacy_pipeline.py`** — the pipeline layers with a small test DataFrame:

- Does silver drop rows where customerID is NULL?
- Is `first_name` gone from the silver output? What about `last_name`, `email_address`, `phone_number`, `address`?
- Are `full_name_hash`, `email_address_hash`, etc. present?
- Do non-PII columns like `city` and `state` pass through untouched?
- Is the hashed value actually different from the original value?
- Does gold produce the right counts per region?
- Does gold contain zero PII columns?

> **IMAGE PROMPT — Test results screenshot mockup:**
> A mockup of the Databricks workspace testing sidebar. Dark theme. The sidebar shows a list of 13 test cases, all with green checkmark icons. They're organized under two collapsible sections: "TestHashPii (5 tests)" and "TestHashFullName (4 tests)" under test_pii_utils.py, then "TestSilverLayer (5 tests)" and "TestGoldLayer (3 tests)" under test_privacy_pipeline.py. At the top, a green banner reads "13 passed, 0 failed". The bottom panel shows "PASSED" next to the last-run test with execution time "0.8s". Realistic Databricks UI styling.

### How to run them

**In the workspace:** Open any test file. The Tests sidebar appears automatically on the left. Hit the play-all button. That's it.

**Locally:**

```bash
cd privacy_pipeline
pip install pyspark pytest
pytest tests/ -v
```

You'll see each test name, a PASSED/FAILED status, and a total at the bottom.

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

A **job** with two tasks chained together:

1. `run_unit_tests` — runs the test notebook. If anything fails, the job stops here.
2. `refresh_pipeline` — only runs if tests pass. Triggers a full pipeline refresh.

The job runs daily at 8 AM UTC. Change the cron expression if you want a different schedule.

> **IMAGE PROMPT — DAB deployment flow diagram:**
> A vertical flow diagram with 4 steps, connected by downward arrows. Step 1: a laptop icon with "databricks bundle validate" — label: "Check YAML syntax & references". Step 2: a cloud upload icon with "databricks bundle deploy" — label: "Push code & config to workspace". Step 3: a play button icon with "databricks bundle run" — label: "Trigger the job". Step 4: two connected task boxes inside a dashed border labeled "Job execution": first box is "Run unit tests" (with a test tube icon), arrow to second box "Refresh pipeline" (with a pipeline/flow icon). A red X branch from the first box goes to "Stop — tests failed". Flat design, muted colors with Databricks orange for the action icons.

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

# Run the pipeline directly
databricks bundle run privacy_pipeline

# Or run the full job (tests first, then pipeline)
databricks bundle run privacy_pipeline_job
```

For production:

```bash
databricks bundle deploy -t prod
databricks bundle run -t prod privacy_pipeline_job
```

Three commands. You're live.

---

## Step 6 — Check your work

Once the pipeline finishes, open a SQL editor in your workspace and poke around:

```sql
-- Silver: hashed PII, no raw names or emails
SELECT * FROM main.privacy_tutorial.silver_customers_hashed LIMIT 10;

-- Confirm the schema — no first_name, no email_address
DESCRIBE main.privacy_tutorial.silver_customers_hashed;

-- Gold: aggregated, fully anonymous
SELECT * FROM main.privacy_tutorial.gold_customers_by_region
ORDER BY customer_count DESC;
```

If you see hex strings where names used to be, and counts where rows used to be — you're done.

> **IMAGE PROMPT — Query results mockup:**
> A Databricks SQL editor mockup showing query results. The top half shows the SQL query `SELECT * FROM main.privacy_tutorial.silver_customers_hashed LIMIT 5`. The bottom half shows a results table with columns: customerID (showing integers like 1, 2, 3), full_name_hash (showing long hex strings like "a3f8c1d9e2b7..."), email_address_hash (more hex), phone_number_hash (more hex), address_hash (more hex), city ("Seattle", "Portland"), state ("WA", "OR"), country ("US"), gender ("Female", "Male"). The hex strings are subtly highlighted in green to draw attention. Dark theme, realistic Databricks SQL UI chrome.

---

## Quick recap

Here's what just happened, in plain terms:

**Spark Declarative Pipelines** let you define tables as decorated Python functions. You write `@dp.materialized_view()`, return a DataFrame, and the engine handles the rest — dependencies, ordering, incremental refreshes.

**Expectations** like `@dp.expect_or_drop()` are built-in data quality checks. Bad rows get filtered and logged. No extra framework needed.

**Workspace unit tests** work with standard pytest conventions. Name your file `test_*.py`, write `test_` functions, and the sidebar picks them up. You can run them locally too.

**DABs** turn your project into a deployable bundle. YAML defines the what and where. The CLI does the rest.

---

## What to do next

Swap the hardcoded salt for a real secret. That's the first thing.

After that, think about streaming. Change `bronze_customers` from `@dp.materialized_view()` to `@dp.table()` with `spark.readStream` and you get incremental ingestion.

Add more expectations. Validate email format with a regex. Check that phone numbers have the right length. Flag addresses that are suspiciously short.

Set up CI/CD. Add `databricks bundle validate` and `databricks bundle deploy -t staging` to your GitHub Actions workflow. Now every PR validates the bundle before it merges.

You've got a solid foundation. Build on it.
