# Databricks notebook source
# MAGIC %md
# MAGIC # Run Unit Tests
# MAGIC This notebook is executed as a job task before the pipeline refresh.
# MAGIC It runs pytest and fails the task if any test fails.

# COMMAND ----------

import subprocess
import sys

# Run pytest on the tests directory next to this notebook
result = subprocess.run(
    [sys.executable, "-m", "pytest", "-v", "--tb=short", "."],
    capture_output=True,
    text=True,
    cwd="/Workspace" + dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().rsplit("/", 1)[0],  # noqa: F821
)

print(result.stdout)
print(result.stderr)

if result.returncode != 0:
    raise Exception(f"Unit tests failed with return code {result.returncode}")
