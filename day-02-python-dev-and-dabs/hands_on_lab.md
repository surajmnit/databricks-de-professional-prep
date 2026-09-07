# Day 2 — Hands-On Lab: Python Project Structure, DABs, Dependencies, and UDFs

## Lab Objectives

1. Create a modular Python package for ETL use
2. Install it as a cluster library and test imports
3. Compare Python UDF vs Pandas UDF performance
4. Generate and validate a DAB configuration
5. Diagnose a library installation failure

**Note:** This lab assumes you have a running Databricks cluster. Steps 1–3 require a notebook. Steps 4–5 require the Databricks CLI installed locally (see prerequisites).

---

## Prerequisites

### For Steps 1–3 (Notebook)
- Running Databricks cluster (Community Edition sufficient)
- Attached notebook

### For Steps 4–5 (CLI)
- Databricks CLI installed (`pip install databricks-cli`)
- Authenticated (`databricks auth login`)
- A test workspace URL and token

---

## Step 1 — Build a Modular Python Package

**Objective:** Create a reusable ETL package that can be installed on a Databricks cluster.

### 1a. Create the package structure (run in a notebook cell)

```python
import os

# Create the package directory structure
base_path = "/Workspace/Shared/etl_project"
os.makedirs(f"{base_path}/etl_project/etl", exist_ok=True)
os.makedirs(f"{base_path}/etl_project/utils", exist_ok=True)

# Create __init__.py files (mark directories as Python packages)
for pkg in ["etl_project", "etl_project/etl", "etl_project/utils"]:
    init_file = f"{base_path}/{pkg}/__init__.py"
    with open(init_file, "w") as f:
        if pkg == "etl_project/etl":
            f.write('from .ingest import *\nfrom .transform import *\nfrom .validate import *\n')
        elif pkg == "etl_project/utils":
            f.write('from .logging_config import *\n')
        else:
            f.write('')
    print(f"Created: {init_file}")
```

### 1b. Create the ETL module files

```python
# Create etl/ingest.py
ingest_code = '''
from pyspark.sql import DataFrame

def read_bronze(spark, source_path: str, format: str = "parquet") -> DataFrame:
    """Read from bronze layer."""
    return (
        spark.read
        .format(format)
        .load(source_path)
    )

def read_from_adls(spark, account: str, container: str, path: str) -> DataFrame:
    """Read from Azure Data Lake Storage."""
    return spark.read.format("delta").load(f"abfss://{container}@{account}.dfs.core.windows.net/{path}")
'''

with open(f"{base_path}/etl_project/etl/ingest.py", "w") as f:
    f.write(ingest_code)
print("Created: etl/ingest.py")

# Create etl/transform.py
transform_code = '''
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, upper, trim, when

def clean_pii(df: DataFrame, pii_columns: list) -> DataFrame:
    """Mask PII columns by replacing with REDACTED."""
    for col_name in pii_columns:
        df = df.withColumn(col_name, when(col(col_name).isNotNull(), "REDACTED").otherwise(None))
    return df

def standardize_names(df: DataFrame, name_columns: list) -> DataFrame:
    """Uppercase and trim string columns."""
    for col_name in name_columns:
        df = df.withColumn(col_name, upper(trim(col(col_name))))
    return df
'''

with open(f"{base_path}/etl_project/etl/transform.py", "w") as f:
    f.write(transform_code)
print("Created: etl/transform.py")

# Create etl/validate.py
validate_code = '''
from pyspark.sql import DataFrame

def validate_not_null(df: DataFrame, columns: list) -> bool:
    """Return True if no null values in specified columns."""
    for col_name in columns:
        null_count = df.filter(col(col_name).isNull()).count()
        if null_count > 0:
            print(f"Validation failed: {col_name} has {null_count} null values")
            return False
    return True

def validate_row_count(df: DataFrame, min_rows: int) -> bool:
    """Return True if DataFrame has at least min_rows."""
    count = df.count()
    if count < min_rows:
        print(f"Validation failed: expected >= {min_rows} rows, got {count}")
        return False
    return True
'''

with open(f"{base_path}/etl_project/etl/validate.py", "w") as f:
    f.write(validate_code)
print("Created: etl/validate.py")

# Create utils/logging_config.py
logging_code = '''
import logging
import sys

def get_logger(name: str):
    """Get a configured logger."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger
'''

with open(f"{base_path}/etl_project/utils/logging_config.py", "w") as f:
    f.write(logging_code)
print("Created: utils/logging_config.py")

print("\nPackage structure created.")
```

### 1c. Create setup.py for wheel building

```python
setup_code = '''
from setuptools import setup, find_packages

setup(
    name="etl_project",
    version="1.0.0",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[
        "pyspark>=3.4.0",
    ],
    description="Reusable ETL utilities for Databricks",
)
'''

with open(f"{base_path}/setup.py", "w") as f:
    f.write(setup_code)
print("Created: setup.py")
```

**Expected output:** Package files are created at `/Workspace/Shared/etl_project/`.

**What to note:** The package name `etl_project` uses underscores (Python-safe). Directory `etl_project/` contains `etl/` and `utils/` subpackages.

---

## Step 2 — Test the Package and Python UDF vs Pandas UDF

**Objective:** Import the package, use the ETL functions, and compare UDF performance.

### 2a. Import and use the package (if cluster has it installed)

```python
import sys
sys.path.insert(0, "/Workspace/Shared/etl_project")

from etl_project.etl.ingest import read_bronze
from etl_project.etl.transform import clean_pii, standardize_names
from etl_project.etl.validate import validate_not_null, validate_row_count
from etl_project.utils.logging_config import get_logger

logger = get_logger("etl_lab")

# Test: Create sample data
sample_data = [
    ("john@example.com", "John Doe", 1000),
    ("jane@example.com", "Jane Smith", 2000),
    (None, "No Email User", 3000),
]
df = spark.createDataFrame(sample_data, ["email", "name", "revenue"])
print("Sample data:")
df.show()

# Test: Clean PII
df_clean = clean_pii(df, ["email"])
print("After PII cleaning:")
df_clean.show()

# Test: Standardize names
df_std = standardize_names(df_clean, ["name"])
print("After standardization:")
df_std.show()

# Test: Validations
print(f"Not null valid (email, name): {validate_not_null(df_std, ['name'])}")
print(f"Row count >= 2: {validate_row_count(df_std, 2)}")
```

**Expected output:** The ETL functions should execute successfully. The null email in row 3 should trigger a warning from `validate_not_null` (since it checks for null in `email`, which is now "REDACTED" — but wait, `clean_pii` replaced nulls with "REDACTED" only for non-null input; null stays null).

### 2b. Performance Comparison: Python UDF vs Pandas UDF

```python
import time
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf, udf
from pyspark.sql.types import IntegerType

# Generate test data: 1 million rows
print("Generating 1M rows...")
test_data = [(i, f"key_{i % 1000}", i * 1.5) for i in range(1_000_000)]
df = spark.createDataFrame(test_data, ["id", "category", "value"])
print(f"Partitions: {df.rdd.getNumPartitions()}")

# Python UDF (row-by-row)
@udf(returnType=IntegerType())
def add_hundred(value):
    if value is not None:
        return int(value + 100)
    return None

# Pandas UDF (vectorized batch)
@pandas_udf(IntegerType())
def add_hundred_pandas(value: pd.Series) -> pd.Series:
    return (value + 100).astype("int")

# Benchmark Python UDF
start = time.time()
python_result = df.withColumn("result", add_hundred("value")).agg(F.sum("result")).collect()
python_time = time.time() - start
print(f"Python UDF: {python_time:.2f}s")

# Benchmark Pandas UDF
start = time.time()
pandas_result = df.withColumn("result", add_hundred_pandas("value")).agg(F.sum("result")).collect()
pandas_time = time.time() - start
print(f"Pandas UDF: {pandas_time:.2f}s")

print(f"Pandas UDF speedup: {python_time / pandas_time:.1f}x faster")

# Verify correctness
print(f"Results match: {python_result[0][0] == pandas_result[0][0]}")
```

**Expected output:** Pandas UDF should be significantly faster (typically 5–20x). The exact speedup depends on cluster size, data size, and whether the operation is truly vectorizable.

**Break it on purpose:** Change the Pandas UDF to process row-by-row:

```python
# BROKEN: Row-by-row iteration defeats the purpose of Pandas UDF
@pandas_udf(IntegerType())
def add_hundred_broken(value: pd.Series) -> pd.Series:
    result = []
    for v in value:  # This is slow!
        result.append(int(v + 100) if v is not None else None)
    return pd.Series(result)

start = time.time()
broken_result = df.withColumn("result", add_hundred_broken("value")).agg(F.sum("result")).collect()
broken_time = time.time() - start
print(f"Broken Pandas UDF (row-by-row): {broken_time:.2f}s")
print(f"Now slower than Python UDF: {broken_time > python_time}")
```

This demonstrates that the performance advantage of Pandas UDFs comes from vectorization, not from the framework itself.

---

## Step 3 — Diagnose Library Installation Issues

**Objective:** Understand the difference between notebook-scoped and cluster-scoped installs, and how to troubleshoot failures.

### 3a. Notebook-scoped install (driver only)

```python
# Install a package in the current notebook session only
%pip install pyarrow --quiet

# Verify it's available
import pyarrow
print(f"pyarrow version: {pyarrow.__version__}")

# Check if it's available on executors (spoiler: it might not be!)
def check_executor_packages():
    import pandas as pd
    try:
        import pyarrow
        return f"pyarrow: {pyarrow.__version__}"
    except ImportError:
        return "pyarrow NOT available on executor"

# This runs on the driver, not executors
print(check_executor_packages())

# To truly test executors, create a UDF that checks:
@pandas_udf("string")
def check_packages_udf() -> pd.Series:
    import sys
    packages = sorted([p for p in sys.modules.keys() if 'pyarrow' in p.lower()])
    return pd.Series([str(packages)])

# Note: This UDF will show packages installed at cluster level, not notebook level
```

**What to observe:** `%pip install` packages are available to the driver but NOT automatically to executors unless they are installed at the cluster level.

### 3b. Common troubleshooting patterns

```python
# Pattern 1: Version conflict
# Symptom: Different behavior on driver vs executors
# Try: Check which PyArrow version is bundled with Spark
print(f"Spark's bundled PyArrow: {spark.conf.get('spark.sql.execution.arrow.pyspark.enabled')}")
import pyspark
print(f"Spark version: {pyspark.__version__}")

# Pattern 2: Package not found on workers
# Symptom: UDF fails with ModuleNotFoundError on executors but works on driver
# Solution: Install at cluster level, not notebook level

# Pattern 3: Java version mismatch
# For packages with native code (numpy, pandas, etc.)
# Databricks manages Java via DBR; native code must match DBR's Python environment
```

---

## Step 4 — Create and Validate a DAB (CLI Required)

**Objective:** Create a DAB configuration for the ETL project and validate it.

*This step requires the Databricks CLI. If not available, read through and understand the pattern.*

### 4a. Initialize a DAB structure

```bash
# Navigate to your project directory
cd /path/to/your/etl_project

# Initialize a new DAB
databricks bundle init

# This creates:
# - databricks.yml (bundle root config)
# - .databricks/ (bundle state directory, add to .gitignore)
```

### 4b. Edit the databricks.yml

```yaml
# databricks.yml
bundle:
  name: etl-project
  target: dev

targets:
  dev:
    workspace:
      host: https://your-workspace.cloud.databricks.com
    default_clusters:
      node_type_id: Standard_D4s_v3
      num_workers: 2
  prod:
    workspace:
      host: https://prod-workspace.cloud.databricks.com
    default_clusters:
      node_type_id: Standard_D8s_v3
      num_workers: 6

resources:
  jobs:
    bronze_job:
      name: ${bundle.target}-bronze-ingest
      tasks:
        - task_key: ingest_data
          notebook_task:
            notebook_path: /Workspace/Shared/etl_project/notebooks/bronze_ingest.py
          timeout_seconds: 1800
          retry_on_timeout: true
          max_retries: 2

    silver_job:
      name: ${bundle.target}-silver-transform
      tasks:
        - task_key: transform_data
          depends_on:
            - task_key: ingest_data
          notebook_task:
            notebook_path: /Workspace/Shared/etl_project/notebooks/silver_transform.py
          clusterless: true
```

### 4c. Validate and deploy

```bash
# Validate the bundle (checks YAML syntax, resource definitions)
databricks bundle validate

# Deploy to dev (dry run: --dry-run flag available)
databricks bundle deploy dev --no-prompts

# List deployed resources
databricks bundle list

# Run the deployed job
databricks bundle run dev bronze_job
```

**Expected output:** The CLI validates YAML, deploys resources, and shows deployment status.

**Break it on purpose:** Introduce a syntax error in the YAML:

```yaml
# BROKEN: Invalid YAML (tab character instead of spaces, or wrong indentation)
resources:
  jobs:
    bronze_job:
      name: ${bundle.target}-bronze-ingest
      tasks:
    	- task_key: ingest   # <-- tab indentation will fail
```

Run `databricks bundle validate` and observe the error message. This simulates a real deployment failure.

### 4d. Environment promotion

```bash
# Deploy to dev
databricks bundle deploy dev --no-prompts

# Promote to prod (same bundle, different target)
databricks bundle deploy prod --no-prompts

# Check what changed in prod vs dev
databricks bundle diff prod
```

---

## Step 5 — Diagnose a DAB Deployment Failure

**Objective:** Interpret common DAB error messages.

### Scenario: Authentication failure

```bash
$ databricks bundle deploy dev
Error: not authenticated. Run 'databricks auth login' first.
```

**Fix:** `databricks auth login --host https://your-workspace.cloud.databricks.com`

### Scenario: Workspace host mismatch

```bash
$ databricks bundle deploy prod
Error: bundle target 'prod' workspace host does not match current authenticated host
```

**Fix:** Authenticate to the correct workspace, or update the host in `databricks.yml`.

### Scenario: Resource already exists with different config

```bash
$ databricks bundle deploy dev
Error: Resource 'dev-bronze-ingest' already exists with different configuration.
Use --force to overwrite.
```

**Fix:** `databricks bundle deploy dev --force` (use with caution in production)

---

## Stretch Task

Create a complete ETL pipeline using the package you built, wrapped in a DAB:

1. Create a notebook at `/Workspace/Shared/etl_project/notebooks/bronze_ingest.py` that uses the package
2. Add the notebook to a DAB with two jobs: bronze (ingest) and silver (transform)
3. Configure silver to depend on bronze using `depends_on`
4. Deploy the DAB and trigger both jobs
5. Verify in the Jobs UI that bronze completes before silver starts

```python
# notebook skeleton for bronze_ingest.py
import sys
sys.path.insert(0, "/Workspace/Shared/etl_project")

from etl_project.etl.ingest import read_bronze
from etl_project.etl.validate import validate_row_count

# Ingest from sample data path
df = read_bronze(spark, "/path/to/source/data", "parquet")
assert validate_row_count(df, 1), "Ingestion validation failed"

# Write to bronze layer
df.write.format("delta").mode("overwrite").saveAsTable("bronze.my_table")
print("Bronze ingestion complete")
```

---

## Lab Checklist

- [ ] Created modular Python package structure
- [ ] Tested ETL functions via package import
- [ ] Benchmarked Python UDF vs Pandas UDF
- [ ] Observed Pandas UDF speedup from vectorization
- [ ] Verified notebook-scoped vs cluster-scoped library behavior
- [ ] Created and validated a DAB via CLI
- [ ] Deployed DAB to dev target
- [ ] Diagnosed and fixed a DAB YAML error
- [ ] (Stretch) Completed ETL pipeline in DAB with job dependency

---

## Cross-References

- **Day 3 (SQL Transformations + Testing):** Use DataFrame.transform for cleaner testing; integrate packages into ETL pipelines.
- **Day 25 (DABs + CI/CD):** Full CI/CD pipeline setup with GitHub Actions, environment promotion.
- **Day 23 (Debugging):** Use cluster logs and Spark UI to debug Python UDF failures on executors.
