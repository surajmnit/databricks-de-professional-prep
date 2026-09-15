# Day 2 — Python Development, Declarative Automation Bundles, Dependencies, and UDFs

> ⚠️ **Correction notice:** This version fixes several issues found in an earlier draft: (1) the `databricks.yml` example had a corrupted key (mixed English/Chinese characters) and used fabricated fields (`clusterless: true`, a bare `default_clusters` block, `pipeline_type`/`channels`/`contents: .jsonnet`) that don't match the real Bundle or Lakeflow pipeline schema; (2) the Iterator-Style Pandas UDF code had a syntax error and was invoked incorrectly; (3) the Grouped Map Pandas UDF example used the deprecated `PandasUDFType.GROUPED_MAP` decorator with no mention of the current `applyInPandas` API; (4) `spark.databricks.cluster.profile` was cited as a fix for version-shadowing without verification; (5) the claim that Pandas UDFs should use `returnType=IntegerType` (the bare class) rather than `IntegerType()` (an instance) was **backwards** — both `udf()` and `pandas_udf()` require an instantiated `DataType` (or a DDL string); passing the bare class raises a `TypeError`. All five are corrected below.

## Exam Objectives (Exam Guide, July 2026)

This day maps to **Section 1: Developing Code for Data Processing using Python and SQL — 22% of exam**

Specifically:

> "Design and implement a scalable Python project structure optimized for Declarative Automation Bundles (formerly Databricks Asset Bundles / DABs), enabling modular development, deployment automation, and CI/CD integration."

> "Manage and troubleshoot external third-party library installations and dependencies in Databricks, including PyPI packages, local wheels, and source archives."

> "Develop User-Defined Functions (UDFs) using Pandas/Python UDF."

---

## Part 1 — Scalable Python Project Structure

### Why Structure Matters for the Exam

Databricks Professional exam questions about deployment and CI/CD are scenario-based. A question about "which project structure supports modular development" is really testing whether you understand how Databricks resolves Python paths, how notebooks interact with packaged code, and how DABs reference modules.

### The Standard Databricks Python Project Structure

```
my-project/
├── README.md
├── requirements.txt                    # Python dependencies
├── setup.py                            # Package definition (optional)
├── .gitignore
│
├── my_project/                         # Package name (Python-safe: no dashes)
│   ├── __init__.py                     # Makes this a Python package
│   │
│   ├── etl/                            # ETL module
│   │   ├── __init__.py
│   │   ├── ingest.py                   # Ingestion functions
│   │   ├── transform.py                # Transformation functions
│   │   ├── validate.py                 # Validation functions
│   │   └── load.py                     # Load functions
│   │
│   ├── utils/                          # Shared utilities
│   │   ├── __init__.py
│   │   ├── logging_config.py
│   │   ├── schema_registry.py
│   │   └── secrets_helper.py
│   │
│   └── constants.py                    # Shared constants
│
├── notebooks/                          # Databricks notebooks (exported .py or .dbc)
│   ├── bronze_ingest.py
│   ├── silver_transform.py
│   └── gold_aggregate.py
│
└── databricks.yml                       # Declarative Automation Bundle definition
```

**Key rules for Python package naming:**
- Directory names must be Python-identifier safe: `my_project/` not `my-project/`
- Package name in `setup.py` can differ from directory name
- Subpackages: `my_project.etl.ingest` — the dot notation refers to module hierarchy

### How Databricks Resolves Python Imports

When you install a package into a Databricks cluster (via library installation or `%pip install`), the package is available to all notebooks attached to that cluster. The Python path includes:

1. `/databricks/python/lib/python3.x/site-packages/` — cluster-installed libraries
2. `/root/.local/lib/python3.x/site-packages/` — user-installed libraries
3. `dbfs:/...` — libraries installed from DBFS
4. `/Workspace/...` — notebooks and files in the workspace (accessible via `%run`)

**Important for the exam:** `%run` in a notebook makes another notebook's variables available in the current scope, but it does NOT respect Python module import semantics. For modular Python development, you should use:
- `import my_project.etl.ingest` after installing the package
- `%pip install my_package.whl` to install from a local wheel

**Exam trap:** `%run ./notebook` vs `import my_module` — these are not interchangeable. `%run` merges global scope; `import` respects module boundaries. Mixing them causes unexpected variable shadowing.

### The `__init__.py` File

Every directory that should be a Python package must contain `__init__.py`. It can be empty or can execute initialization code:

```python
# Empty — just marks this as a package
# (no content needed in Python 3.3+ for implicit namespace packages)

# Or with initialization:
from .ingest import ingest_from_adls
from .transform import apply_transformations

__all__ = ["ingest_from_adls", "apply_transformations"]
```

**Exam trap:** Forgetting `__init__.py` causes `ImportError: attempted relative import with no known parent package`. This is a common Python packaging error that shows up in troubleshooting questions.

---

## Part 2 — Declarative Automation Bundles (DABs)

### What Is a DAB?

Declarative Automation Bundles are YAML-based project definitions that enable reproducible, version-controlled deployments of Databricks resources. They replace the manual point-and-click UI workflow for creating jobs, clusters, and pipelines.

**Terminology reminder:** The exam guide (July 2026) uses "Declarative Automation Bundles." Older material (pre-2025) calls these "Databricks Asset Bundles (DABs)." Both refer to the same YAML-based deployment mechanism. Use the current name.

### The `databricks.yml` File

This is the heart of a DAB. Environment-specific compute is handled through **bundle variables**, not a bare per-target cluster block:

```yaml
# databricks.yml
bundle:
  name: my-etl-project

variables:
  node_type:
    description: Worker node type for job clusters
    default: Standard_D4s_v3
  num_workers:
    description: Number of workers for job clusters
    default: 2

targets:
  dev:
    default: true
    workspace:
      host: https://dbc-xxxx-ondbx.cloud.databricks.com
    variables:
      num_workers: 2
  staging:
    workspace:
      host: https://dbc-yyyy-ondbx.cloud.databricks.com
    variables:
      num_workers: 4
  prod:
    workspace:
      host: https://dbc-zzzz-ondbx.cloud.databricks.com
    variables:
      node_type: Standard_D8s_v3
      num_workers: 8
```

### Resources in a DAB

A DAB can declare multiple resource types. Job clusters are defined once under `job_clusters` and referenced by tasks via `job_cluster_key` — variables let the same definition resolve to different sizes per target:

```yaml
resources:
  jobs:
    bronze_ingest_job:
      name: ${bundle.target}-bronze-ingest
      job_clusters:
        - job_cluster_key: main
          new_cluster:
            spark_version: "15.4.x-scala2.12"
            node_type_id: ${var.node_type}
            num_workers: ${var.num_workers}
      tasks:
        - task_key: ingest
          job_cluster_key: main
          notebook_task:
            notebook_path: ./notebooks/bronze_ingest.py

    silver_transform_job:
      name: ${bundle.target}-silver-transform
      tasks:
        - task_key: transform
          depends_on:
            - task_key: ingest
          notebook_task:
            notebook_path: ./notebooks/silver_transform.py
          timeout_seconds: 3600
          max_retries: 2
          # No job_cluster_key/new_cluster block at all: in a serverless-enabled
          # workspace, a notebook task with no compute reference runs on
          # serverless compute automatically. There is no "clusterless: true"
          # flag — serverless is the absence of a cluster reference, not a flag.

  pipelines:
    live_etl_pipeline:
      name: ${bundle.target}-live-etl
      target: my_schema
      continuous: false
      channel: CURRENT
      libraries:
        - notebook:
            path: ./pipelines/etl_pipeline.py
      configuration:
        spark.databricks.delta.autoOptimize.enabled: "true"
```

**Exam trap:** Don't confuse a job cluster (`new_cluster`, tied to one job run and torn down after) with a bundle *variable* (a placeholder resolved per target). Fields like `pipeline_type`, `channels` (plural), or a `.jsonnet` pipeline source aren't part of the real schema — Lakeflow pipeline libraries are declared as `notebook:` or `file:` entries, and `channel` is a single string (`CURRENT` or `PREVIEW`), not a list.

### DAB CLI Commands

```bash
# Authenticate
databricks auth login --host https://dbc-xxxx-ondbx.cloud.databricks.com

# Initialize a new bundle in an existing project
databricks bundle init

# Validate bundle configuration
databricks bundle validate

# Deploy to dev target
databricks bundle deploy dev

# Deploy to production (with confirmation prompt)
databricks bundle deploy prod

# Run a job from the bundle
databricks bundle run dev bronze_ingest_job

# Generate a template
databricks bundle init --template python
```

**Exam trap:** `databricks bundle deploy` does NOT automatically trigger the deployed jobs. It creates or updates resources in the workspace. Jobs still need to be triggered manually or scheduled separately.

### Environment Promotion

A key DAB concept is environment promotion — deploying the same code to dev, staging, and prod with different configurations:

```yaml
# Different targets = same bundle, different configs
targets:
  dev:
    workspace:
      host: https://dev-workspace.cloud.databricks.com
  prod:
    workspace:
      host: https://prod-workspace.cloud.databricks.com
```

```bash
# Deploy to each environment
databricks bundle deploy dev
databricks bundle deploy prod
```

**Exam question pattern:** "A team wants to promote their ETL pipeline from dev to prod with minimal changes. Which approach supports this?" Answer: DABs with environment targets — same bundle, different target configs (and, where sizing differs, bundle variables overridden per target).

### DABs and CI/CD Integration

DABs are designed for Git-based CI/CD:

```yaml
# GitHub Actions example
jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Set up Databricks CLI
        uses: databricks/setup-cli@v3
      - name: Deploy to staging
        run: |
          databricks bundle deploy staging --no-prompts
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
```

---

## Part 3 — Third-Party Library Management

### Installation Methods in Databricks

| Method | Scope | Use Case |
|---|---|---|
| `%pip install <package>` | Notebook-scoped (current session) | Ad-hoc, quick testing |
| `%conda install <package>` | Notebook-scoped | Conda-based environments |
| Cluster-scoped library (UI/API/CLI) | All notebooks on cluster | Team-shared libraries |
| `dbutils.library.install()` | Notebook-scoped (legacy) | Legacy notebooks |
| Install from DBFS | Cluster or notebook | Pre-built wheels, custom packages |
| Install from PyPI | Cluster or notebook | Standard PyPI packages |
| Install from requirements.txt | Cluster | Bulk install |

### PyPI Packages

```python
# Notebook-scoped installation
%pip install great-expectations==0.18.0

# Multiple packages
%pip install pandas==2.1.0 numpy==1.26.0

# With constraints
%pip install -r /Workspace/my_project/requirements.txt
```

```bash
# Cluster-scoped via CLI
databricks libraries install --cluster-id <cluster-id> --pypi-package great-expectations==0.18.0

# From requirements file
databricks libraries install --cluster-id <cluster-id> --requirements /path/to/requirements.txt
```

### Local Wheels

A wheel (.whl) file is a pre-built Python package distribution:

```bash
# Build a wheel from your project
cd my_project
python -m pip wheel . -w dist/

# Install from wheel (DBFS path)
%pip install /dbfs/path/to/my_project-1.0.0-py3-none-any.whl
```

```python
# In DAB resource definition: install wheel as a cluster library
resources:
  jobs:
    my_job:
      tasks:
        - task_key: run_etl
          notebook_task:
            notebook_path: ./notebooks/etl.py
          libraries:
            - whl: ./dist/my_project-1.0.0-py3-none-any.whl
```

### Source Archives

```bash
# Install from a source archive (tar.gz or zip)
%pip install https://example.com/my_package.tar.gz

# From a GitHub release
%pip install git+https://github.com/user/repo@v1.2.3
```

### Troubleshooting Library Issues

**Problem: Package version conflict**
- Symptom: `ImportError` or `AttributeError` at runtime
- Cause: Two packages require incompatible versions of a shared dependency
- Solution: Pin exact versions in a `requirements.txt` installed at the cluster level, and prefer a dedicated cluster (or cluster policy) per team/project so version sets used by different workloads don't collide on a shared cluster

**Problem: Package not available on worker nodes**
- Symptom: Works on driver, fails on executors
- Cause: Notebook-scoped install only affects the driver; executors need cluster-scoped install
- Solution: Install at cluster level via UI or CLI, not via `%pip install`

**Problem: PyArrow version mismatch**
- Common issue with Pandas UDFs
- Spark has a bundled PyArrow version; user-installed version conflicts
- Solution: Use the bundled PyArrow (`import pyarrow` — no separate install needed) or install matching version

**Problem: Java library conflicts in PySpark**
- Some packages include JARs that conflict with Spark's bundled JARs
- Solution: Use cluster init scripts for JVM-level dependencies, not pip

---

## Part 4 — User-Defined Functions (UDFs)

### Python UDFs

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, StringType

# Simple Python UDF
@udf(returnType=IntegerType())
def add_one(x):
    return x + 1

# Register for SQL use
spark.udf.register("add_one", add_one)

# Use in DataFrame
df.withColumn("value_plus_one", add_one("value"))

# Use in SQL
spark.sql("SELECT add_one(value) FROM my_table")
```

**How Python UDFs work (internals):**
1. Driver serializes the UDF closure and sends it to each executor
2. Each executor spawns a separate Python process (not JVM)
3. Data is serialized from JVM to Python (via Py4J), processed row-by-row in Python
4. Results are serialized back to JVM
5. Row-by-row processing = **no vectorization** = slow

**Performance characteristics:**
- One row at a time: no parallelization within the UDF itself
- Python process overhead: significant for large datasets
- Memory: Python worker memory (`spark.python.worker.memory`, default 512MB per worker) is separate from executor heap

**When to use Python UDFs:**
- Logic is complex and cannot be expressed in Spark SQL primitives
- Small to medium datasets (not a good fit for large-scale transformation)

**When NOT to use Python UDFs:**
- High-volume production pipelines — use Spark SQL functions or Pandas UDFs instead
- When performance is critical

### Pandas UDFs

Pandas UDFs use Apache Arrow for zero-copy serialization between JVM and Python:

```python
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import IntegerType, StructType, StructField, StringType, LongType
import pandas as pd

# Scalar Pandas UDF (vectorized, processes batches)
@pandas_udf(IntegerType())
def add_one_batch(s: pd.Series) -> pd.Series:
    return s + 1

# Grouped Map — current API (Spark 3.0+): groupBy(...).applyInPandas(func, schema)
# The older @pandas_udf(..., PandasUDFType.GROUPED_MAP) decorator still runs but is
# deprecated as of Spark 3.0; applyInPandas is the current idiomatic approach and
# what the exam expects you to recognize.
output_schema = StructType([
    StructField("grp", StringType()),
    StructField("total", LongType()),
])

def sum_per_group(pdf: pd.DataFrame) -> pd.DataFrame:
    return pdf.groupby("grp", as_index=False).agg(total=("value", "sum"))

result_df = df.groupBy("grp").applyInPandas(sum_per_group, schema=output_schema)
```

**How Pandas UDFs work (internals):**
1. Data is serialized to Arrow format (zero-copy, columnar)
2. Arrow data is passed to Python in batch (not row-by-row)
3. Pandas operations process the batch as a whole (vectorized NumPy/Pandas)
4. Results serialized back to Arrow format
5. Arrow data converted back to Spark DataFrame

**Performance comparison:**

| Aspect | Python UDF | Pandas UDF |
|---|---|---|
| Processing | Row-by-row in Python subprocess | Batch (vectorized) via Arrow |
| Serialization | Py4J (pickle-like) | Apache Arrow (zero-copy) |
| Speed | Slow (~2–10x slower than Spark SQL) | Fast (near Spark SQL speed) |
| Memory | Python worker process heap | Uses Arrow, more memory efficient |
| Use case | Small data, complex logic | Medium-large data, vectorizable ops |

**Exam trap:** Not all Pandas UDFs are faster. A Pandas UDF that processes one row at a time (using `.itertuples()` inside) has no advantage. The speed comes from vectorized batch processing.

**Return type exam trap (corrected):** `returnType` on both `udf()` and `pandas_udf()` must be either an **instantiated** `DataType` object (e.g., `StringType()`, `IntegerType()`) or a DDL-formatted string (e.g., `"string"`, `"int"`). Passing the **bare class** without parentheses (`StringType` instead of `StringType()`) raises a `TypeError` immediately, for both decorator types — there is no version of this API where the un-instantiated class is the correct form.

### When to Choose Pandas UDF Over Python UDF

| Scenario | Recommended |
|---|---|
| Vectorizable arithmetic on columns | Pandas UDF |
| Complex Python logic per row | Python UDF (last resort) |
| Large dataset with simple transforms | Pandas UDF |
| Grouped aggregation with custom logic | `groupBy(...).applyInPandas(...)` |
| String manipulation (regex, etc.) | Spark SQL functions (faster than either UDF for strings) |

### Apache Spark SQL Functions as UDF Alternatives

Before writing a UDF, always check if a Spark SQL function exists:

```python
# Instead of a Python UDF:
@udf(returnType=StringType())
def normalize_email(email):
    return email.lower().strip() if email else None

# Use Spark SQL functions:
from pyspark.sql.functions import lower, trim, when, col
normalized = df.withColumn(
    "email",
    lower(trim(col("email")))
)
```

**Rule:** If a Spark SQL function can do it, use the SQL function — it executes within the JVM (no Python overhead) and Spark can optimize around it.

### Iterator-Style Pandas UDFs

For memory-constrained scenarios where an entire column shouldn't be materialized in Python at once, an **Iterator of Series** UDF processes the column in sequential batches, and can maintain running state (like a cumulative total) across those batches:

```python
from pyspark.sql.functions import pandas_udf
from typing import Iterator
import pandas as pd

@pandas_udf("long")
def running_total(batches: Iterator[pd.Series]) -> Iterator[pd.Series]:
    total = 0
    for batch in batches:
        cumulative = batch.cumsum() + total
        total = cumulative.iloc[-1]
        yield cumulative

result_df = df.select("id", running_total("value").alias("running_total"))
```

Each `batch` is one Arrow record batch worth of the input column (not the whole column at once), so memory stays bounded regardless of table size, while `total` carries the running state from one batch into the next. This is rarely tested directly on the exam but is worth recognizing as the pattern for "process a huge column without materializing all of it in Python."

---

## Glossary Updates

Add to `00-resources/glossary.md`:

| Term | Definition | Day |
|---|---|---|
| Declarative Automation Bundles | YAML-based project definitions for reproducible Databricks resource deployment | 2 |
| Python UDF | User-defined function executed row-by-row in a Python subprocess on executors | 2 |
| Pandas UDF | Vectorized user-defined function using Apache Arrow; processes data in batches | 2 |
| databricks.yml | Root configuration file for a Declarative Automation Bundle | 2 |
| Wheel (.whl) | Pre-built Python package distribution file | 2 |
| `%pip install` | Notebook-scoped Python package installation (driver only, not executors) | 2 |
| Cluster-scoped library | Python/Java library installed on all nodes of a cluster | 2 |
