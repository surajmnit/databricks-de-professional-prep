# Day 2 — Quiz: Python Development, DABs, Dependencies, and UDFs

**Objective coverage:** Section 1 — Developing Code for Data Processing using Python and SQL (22%)

---

## Question 1

**Objective:** Design Python project structure for Declarative Automation Bundles.

A data engineering team is building a Python package for their ETL pipeline. The project structure is:

```
etl_project/
├── etl/
│   ├── __init__.py
│   └── ingest.py
├── utils/
│   ├── __init__.py
│   └── logger.py
└── setup.py
```

A notebook attempts to import with:

```python
from etl.ingest import read_bronze
```

The import fails with `ModuleNotFoundError: No module named 'etl'`.

Which root cause is most likely?

A. The `etl_project/` directory name uses underscores, which Python does not allow
B. The `etl/` directory is missing `__init__.py`
C. The package was not installed on the cluster before importing
D. `setup.py` is malformed and preventing package registration

---

## Question 2

**Objective:** Manage third-party library installations in Databricks.

A data scientist installs a package in a notebook using `%pip install lightgbm==4.0.0`. The package works in cells that run on the driver, but a Python UDF that relies on `lightgbm` fails on all executors with `ModuleNotFoundError`.

Which explanation is most accurate?

A. LightGBM requires a native Java library, which is not supported via pip
B. `%pip install` only affects the driver node; executors require cluster-scoped installation
C. Python UDFs cannot access any pip-installed packages due to sandboxing
D. The executor Python environment uses a different Python version than the driver

---

## Question 3

**Objective:** Develop UDFs using Pandas vs Python UDF.

A pipeline processing 500 million rows uses a Pandas UDF to calculate a running total per group:

```python
@pandas_udf("double")
def running_total(s: pd.Series) -> pd.Series:
    return s.cumsum()
```

The team notices the UDF is running slower than expected. Profiling reveals the internal loop processes one row at a time due to a custom aggregation requirement.

What is the most likely cause of the slowdown?

A. The Pandas UDF is running in a Python subprocess that is not vectorized
B. The return type `double` is not compatible with Pandas UDFs
C. Grouped aggregation inside a Pandas UDF requires `groupBy(...).applyInPandas(...)`, not a scalar UDF
D. 500 million rows exceeds the maximum batch size for Pandas UDFs

---

## Question 4

**Objective:** Design project structure for Declarative Automation Bundles.

A DevOps engineer is configuring a DAB for an ETL project with three environments: dev, staging, prod. They want the same code deployed to all three environments with only the compute configuration changing.

Which databricks.yml pattern correctly implements this?

A. Create three separate databricks.yml files: databricks-dev.yml, databricks-staging.yml, databricks-prod.yml
B. Use a single databricks.yml with a `targets` section containing dev, staging, and prod configurations
C. Use environment variables in the YAML and pass them via `databricks bundle deploy --var`
D. Create separate bundle directories for each environment under a monorepo structure

---

## Question 5

**Objective:** Manage library dependencies and troubleshoot installation issues.

A team reports that their PySpark job works on their local machine but fails on Databricks with `ImportError: cannot import name 'geopandas' from 'pandas'`. Both environments use pandas 2.1.0.

Which investigation is most likely to reveal the root cause?

A. Check if the Databricks cluster DBR version includes a bundled pandas that shadows the installed version
B. Verify that geopandas is installed at the cluster level, not just the notebook level
C. Confirm the Python version on Databricks matches the local machine (3.8 vs 3.9)
D. Check if the cluster has an instance profile that restricts network access to PyPI

---

## Question 6

**Objective:** Deploy Databricks resources using Declarative Automation Bundles.

A data engineer runs `databricks bundle deploy prod` but the command fails with:

```
Error: Resource 'prod-analytics-job' already exists with different configuration.
```

The engineer wants to update the existing job with the new configuration.

Which command resolves this?

A. `databricks bundle deploy prod --overwrite`
B. `databricks bundle deploy prod --force`
C. `databricks bundle deploy prod --update`
D. `databricks bundle deploy prod --ignore-existing`

---

## Question 7

**Objective:** Develop UDFs using Pandas UDF — return type must be an instantiated `DataType`.

A data engineer writes the following Pandas UDF:

```python
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType
import pandas as pd

@pandas_udf(returnType=StringType)
def mask_email(email: pd.Series) -> pd.Series:
    return email.str.replace(r'(?<=.{2}).(?=[^@]+@)', '*', regex=True)
```

This raises a `TypeError` the moment the UDF is defined, before any data is processed. What is the most likely root cause?

A. `returnType` was passed the `StringType` class itself instead of an instantiated `StringType()` object
B. The regex pattern uses a lookbehind, which Arrow-based UDFs cannot serialize
C. Pandas UDFs cannot return `StringType` at all — only numeric types are supported
D. The function is missing an explicit `functionType=PandasUDFType.SCALAR` argument, which current Spark versions require

---

## Question 8

**Objective:** Design scalable Python project structure for modular development.

A team structure their project as follows:

```
data_pipeline/
├── pipeline.py         # Main ETL logic
├── helpers.py          # Helper functions
└── config.py           # Configuration
```

All notebooks in the workspace import from `pipeline.py` using `import pipeline`. Over time, they find that variable conflicts occur when multiple notebooks are run simultaneously in the same session.

Which project restructuring resolves this while maintaining CI/CD compatibility?

A. Move all code into a single notebook to avoid module conflicts
B. Use `dbutils.notebook.run()` to invoke other notebooks instead of Python imports
C. Convert to a proper Python package structure with `__init__.py` and install on the cluster
D. Prefix all function names with a unique namespace to avoid collisions

---

## Question 9

**Objective:** Deploy and configure Databricks resources via DABs.

A DAB is deployed to a dev workspace. After some weeks, the team realizes that `databricks bundle run dev` is triggering jobs that reference the old notebook paths. The databricks.yml has:

```yaml
resources:
  jobs:
    my_job:
      tasks:
        - task_key: run_etl
          notebook_task:
            notebook_path: ./notebooks/etl.py
```

The notebook was moved from `./notebooks/etl.py` to `./notebooks/etl_v2.py` but the bundle was not re-deployed.

When the engineer runs `databricks bundle deploy dev --no-prompts`, which behavior occurs?

A. The bundle detects the path change and automatically updates the job configuration
B. The bundle deploys the old path and overwrites the job to point to the non-existent notebook
C. The bundle fails validation because the referenced notebook does not exist at the path
D. The bundle deploys but leaves the existing job unchanged (no diff detected)

---

## Question 10

**Objective:** Develop Python UDFs and understand performance implications.

A production pipeline processes 10 million rows per day using a Python UDF:

```python
@udf(returnType=IntegerType())
def calculate_score(x, y, z):
    return int(math.sqrt(x**2 + y**2 + z**2))
```

The team is migrating to Databricks and wants to optimize this UDF for production scale.

Which approach is most appropriate?

A. Replace with an equivalent Spark SQL expression using `sqrt` and basic arithmetic
B. Increase `spark.python.worker.memory` to allow more rows per batch
C. Convert to a Pandas UDF and ensure the math operations are vectorizable
D. Both A and C are valid approaches; choose based on whether the expression is expressible in SQL

---

## Answer Key

### Question 1: **Answer C — The package was not installed on the cluster before importing.**

**Why:** Simply placing files in `/Workspace/` does not make a directory a Python package at import time. Python's import system requires either:
1. The package to be installed via `pip install -e /Workspace/path/to/package`, OR
2. The path to be added to `sys.path` via `sys.path.insert(0, path)`

Placing files in `/Workspace/` makes them accessible for `%run` (notebook-to-notebook), but does not make them importable as Python modules. This is a fundamental distinction the exam tests.

**Why the other options are wrong:**
- A: Underscores are valid in directory names; hyphens are the problem. `etl/` is a valid directory name.
- B: `__init__.py` exists in the structure shown — this is not the problem.
- D: `setup.py` is not required for a directory to be importable; it's only needed for building a distributable package. The immediate problem is the missing install or `sys.path` adjustment.

**Exam trap:** Students often assume that files in `/Workspace/` are automatically importable like a standard Python project. They are not — Databricks requires either installation or explicit `sys.path` manipulation.

---

### Question 2: **Answer B — `%pip install` only affects the driver node; executors require cluster-scoped installation.**

**Why:** `%pip install` is a notebook-scoped operation. It installs the package into the Python environment of the **driver** process only. Executors run their own Python subprocess processes that are not affected by notebook-scoped installs. Any library used in a Python UDF must be installed at the **cluster level** (via the Databricks UI, CLI, or API).

**Why the other options are wrong:**
- A: LightGBM is a pure Python package (with optional native acceleration); it is fully supported via pip on Databricks.
- C: Python UDFs can access pip-installed packages — just not packages installed only at the notebook scope. If installed at the cluster level, they are accessible.
- D: Executor Python version matches the driver by default. This is not the cause.

**Exam trap:** The specific detail that distinguishes this question is "works on the driver but fails on executors." This is the canonical signature of a notebook-scoped-only install. The fix is cluster-level installation.

---

### Question 3: **Answer C — Grouped aggregation inside a Pandas UDF requires `groupBy(...).applyInPandas(...)`, not a scalar UDF.**

**Why:** The UDF uses `s.cumsum()` on the entire Series — this calculates a global running total, not a per-group running total. For per-group operations, Spark's current API is `groupBy(...).applyInPandas(func, schema)`, which passes one Pandas DataFrame per group to the function:

```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

schema = StructType([
    StructField("grp", StringType()),
    StructField("value", DoubleType()),
    StructField("running_total", DoubleType()),
])

def running_total_per_group(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf["running_total"] = pdf["value"].cumsum()
    return pdf

result = df.groupBy("grp").applyInPandas(running_total_per_group, schema=schema)
```

A scalar Pandas UDF (as written) receives data batch by batch, where each batch may not represent a complete group — so a global `cumsum()` gives wrong results.

**Why the other options are wrong:**
- A: Pandas UDFs ARE vectorized and do run in a Python subprocess. The described behavior (one row at a time due to custom aggregation) is not a Pandas UDF limitation but a code design issue.
- B: `double` (as a DDL string) or `DoubleType()` (as an instance) are both valid Pandas UDF return types; there is no compatibility issue.
- D: There is no fixed maximum batch size that causes Pandas UDFs to fail. Large datasets are processed in batches within the Arrow pipeline.

**Exam trap:** The question describes a "custom aggregation requirement" that processes one row at a time — this is a code-level design problem, not a Pandas UDF framework problem. The exam tests whether you know that grouped operations need `applyInPandas`, not a scalar `pandas_udf`.

---

### Question 4: **Answer B — Use a single databricks.yml with a `targets` section containing dev, staging, and prod configurations.**

**Why:** DABs are designed for environment promotion via the `targets` block in a single `databricks.yml`. The same bundle definition (resources, pipelines, notebooks) is deployed to different workspaces/environments by changing the target, with sizing differences handled via bundle **variables** overridden per target:

```yaml
# One databricks.yml, multiple targets
bundle:
  name: etl-project
targets:
  dev:     # deploy: databricks bundle deploy dev
    workspace:
      host: https://dev-workspace.cloud.databricks.com
  staging: # deploy: databricks bundle deploy staging
    workspace:
      host: https://staging-workspace.cloud.databricks.com
  prod:    # deploy: databricks bundle deploy prod
    workspace:
      host: https://prod-workspace.cloud.databricks.com
```

**Why the other options are wrong:**
- A: Multiple YAML files are not the DAB pattern. Databricks.yml is the single source of truth.
- C: While `--var` flags exist for one-off overrides, the primary pattern for environment config is the `targets` block with per-target `variables`, not passing everything via CLI flags.
- D: Separate bundle directories per environment are not the pattern — one bundle, multiple targets.

**Exam trap:** The exam may present a scenario with "three databricks.yml files" as a distractor. The correct pattern is always a single bundle with multiple targets.

---

### Question 5: **Answer A — Check if the Databricks cluster DBR version includes a bundled pandas that shadows the installed version.**

**Why:** Databricks Runtime (DBR) bundles a specific version of pandas that is installed at the system level. When you install pandas via `%pip install pandas==2.1.0`, it goes into the user site-packages directory, which may or may not be on the Python path ahead of the DBR-bundled pandas. In many DBR versions, the DBR-bundled packages are loaded first, shadowing user-installed versions.

The error `cannot import name 'geopandas' from 'pandas'` suggests a version conflict between pandas and geopandas that occurs when the wrong pandas version is imported.

**Why the other options are wrong:**
- B: "Cluster level" is the correct fix for driver-only installs, but the error is about geopandas specifically, not about the package not being found at all.
- C: Databricks DBR uses a consistent Python version across driver and executors. Version mismatch is possible but not the primary cause of this import error.
- D: Network access to PyPI is required for pip install, but if the install succeeded (as stated in the scenario), the package was accessible. The error occurs at runtime due to import path ordering.

**Exam trap:** Databricks Runtime bundles many packages at the system level. This can cause shadowing where user-installed packages don't override system packages. This is a known Databricks packaging issue.

---

### Question 6: **Answer B — `databricks bundle deploy prod --force`**

**Why:** When a resource already exists with a different configuration, `databricks bundle deploy` by default refuses to overwrite it. The `--force` flag tells the CLI to update the existing resource with the new configuration.

**Why the other options are wrong:**
- A: `--overwrite` is not a valid Databricks CLI flag for bundle deploy.
- C: `--update` is not a valid flag.
- D: `--ignore-existing` is not a valid flag.

**Exam trap:** The flag is `--force`, not `--overwrite`. This is a specific CLI flag that candidates must know.

---

### Question 7: **Answer A — `returnType` was passed the `StringType` class itself instead of an instantiated `StringType()` object.**

**Why:** `pandas_udf`'s `returnType` parameter (like `udf`'s) must be either an **instantiated** `pyspark.sql.types.DataType` object (e.g., `StringType()`) or a DDL-formatted type string (e.g., `"string"`). Passing the bare class — `StringType` with no parentheses — is not a valid `DataType` instance, and Spark raises a `TypeError` immediately when the decorator is evaluated, before the UDF ever touches data. The fix is simply:

```python
@pandas_udf(returnType=StringType())   # instantiated — correct
# or
@pandas_udf("string")                  # DDL string — also correct
```

**Why the other options are wrong:**
- B: Arrow serialization doesn't care about regex complexity — there's no such limitation, and the error described happens at *definition* time, before the regex is ever evaluated against data.
- C: False — Pandas UDFs support the full range of Spark SQL types, not just numeric ones.
- D: False — `functionType` defaults to `PandasUDFType.SCALAR` and does not need to be passed explicitly in current Spark versions; the type hints on the function signature are what current Spark uses to infer UDF behavior.

**Exam trap:** It's tempting to memorize "always use the class, not the instance" or vice versa as a blanket rule — the actual rule is simpler and applies identically to both `udf()` and `pandas_udf()`: **always instantiate** (`StringType()`), whether you pass a `DataType` object or skip it entirely in favor of a DDL string (`"string"`).

---

### Question 8: **Answer C — Convert to a proper Python package structure with `__init__.py` and install on the cluster.**

**Why:** Variable conflicts from simultaneous notebook execution in the same session happen because `%run` merges global scope — all variables from the imported notebook are injected into the caller's global namespace. Converting to a proper Python package with explicit imports (`from pipeline import ETLClass`) ensures module isolation. Each notebook gets its own module instance, preventing global namespace pollution.

**Why the other options are wrong:**
- A: Single notebook defeats the purpose of modular development and makes CI/CD harder.
- B: `dbutils.notebook.run()` is for workflow orchestration (notebook-as-task), not for code reuse within a session.
- D: Prefixing names helps but does not resolve the fundamental architectural problem of global scope merging.

**Exam trap:** `%run` and `import` have fundamentally different scoping semantics. `%run` is fine for simple notebook chains but causes namespace pollution in complex projects. The exam distinguishes between simple notebook orchestration and scalable project structure.

---

### Question 9: **Answer B — The bundle deploys the old path and overwrites the job to point to the non-existent notebook.**

**Why:** DABs do NOT validate that referenced notebooks exist at deploy time. The `notebook_path: ./notebooks/etl.py` in the YAML is a string reference — the bundle deploys it as-is without checking the actual notebook location. After deployment, the job in the workspace UI points to the non-existent path. The job will fail at runtime when it tries to execute the missing notebook.

**Why the other options are wrong:**
- A: DABs do not auto-detect path changes. You must update the YAML manually.
- C: `databricks bundle validate` checks YAML syntax but does not verify notebook existence.
- D: The bundle WILL overwrite the existing job's notebook path reference — the job will be updated, just to a broken path.

**Exam trap:** Students often assume that `databricks bundle deploy` validates the workspace state. It does not — it writes the configuration as declared, regardless of whether the referenced resources exist. This is explicitly tested.

---

### Question 10: **Answer D — Both A and C are valid approaches; choose based on whether the expression is expressible in SQL.**

**Why:** The expression `sqrt(x^2 + y^2 + z^2)` can be fully expressed in Spark SQL:

```python
from pyspark.sql.functions import sqrt, col
df.withColumn("score", sqrt(col("x")**2 + col("y")**2 + col("z")**2))
```

This runs entirely within the JVM (no Python subprocess) and is the fastest option. However, if the logic is complex and cannot be expressed in SQL, a Pandas UDF would be appropriate as long as the operations are vectorizable.

**Why the other options are wrong:**
- A: Correct but not the most complete answer — Pandas UDF is also valid.
- B: Increasing `spark.python.worker.memory` does not affect the row-by-row processing model of Python UDFs. Memory allows larger batches but doesn't vectorize the computation.
- C: Correct but not the most complete answer — Spark SQL expression is also valid.

**Exam trap:** The exam often tests "which approach is most appropriate" — the answer is rarely a single technique. The best answer is the one that applies the right tool for the specific situation: SQL functions when expressible, Pandas UDFs when vectorizable but not expressible in SQL, and Python UDFs as a last resort.

---

## Difficulty Ratings

| Q# | Difficulty | Topic |
|---|---|---|
| 1 | Medium | Files in `/Workspace/` are not auto-importable |
| 2 | Critical | `%pip install` is driver-only; UDFs need cluster-level install |
| 3 | Medium | Grouped ops need `applyInPandas`, not a scalar UDF |
| 4 | Medium | Single `databricks.yml`, multiple `targets` |
| 5 | Medium | DBR-bundled package shadowing |
| 6 | Medium | `--force`, not `--overwrite` |
| 7 | Hard | `returnType` must be instantiated (`StringType()`), never the bare class |
| 8 | Medium | `%run` merges scope; proper packages prevent it |
| 9 | Medium | Bundle deploy doesn't validate notebook paths exist |
| 10 | Medium | SQL function > Pandas UDF > Python UDF, in that order of preference |
