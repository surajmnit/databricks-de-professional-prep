# Day 2 — Cheat Sheet: Python Dev, DABs, Dependencies, UDFs

## Python Package Project Structure

```
my_project/                    # Directory: use underscores, not hyphens
├── __init__.py                # Required to mark as package
├── etl/                       # Subpackage
│   ├── __init__.py
│   ├── ingest.py
│   ├── transform.py
│   └── validate.py
├── utils/
│   ├── __init__.py
│   └── logging_config.py
├── setup.py                   # For building wheel
└── requirements.txt
```

**Import paths resolve to:** `from my_project.etl.ingest import read_bronze`

**Key rule:** Files in `/Workspace/` are NOT automatically importable. Must `pip install -e .` or `sys.path.insert(0, path)`.

---

## Declarative Automation Bundles (DABs)

**databricks.yml — single config file, multiple targets, sizing via variables:**

```yaml
bundle:
  name: my-project

variables:
  num_workers:
    default: 2

targets:
  dev:
    default: true
    workspace:
      host: https://dev-workspace.cloud.databricks.com
  prod:
    workspace:
      host: https://prod-workspace.cloud.databricks.com
    variables:
      num_workers: 8

resources:
  jobs:
    my_job:
      name: ${bundle.target}-my-job
      job_clusters:
        - job_cluster_key: main
          new_cluster:
            spark_version: "15.4.x-scala2.12"
            num_workers: ${var.num_workers}
      tasks:
        - task_key: step1
          job_cluster_key: main
          notebook_task:
            notebook_path: ./notebooks/etl.py
```

**Key CLI commands:**
| Command | Action |
|---|---|
| `databricks bundle init` | Initialize new bundle |
| `databricks bundle validate` | Check YAML syntax |
| `databricks bundle deploy dev` | Deploy to dev |
| `databricks bundle deploy prod --force` | Overwrite existing prod resources |
| `databricks bundle run dev job_name` | Trigger a deployed job |
| `databricks bundle diff prod` | Compare prod config changes |

**Exam trap:** `databricks bundle deploy` does NOT validate that referenced notebooks exist. Job will fail at runtime if path is wrong.

**Exam trap:** there is no `clusterless: true` flag and no bare `default_clusters` target field. Serverless compute for a task is simply the *absence* of a `job_cluster_key`/`new_cluster` reference; per-target sizing is done with bundle **variables**, not a made-up cluster block.

---

## Library Installation in Databricks

| Method | Scope | Executors Covered? |
|---|---|---|
| `%pip install <pkg>` | Notebook/driver | No |
| Cluster-scoped library (UI/API/CLI) | All cluster nodes | Yes |
| `dbutils.library.install()` | Notebook (legacy) | No |
| Install from `.whl` on DBFS | Cluster | Yes |
| `requirements.txt` at cluster level | Cluster | Yes |

**Key fact:** `%pip install` only affects the driver. UDFs need cluster-level installation.

---

## Python UDF vs Pandas UDF

| Aspect | Python UDF | Pandas UDF |
|---|---|---|
| Processing | Row-by-row in Python subprocess | Batch (vectorized) via Arrow |
| Serialization | Py4J (pickle-like) | Apache Arrow (zero-copy) |
| Speed | Slow (2–10x slower than SQL) | Fast (near SQL speed) |
| Return type annotation | `returnType=IntegerType()` (instantiated) | `returnType=IntegerType()` (instantiated — **same rule**, not a bare class) |
| Grouped operations | N/A — Spark handles grouping natively | Use `groupBy(...).applyInPandas(func, schema)` |
| Memory | `spark.python.worker.memory` (off-heap) | Uses Arrow batches |

**Rule:** Use Spark SQL functions first. If not possible: Pandas UDF > Python UDF.

**Return-type exam trap (corrected):** `returnType` must always be an **instantiated** `DataType` (e.g., `IntegerType()`, `StringType()`) or a DDL string (e.g., `"int"`, `"string"`) — for *both* `udf()` and `pandas_udf()`. Passing the bare class without parentheses (`IntegerType` instead of `IntegerType()`) raises a `TypeError` in either case; there is no form of this API where the un-instantiated class is correct.

**Grouped Map exam trap:** `PandasUDFType.GROUPED_MAP` still runs but is deprecated since Spark 3.0 — `groupBy(...).applyInPandas(func, schema)` is the current, exam-expected API.

---

## Python UDF Common Failure Patterns

| Symptom | Root Cause | Fix |
|---|---|---|
| Works on driver, fails on executors | Notebook-scoped pip install | Cluster-level install |
| OOM on executor, JVM heap fine | Python worker memory exceeded | Increase `spark.python.worker.memory` |
| Different behavior on driver vs executor | DBR bundled package shadowing | Check `spark.sql.execution.arrow.pyspark.enabled` |
| ModuleNotFoundError in UDF | Package not installed on cluster | Install at cluster level |
| `TypeError` at UDF definition time | `returnType` passed as bare class, not instantiated | Use `IntegerType()`/`StringType()` (with parentheses), not the class itself |

---

## DAB CI/CD Pattern

```yaml
# GitHub Actions
- name: Deploy to staging
  run: databricks bundle deploy staging --no-prompts
  env:
    DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
    DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
```

Bundle name, workspace host, and resource definitions are the three things that vary across environments.

---

## Wheel (.whl) Build and Install

```bash
# Build
cd my_project
python -m pip wheel . -w dist/

# Install on cluster
databricks libraries install --cluster-id <id> --whl ./dist/my_project-1.0.0-py3-none-any.whl

# Install in notebook
%pip install /dbfs/path/to/my_project-1.0.0-py3-none-any.whl
```
