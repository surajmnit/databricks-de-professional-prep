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

**databricks.yml — single config file, multiple targets:**

```yaml
bundle:
  name: my-project
  target: dev
targets:
  dev:
    workspace:
      host: https://dev-workspace.cloud.databricks.com
  prod:
    workspace:
      host: https://prod-workspace.cloud.databricks.com
resources:
  jobs:
    my_job:
      name: ${bundle.target}-my-job
      tasks:
        - task_key: step1
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
| Return type annotation | `returnType=IntegerType()` | `returnType=IntegerType` (class, not instance) |
| GROUPED_MAP needed for groups? | No (Spark handles grouping) | Yes |
| Memory | `spark.python.worker.memory` (off-heap) | Uses Arrow batches |

**Rule:** Use Spark SQL functions first. If not possible: Pandas UDF > Python UDF.

**Pandas UDF return type exam trap:** Use `StringType` (class), NOT `StringType()` (instance).

---

## Python UDF Common Failure Patterns

| Symptom | Root Cause | Fix |
|---|---|---|
| Works on driver, fails on executors | Notebook-scoped pip install | Cluster-level install |
| OOM on executor, JVM heap fine | Python worker memory exceeded | Increase `spark.python.worker.memory` |
| Different behavior on driver vs executor | DBR bundled package shadowing | Check `spark.sql.execution.arrow.pyspark.enabled` |
| ModuleNotFoundError in UDF | Package not installed on cluster | Install at cluster level |
| PyArrow serialization error | Return type incorrectly instantiated | Use `StringType` not `StringType()` |

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
