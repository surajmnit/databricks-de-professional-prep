# Glossary — Databricks Certified Data Engineer Professional

## Terminology Remappings (July 2026 Exam Guide)

> **These renames are ACTIVE on the current exam. Use the CURRENT names in your reasoning.**

| Old Name (Legacy Material) | Current Official Name (Exam Guide) | Notes |
|---|---|---|
| Delta Live Tables (DLT) | **Lakeflow Spark Declarative Pipelines** | Pipelines feature, now called Lakeflow |
| Databricks Asset Bundles (DABs) | **Declarative Automation Bundles** | YAML-based project deployment |
| APPLY CHANGES | **AUTO CDC** | CDC syntax in Lakeflow pipelines |
| Databricks Repos | **Git Folders** | Git integration in workspace |
| Delta Lake Auto-Optimize | **Delta Auto-Optimize** (name retained) | Feature name mostly unchanged |
| Clone (shallow/deep) | Clone (shallow/deep) (name retained) | Feature name unchanged |

---

## Core Concepts

### Spark Terms

| Term | Definition |
|---|---|
| **DAG** | Directed Acyclic Graph — logical plan of transformations built by the driver |
| **Driver** | JVM process that runs SparkContext, builds DAG, schedules tasks |
| **Executor** | JVM process per node that runs tasks, stores data in memory/disk |
| **Worker** | Physical or VM node; runs executor processes (not a Spark daemon in Databricks) |
| **Job** | Created by an ACTION; contains one or more Stages |
| **Stage** | Set of tasks that can run without a shuffle; delimited by shuffle boundaries |
| **Task** | Smallest unit of work; processes one partition on one executor |
| **Narrow transformation** | Transformation that requires no shuffle (filter, select, withColumn) |
| **Wide transformation** | Transformation requiring a shuffle across partitions (groupBy, join, repartition) |
| **Shuffle** | Data movement between executors; expensive; creates a new Stage boundary |
| **Shuffle read** | Reading shuffle output files from disk/network from other executors |
| **Shuffle write** | Writing intermediate data to disk for other executors to read |
| **Partition** | Logical chunk of data; one Task processes one Partition |
| **Repartition** | Reshuffles data to exactly n partitions (always triggers shuffle) |
| **Coalesce** | Reduces partitions to n (no shuffle); cannot increase partition count |
| **Broadcast join** | Small table sent to all executors; avoids shuffle; controlled by `spark.sql.autoBroadcastJoinThreshold` |
| **AQE** | Adaptive Query Execution; Spark 3.x runtime query plan optimization |
| **Spill** | When executor memory is exceeded; data written to disk |
| **Skew** | Unbalanced data distribution across partitions; one partition much larger than others |
| **Driver OOM** | Driver JVM heap exhausted; typically from `collect()` of large data |
| **Executor OOM** | Executor heap exhausted; from large partitions, cache bloat, Python UDF memory |

### Delta Lake Terms

| Term | Definition |
|---|---|
| **Transaction Log (_delta_log)** | Append-only log of every transaction; defines table state |
| **ACID transactions** | Atomicity, Consistency, Isolation, Durability — guaranteed by transaction log |
| **Data skipping** | MIN/MAX stats in transaction log allow skipping files that don't match predicates |
| **File pruning** | Only relevant files are read based on data skipping and partition pruning |
| **Deletion vectors** | Tracks deleted rows without rewriting data files; improves delete/update/merge performance |
| **Liquid Clustering** | Databricks approach to data organization; replaces partitioning/Z-Ordering for most cases |
| **Z-Ordering** | Sort by column(s) to co-locate related data; improves data skipping for filtered queries |
| **Partitioning** | Directory-based organization by column value; creates separate files per value |
| **Change Data Feed (CDF)** | Delta feature that records row-level change operations (inserts, updates, deletes) |
| **MERGE** | Upsert operation: INSERT + UPDATE + DELETE in one statement |
| **SCD Type 2** | Slowly Changing Dimension pattern; tracks historical changes to dimension data |
| **Clone (shallow)** | Creates table referencing same data files; fast, no data copy |
| **Clone (deep)** | Copies data files; creates independent table |
| **Time travel** | Query historical table state using version number or timestamp |

### Platform Terms

| Term | Definition |
|---|---|
| **Unity Catalog** | Account-level governance: metastore → catalog → schema → table/view |
| **Metastore** | Root of UC hierarchy; scoped to one cloud region per account |
| **Control Plane** | Databricks-managed services: UI, job scheduler, API, UC governance |
| **Data Plane** | Customer cloud account: storage, compute (executors), Delta Lake files |
| **DBFS** | Mount layer over cloud storage (S3/ADLS/GCS); not a separate storage system |
| **All-Purpose Cluster** | Interactive cluster; billed while running (DBU + cloud instance) |
| **Job Cluster** | Ephemeral cluster; starts on job trigger, terminates after; DBU only |
| **Serverless Compute** | No cluster management; higher DBU rate, no instance wait time |
| **DBR** | Databricks Runtime version; ships specific Spark version |
| **Databricks CLI** | Command-line tool for managing workspaces, jobs, DABs |
| **REST API** | Programmatic access to Databricks workspace, jobs, pipelines |
| **Declarative Automation Bundles** | YAML-defined project; deploys notebooks, jobs, clusters, pipelines |
| **Git Folders** | Workspace objects linked to a Git repository; enables CI/CD |

### Pipeline / Streaming Terms

| Term | Definition |
|---|---|
| **Lakeflow Spark Declarative Pipelines** | Pipelines feature (formerly DLT); YAML-defined ETL pipelines with built-in observability |
| **Auto Loader** | Ingestion tool for cloud storage; auto-detects new files; schema inference/evolution |
| **Quarantine** | Auto Loader pattern for bad records; isolated to `_autoloader/_checkpoint/quarantine/` |
| **AUTO CDC** | CDC syntax in Lakeflow pipelines (formerly APPLY CHANGES); row-level change tracking |
| **Structured Streaming** | Spark's streaming API; micro-batch or continuous processing |
| **Microbatch** | Small batch of streaming data processed together in one trigger interval |
| **Trigger interval** | Frequency with which Structured Streaming processes accumulated data |
| **Watermark** | Event-time threshold for handling late-arriving data; drops data older than watermark |
| **Streaming Table** | Live-updating table in Lakeflow; incremental materialization of streaming source |
| **Materialized View** | Pre-computed query result; refreshed on trigger or schedule |
| **Expectations** | Lakeflow feature for data quality rules with quarantine on failure |
| **Control Flow** | Pipeline operators: if/else, for/each, in Lakeflow declarative pipelines |

### Security / Governance Terms

| Term | Definition |
|---|---|
| **ACL** | Access Control List; defines who can access what at which level |
| **Least privilege** | Principle: grant minimum permissions needed to perform an action |
| **Row filter** | UC feature: filters rows returned to user based on their identity/group membership |
| **Column mask** | UC feature: applies a function to columns to mask sensitive data (e.g., email → substring) |
| **Anonymization** | Process of removing PII; replaced with non-reversible tokens/hashes |
| **Pseudonymization** | Replacing PII with artificial identifiers; reversible with a key |
| **Hashing** | One-way function: PII → fixed-length hash; cannot be reversed without salt |
| **Tokenization** | Replacing PII with tokens; tokens map back to original via lookup table |
| **Suppression** | Removing PII fields entirely from output |
| **Generalization** | Reducing precision: full date → month/year; exact address → city |
| **Delta Sharing** | Protocol for sharing Delta tables with external platforms (D2D or D2O) |
| **D2D Sharing** | Databricks-to-Databricks sharing; uses Unity Catalog recipient definitions |
| **D2O Sharing** | Databricks-to-other-platform sharing; uses open sharing protocol (REST-based) |
| **Lakehouse Federation** | Query external databases (MySQL, PG, Snowflake, etc.) from Databricks via Unity Catalog |

### Monitoring Terms

| Term | Definition |
|---|---|
| **Spark UI** | Web UI on driver (port 4040); shows Jobs, Stages, Tasks, Storage, Environment |
| **Query Profile** | Databricks UI for detailed query execution metrics; shows operators and timing |
| **System Tables** | Queryable audit, billing, and usage tables in Unity Catalog system schema |
| **Event Logs** | Lakeflow pipeline logs (JSON); detailed per-batch metrics, errors, data quality |
| **SQL Alerts** | Databricks feature: trigger notification when a SQL query returns a threshold result |

---

## Exam-Specific Naming Conventions

- Write **"Git Folders"**, not "Repos"
- Write **"Declarative Automation Bundles"**, not "DABs" or "Asset Bundles"
- Write **"Lakeflow Spark Declarative Pipelines"**, not "DLT"
- Write **"AUTO CDC"**, not "APPLY CHANGES"
- Write **"Streaming Tables"** and **"Materialized Views"** for Lakeflow concepts

---

## Cross-Reference: Glossary Updates

> Append new terms here as you encounter them during the program.
> Format: `Term — Brief definition — Day introduced`


---

## Day 2 Updates

| Term | Definition | Day |
|---|---|---|
| Declarative Automation Bundles (DABs) | YAML-based project definitions for reproducible Databricks resource deployment | 2 |
| databricks.yml | Root configuration file for a DAB; defines resources and targets | 2 |
| Python UDF | Row-by-row function in Python subprocess; slow, use Pandas UDF instead | 2 |
| Pandas UDF | Vectorized batch function using Apache Arrow; near-SQL performance | 2 |
| GROUPED_MAP Pandas UDF | Pandas UDF type for per-group operations | 2 |
| Wheel (.whl) | Pre-built Python package distribution | 2 |
| %pip install | Notebook-scoped install; driver only, not executors | 2 |
| Cluster-scoped library | Library installed on all cluster nodes; required for UDF access | 2 |
| DAB --force flag | Overwrites existing resources during databricks bundle deploy | 2 |
| Py4J serialization | Row-by-row serialization used by Python UDFs; slower than Arrow | 2 |


---

## Day 3 Updates

| Term | Definition | Day |
|---|---|---|
| Window function | SQL function over row set via OVER clause; does not collapse rows | 3 |
| ROWS vs RANGE | ROWS=physical row count; RANGE=logical value grouping; differ with duplicates | 3 |
| LEFT SEMI join | Left rows where key exists in right; equivalent to IN subquery | 3 |
| LEFT ANTI join | Left rows where key does NOT exist in right; equivalent to NOT IN | 3 |
| Broadcast join | Small table sent to all executors; avoids shuffle | 3 |
| GROUPING SETS | Multiple aggregation levels in one query | 3 |
| ROLLUP | Hierarchical subtotals: (a,b) > (a) > grand total | 3 |
| CUBE | All combinations: (a,b) > (a) > (b) > grand total | 3 |
| PIVOT | Rotate rows to columns | 3 |
| UNPIVOT | Rotate columns to rows (LATERAL VIEW EXPLODE MAP) | 3 |
| DataFrame.transform | Chainable transformation method; each function independently testable | 3 |
| assertDataFrameEqual | Spark built-in order-independent DataFrame comparison (checkRowOrder=False) | 3 |
| assertSchemaEqual | Spark built-in schema comparison | 3 |
| Salted join | Handle skewed joins by replicating rows with salt keys | 3 |
| checkRowOrder | Parameter for assertDataFrameEqual; True=order-sensitive, False=order-independent | 3 |


---

## Day 4 Updates

| Term | Definition | Day |
|---|---|---|
| SparkContext | Entry point to Spark; runs on driver; builds DAG; schedules tasks | 4 |
| Stage | Set of tasks with no shuffle boundary between them | 4 |
| Task | Smallest unit of work; one per partition | 4 |
| Shuffle boundary | Network data movement between stages; creates new Stage | 4 |
| Narrow transformation | No shuffle (filter, withColumn, select) | 4 |
| Wide transformation | Requires shuffle (groupBy, join, repartition, sort, distinct) | 4 |
| Spark UI | Driver-hosted web UI at driver:4040; shows Jobs/Stages/Tasks | 4 |
| spark.sql.shuffle.partitions | Default partition count for shuffle operations (200) | 4 |
| spark.executor.heartbeatInterval | Frequency of executor heartbeats to driver (default 10s) | 4 |
| spark.stage.maxAttempts | Max retries per stage (default 4) | 4 |
| spark.driver.maxResultSize | Max size of collect() result at driver (default 1GB) | 4 |
| Data skew | Uneven partition distribution; one partition dominates | 4 |
| Dynamic allocation | Auto add/remove executors based on workload | 4 |
