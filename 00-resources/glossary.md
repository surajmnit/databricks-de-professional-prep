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

## Day 1 Updates

| Term | Definition | Day |
|---|---|---|
| SparkContext | Entry point to Spark; runs on driver; builds DAG; schedules tasks | 1 |
| Stage | Set of tasks with no shuffle boundary between them | 1 |
| Task | Smallest unit of work; one per partition | 1 |
| Shuffle boundary | Network data movement between stages; creates new Stage | 1 |
| Narrow transformation | No shuffle (filter, withColumn, select) | 1 |
| Wide transformation | Requires shuffle (groupBy, join, repartition, sort, distinct) | 1 |
| Spark UI | Driver-hosted web UI at driver:4040; shows Jobs/Stages/Tasks | 1 |
| spark.sql.shuffle.partitions | Default partition count for shuffle operations (200) | 1 |
| spark.executor.heartbeatInterval | Frequency of executor heartbeats to driver (default 10s) | 1 |
| spark.stage.maxAttempts | Max retries per stage (default 4) | 1 |
| spark.driver.maxResultSize | Max size of collect() result at driver (default 1GB) | 1 |
| Data skew | Uneven partition distribution; one partition dominates | 1 |
| Dynamic allocation | Auto add/remove executors based on workload | 1 |
| Cluster Manager | Resource allocator (Standalone, YARN, K8s, Mesos) for driver/executors | 1 |
| DBR | Databricks Runtime version; ships specific Spark version | 1 |
| Control Plane | Databricks-managed services: UI, job scheduler, API, UC governance | 1 |
| Data Plane | Customer cloud account: storage, compute (executors), Delta Lake files | 1 |
| DBFS | Mount layer over cloud storage (S3/ADLS/GCS); not a separate storage system | 1 |
| All-Purpose Cluster | Interactive cluster; billed while running (DBU + cloud instance) | 1 |
| Job Cluster | Ephemeral cluster; starts on job trigger, terminates after; DBU only | 1 |
| Serverless Compute | No cluster management; higher DBU rate, no instance wait time | 1 |
| Managed table | UC controls storage location and full lifecycle; DROP deletes files | 1 |
| External table | User specifies path; DROP only removes metastore reference | 1 |
| Whole-stage codegen | Collapses operators into one generated function; prefix | 1 |
| Photon | Databricks vectorized engine; default on newer DBR | 1 |
| Databricks SQL (DBSQL) | Serverless SQL warehouse; billed by DBU per query | 1 |

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


---

## Day 5 Updates

| Term | Definition | Day |
|---|---|---|
| Lazy evaluation | Transformations recorded but not executed until Action | 5 |
| Action | Operation that triggers Spark execution (collect, count, write, etc.) | 5 |
| Transformation | Operation that builds the DAG (filter, groupBy, etc.) | 5 |
| Catalyst Optimizer | Spark query planning engine; transforms logical to physical plan | 5 |
| Physical plan | How Spark will execute the query (operators, order) | 5 |
| Exchange | Shuffle operator in physical plan = new Stage boundary | 5 |
| Whole-stage codegen | Collapses operators into one generated function;  prefix | 5 |
| BroadcastExchange | Shuffle operator for small table broadcast | 5 |
| SortMergeJoin | Default join strategy for large tables | 5 |
| Column pruning | Catalyst removes unused columns from read | 5 |
| Predicate pushdown | Catalyst moves filters closer to data source | 5 |
| Filter pushdown | Filter conditions applied at scan time, not post-scan | 5 |
| HashAggregate | Hash-based aggregation (used for groupBy when applicable) | 5 |
| SortAggregate | Sort-based aggregation (used when hash not applicable) | 5 |


---

## Day 6 Updates

| Term | Definition | Day |
|---|---|---|
| Shuffle | Data movement between executors; most expensive Spark operation | 6 |
| Shuffle write | Map stage output written to local disk before reduce stage | 6 |
| Shuffle read | Reduce stage reads shuffle output from map task files | 6 |
| Exchange | Physical plan operator representing shuffle boundary | 6 |
| HashPartitioner | Default partitioner for groupBy; keys distributed by hash | 6 |
| RangePartitioner | Used for sort; distributes by range boundaries | 6 |
| Shuffle spill | Data written to disk because executor memory insufficient for sort | 6 |
| Local read | Reduce task reads shuffle data from same executor (no network) | 6 |
| Remote read | Reduce task reads shuffle data from other executors (network cost) | 6 |
| BroadcastHashJoin | Join using broadcast table; no shuffle on large table | 6 |
| SortMergeJoin | Default join for large tables; shuffle on both sides | 6 |
| AQE skew join | Auto-splits skewed partitions into smaller sub-partitions | 6 |
| autoBroadcastJoinThreshold | Max table size for auto-broadcast (default 10MB) | 6 |
| Salted join | Technique to handle skew: replicate small table rows by salt key | 6 |
| Shuffle partition count | spark.sql.shuffle.partitions (default 200); controls reduce parallelism | 6 |


---

## Day 8 Updates

| Term | Definition | Day |
|---|---|---|
| Query Profile | DBSQL UI for detailed SQL query execution metrics; shows operators and timing | 8 |
| Spark UI | Driver-hosted web UI at driver:4040; shows Jobs/Stages/Tasks/Storage/Environment | 8 |
| AQE | Adaptive Query Execution; re-optimizes physical plan mid-query using real statistics | 8 |
| Coalesce shuffle partitions | AQE feature to merge small partitions post-shuffle for better parallelism | 8 |
| Skew join optimization | AQE auto-splits skewed partitions into smaller sub-tasks processed in parallel | 8 |
| Dynamic join switching | AQE converts SortMergeJoin to BroadcastHashJoin mid-query based on actual stats | 8 |
| Data skipping | Reading fewer files by comparing query filters against file min/max stats | 8 |
| Bad data skipping | High scanned-vs-pruned ratio; caused by poor clustering or missing statistics | 8 |
| CAN MONITOR | Warehouse-level permission to view query profiles; distinct from UC data grants | 8 |
| Spill | Data written to disk when executor memory insufficient | 8 |
| GC Time % | Garbage collection time; high % indicates memory pressure | 8 |
| Max vs Median task duration | Ratio >5x with skewed shuffle read = data skew | 8 |
| Verbose mode | Query Profile setting to show every operator and additional metrics | 8 |
| Automated insights | Query Profile auto-surfaces bottlenecks (skew, spill, inefficient join) | 8 |


---

## Day 9 Updates

| Term | Definition | Day |
|---|---|---|
| Transaction Log (_delta_log) | Append-only log of every transaction; defines table state | 9 |
| ACID transactions | Atomicity, Consistency, Isolation, Durability — guaranteed by transaction log | 9 |
| Optimistic concurrency | Delta uses version-number retry, not distributed locking | 9 |
| Checkpoint | Parquet consolidation every 10 commits; speeds up table state reconstruction | 9 |
| add action | Transaction log action registering a new data file | 9 |
| remove action | Transaction log action tombstoning a file (logical delete) | 9 |
| metadata action | Transaction log action recording schema, partition columns, properties | 9 |
| protocol action | Transaction log action specifying min reader/writer version | 9 |
| commitInfo | Transaction log action with operation metadata | 9 |
| txn action | Transaction log idempotency marker for streaming writes | 9 |
| Schema enforcement | Delta rejects writes with mismatched schema by default | 9 |
| Schema evolution | mergeSchema for additive changes; overwriteSchema for full replacement | 9 |
| Time travel | Query historical table state using VERSION AS OF or TIMESTAMP AS OF | 9 |
| RESTORE TABLE | Recover a table to a previous version | 9 |
| VACUUM | Physically removes unreferenced files past retention window | 9 |
| Predictive Optimization | UC managed table automation: OPTIMIZE, VACUUM, ANALYZE via serverless compute | 9 |
| Predictiv Optimization exclusions | Does not run on external tables or Delta Sharing recipient tables | 9 |
| Small-file problem | Too many small files from over-partitioning or high-cardinality partition column | 9 |
| Low-cardinality partition | Partition column with few distinct values; coarse-grained filtering | 9 |
| Z-Ordering | Sort by column(s) within files to co-locate related data; manual, periodic | 9 |
| Data skipping via Z-Order | Z-Ordered files enable better min/max stats for filtered columns | 9 |


---

## Day 10 Updates

| Term | Definition | Day |
|---|---|---|
| MERGE INTO | Upsert: INSERT + UPDATE + DELETE in one statement | 10 |
| WHEN MATCHED | Fires when target row matches source row; UPDATE or DELETE | 10 |
| WHEN NOT MATCHED | Fires when source row has no target match; INSERT | 10 |
| WHEN NOT MATCHED BY SOURCE | Fires when target row has no source match; handle disappeared rows | 10 |
| Multiple match error | MERGE fails if ON condition matches multiple source rows to one target row | 10 |
| SCD Type 1 | Overwrite in place; no history preserved | 10 |
| SCD Type 2 | Preserve full history with effective_date/end_date validity windows | 10 |
| CDC | Change Data Capture; capturing row-level changes from source | 10 |
| foreachBatch | Structured Streaming pattern for custom microbatch sinks | 10 |
| Change Data Feed (CDF) | Delta feature recording row-level change operations | 10 |
| enableChangeDataFeed | Table property to enable CDF tracking | 10 |
| readChangeFeed | Read CDF changes via spark.readStream or spark.read | 10 |
| _change_type | CDF column: insert, update_preimage, update_postimage, delete | 10 |
| _commit_version | CDF column: Delta table version the change belongs to | 10 |
| _commit_timestamp | CDF column: when the commit happened | 10 |
| UPDATE = two CDF rows | Each UPDATE produces update_preimage + update_postimage | 10 |
| CDF no backfill | CDF only captures changes after it is enabled | 10 |
| CDF + streaming | CDF stream returns snapshot on first start, then incremental changes | 10 |


---

## Day 11 Updates

| Term | Definition | Day |
|---|---|---|
| Deletion vectors | Merge-on-read soft-delete tracking; avoids full file rewrite on DELETE/UPDATE/MERGE | 11 |
| Copy-on-write | Traditional model: rewriting entire file for any small change | 11 |
| Merge-on-read | New model with deletion vectors: mark rows, apply at read time | 11 |
| Protocol upgrade | Enabling deletion vectors upgrades table protocol; older clients may lose access | 11 |
| REORG TABLE APPLY (PURGE) | Physically rewrites files removing rows marked by deletion vectors | 11 |
| Row-level concurrency | DBR 14.2+ improvement: two writers touching different rows in same file can both succeed | 11 |
| Liquid Clustering | Modern data layout replacing partitioning and Z-Order; incremental, redefinable | 11 |
| CLUSTER BY | SQL syntax for Liquid Clustering keys | 11 |
| CLUSTER BY AUTO | Databricks-managed clustering keys; DBR 15.4 LTS+, UC managed tables | 11 |
| OPTIMIZE | Command to compact files and trigger incremental Liquid Clustering | 11 |
| Clustering key redefinition | Liquid Clustering allows changing keys without full table rewrite | 11 |
| delta.dataSkippingNumIndexedCols | Default 32 columns have stats collected; adjust if key column is outside this | 11 |
| delta.dataSkippingStatsColumns | Explicitly name which columns collect statistics | 11 |
| File pruning | Skip individual files via transaction-log min/max stats; works without partitioning | 11 |
| Partition pruning | Skip entire directories based on partition column filter | 11 |
| ANALYZE TABLE | Collect table statistics for query planning | 11 |
| Photon predictive I/O | Uses deletion vectors to accelerate UPDATE operations | 11 |


---

## Day 18 Updates

| Term | Definition | Day |
|---|---|---|
| Unity Catalog | Account-level governance: metastore → catalog → schema → table/view | 18 |
| Metastore | Root of UC hierarchy; scoped to one cloud region per account | 18 |
| Metastore admin | Highest privilege; can grant any permission, manage all catalogs | 18 |
| Catalog | Top-level namespace in UC | 18 |
| Schema | Namespace within catalog containing tables/views | 18 |
| Managed table | UC owns storage path, lifecycle, and files | 18 |
| External table | User-managed path; UC tracks metadata only | 18 |
| GRANT | Give permission to a principal | 18 |
| REVOKE | Remove permission from a principal | 18 |
| USE CATALOG | Required traversal permission to access a catalog | 18 |
| USE SCHEMA | Required traversal permission to access a schema | 18 |
| SELECT | Permission to read data from a table/view | 18 |
| MODIFY | Permission to write/delete on tables | 18 |
| ALL PRIVILEGES | Shorthand for all applicable privileges on an object | 18 |
| OWNERSHIP | Exclusive; one principal owns each object; can grant to others | 18 |
| CREATE | Permission to create objects within a schema/catalog | 18 |
| BUILTIN | Default roles (data engineer, analyst, etc.) pre-seeded in UC | 18 |
| Service Principal | Machine identity for automated tooling; can hold grants | 18 |
| IF EXISTS / IF NOT EXISTS | Safe DDL: suppress errors for missing/existing objects | 18 |
| Permission inheritance | Child objects inherit parent permissions by default | 18 |
| Privilege amplification | Principals with higher permission automatically get lower on child objects | 18 |
| Securable object | Any UC object that can have permissions: catalog, schema, table, view, function | 18 |
| updatePermissions | Audit log event when ACLs are changed | 18 |
| access.audit | System table for all audit events | 18 |


---

## Day 7 Updates

| Term | Definition | Day |
|---|---|---|
| Execution Memory | Portion of Spark memory for shuffle sort, hash join, aggregation buffers | 7 |
| Storage Memory | Portion of Spark memory for cached DataFrames and broadcast variables | 7 |
| Unified Memory | Spark 1.6+ model where execution and storage share a pool; execution cannot evict storage | 7 |
| Shuffle Spill | Data written to disk when execution memory is insufficient | 7 |
| Python Worker Process | Separate subprocess for Python UDFs; memory from spark.python.worker.memory | 7 |
| MEMORY_AND_DISK | Cache level: memory first, spill to disk if needed | 7 |
| MEMORY_ONLY | Cache level: memory only, recompute if evicted | 7 |
| MEMORY_ONLY_SER | Cache level: serialized in memory, less memory but more CPU | 7 |
| LRU Eviction | Storage memory evicts least recently used cached data when full | 7 |
| GC Pressure | Excessive garbage collection pauses from too many short-lived objects | 7 |
| spark.memory.fraction | Fraction of executor heap for Spark (default 0.6) | 7 |
| spark.python.worker.memory | Per-worker Python subprocess heap (default 512MB) | 7 |
| spark.driver.maxResultSize | Max size for collect() results at driver (default 1GB) | 7 |
| SparkStorageLevel | Enum controlling cache persistence: MEMORY_ONLY, MEMORY_AND_DISK, etc. | 7 |

---

## Day 7 Updates

| Term | Definition | Day |
|---|---|---|
| Unified memory | Execution and storage share a pool; execution can evict storage | 7 |
| Execution memory | Memory for shuffles, sorts, joins, hash aggregation | 7 |
| Storage memory | Memory for cache, broadcasts | 7 |
| Shuffle spill | Data written to disk because executor memory insufficient for sort/agg | 7 |
| Python worker memory | Off-heap memory for Python UDF subprocess (spark.python.worker.memory) | 7 |
| spark.driver.maxResultSize | Max size of collect() result at driver (default 1GB) | 7 |
| spark.memory.fraction | Fraction of executor heap for execution+storage (0.6) | 7 |
| spark.memory.storageFraction | Fraction of memory fraction for storage (0.5) | 7 |
| GC pressure | Frequent GC pauses affecting executor performance | 7 |
| Arrow | Columnar format for JVM-to-Python data transfer; used by Pandas UDFs | 7 |
| Cache bloat | Executor OOM from cached data filling storage memory, starving execution | 7 |
| unpersist() | Explicitly frees cached DataFrame memory | 7 |


---

## Day 20 Updates

| Term | Definition | Day |
|---|---|---|
| Delta Sharing | Open protocol for sharing live Delta tables with any platform | 20 |
| D2D Sharing | Databricks-to-Databricks sharing via Unity Catalog | 20 |
| D2O Sharing | Databricks-to-Other sharing via open REST protocol | 20 |
| OPEN Recipient | Delta Sharing recipient for non-Databricks platforms | 20 |
| Account Recipient | Delta Sharing recipient for Databricks workspaces | 20 |
| Lakehouse Federation | Query external databases from Databricks via Unity Catalog | 20 |
| External Connection | Unity Catalog object with credentials to external DB | 20 |
| Federated Query | Query executed on source DB; only results returned | 20 |


---

## Day 19 Updates

| Term | Definition | Day |
|---|---|---|
| Row filter | UC feature: scalar function filtering rows based on caller identity/group | 19 |
| Column mask | UC feature: scalar function masking column values (e.g., SSN → ***-**-****) | 19 |
| is_account_group_member | UC function to check caller's group membership | 19 |
| DROP ROW FILTER | Must drop filter from table before dropping underlying function | 19 |
| DROP MASK | Must drop mask from table before dropping underlying function | 19 |
| Access mode | Compute security mode: Standard (Shared) or Dedicated (Single User) | 19 |
| Standard access mode | Shared compute; row filters/masks enforced via UDFs (DBR 12.2+) | 19 |
| Dedicated access mode | Single-user compute; enforced natively (DBR 15.4+) | 19 |
| Hashing | One-way function: PII → hash; irreversible without salt | 19 |
| sha2(col, 256) | Databricks SHA-256 hashing; deterministic | 19 |
| Tokenization | Reversible replacement; uses encryption key (from secret scope) | 19 |
| aes_encrypt / aes_decrypt | Databricks tokenization using AES encryption | 19 |
| Suppression | Remove PII entirely; column mask returning NULL or constant | 19 |
| Generalization | Reduce precision while keeping analytical value (e.g., date → year) | 19 |
| GDPR purge lifecycle | DELETE → REORG TABLE APPLY (PURGE) → VACUUM | 19 |
| DELETE alone insufficient | With deletion vectors, only marks rows; doesn't physically remove | 19 |
| REORG TABLE APPLY (PURGE) | Physically rewrites files removing soft-deleted rows | 19 |
| time travel compliance gap | Deleted data queryable via time travel until VACUUM runs past retention | 19 |
| delta.deletedFileRetentionDuration | How long deleted files are retained for time travel (default 7 days) | 19 |
| VACUUM RETAIN 0 HOURS | Emergency compliance: removes all history, breaks time travel | 19 |
| Purge propagation | Must delete in bronze first, then propagate to silver/gold via CDF | 19 |
| Non-Delta upstream sources | GDPR applies to Kafka topics, raw files in cloud storage too | 19 |
| Masking ≠ deletion | Masking does not satisfy GDPR "right to be forgotten" | 19 |


---

## Day 21 Updates

| Term | Definition | Day |
|---|---|---|
| System tables | Databricks-managed Delta tables in system catalog; SQL-queryable observability | 21 |
| system.billing.usage | Row per billable DBU; SKU, workspace, cluster/job/pipeline/warehouse metadata | 21 |
| system.billing.list_prices | Historical SKU pricing; join with usage to compute $ cost | 21 |
| system.access.audit | All audit events: logins, permission changes, resource creation/deletion | 21 |
| system.access.table_lineage | Table-level read/write lineage | 21 |
| system.access.column_lineage | Column-level lineage; does not capture literal values | 21 |
| system.compute.clusters | SCD2 history of every cluster configuration | 21 |
| system.compute.node_types | Static reference: available node types + hardware specs | 21 |
| system.compute.node_timeline | Minute-by-minute CPU/memory per node; 90-day retention | 21 |
| system.lakeflow.jobs | SCD2 history of job configurations | 21 |
| system.lakeflow.job_run_timeline | Start/end/status of every job run | 21 |
| system.lakeflow.job_task_run_timeline | Per-task timeline within a job run + compute IDs | 21 |
| system.lakeflow.pipelines | SCD2 history of pipeline configurations | 21 |
| system.lakeflow.pipeline_update_timeline | Per-update timeline with compute used | 21 |
| system.query.history | Every SQL statement on SQL warehouse; text, duration, status | 21 |
| SCD2 in system tables | Change history stored as new rows; requires QUALIFY rn=1 for current state | 21 |
| statement_id | Join key linking query history to lineage tables and Query Profile | 21 |
| SCD2 query pattern | ROW_NUMBER() OVER (PARTITION BY ... ORDER BY change_time DESC) QUALIFY rn=1 | 21 |
| SCD2 365-day retention | Most system tables retain 365 days; lineage has 1-year window | 21 |
| Lakeflow event log | Structured log per pipeline: audit, data quality, progress, lineage | 21 |
| event_log() | Function to query pipeline event log | 21 |
| Pipeline owner | Only owner can query event_log() directly | 21 |
| event_type | Event category: flow_progress, update_progress, dataset_definition | 21 |
| maturity_level | Event schema stability: STABLE, EVOLVING, DEPRECATED | 21 |
| Pagination | REST APIs return has_more/next_page_token; must loop to get all records | 21 |


---

## Day 22 Updates

| Term | Definition | Day |
|---|---|---|
| SQL Alert | Periodically runs a saved query; sends notification when condition is met | 22 |
| Alert condition | Evaluates only first row; column must be numeric or boolean | 22 |
| Pre-aggregate | Multi-row alert queries must collapse to single row | 22 |
| Empty result state | Defines state when query returns zero rows | 22 |
| CASE WHEN COUNT(*) = 0 | Pattern to detect missing data reliably | 22 |
| Notify on OK | Alert also fires when condition returns to OK after being triggered | 22 |
| Notification Destination | Workspace-level object: email, Slack, Teams, PagerDuty, webhook | 22 |
| Admin-only destinations | Only workspace admins can create/update/delete destinations | 22 |
| Job notifications | email_notifications and webhook_notifications at job/task level | 22 |
| on_start | Notification fires when job run starts | 22 |
| on_success | Notification fires when job run completes successfully | 22 |
| on_failure | Notification fires when job run fails (FAILED, INTERNAL_ERROR, TIMED_OUT) | 22 |
| on_duration_warning_threshold_exceeded | Fires only when health rule is defined | 22 |
| on_streaming_backlog_exceeded | Fires when streaming backlog exceeds threshold (Public Preview) | 22 |
| health.rules | Required config for duration/backlog notifications; not optional | 22 |
| RUN_DURATION_SECONDS | Health metric for duration warning | 22 |
| STREAMING_BACKLOG_* | Health metrics: BYTES, RECORDS, SECONDS, FILES | 22 |
| 10-minute rolling average | Streaming backlog alerts use average, not instantaneous spike | 22 |
| 30-minute resend | Sustained backlog alerts resend every 30 minutes | 22 |
| Max 3 destinations | Per event type limit for job notifications | 22 |
| notification_settings | Suppress notifications for skipped/canceled runs | 22 |
| Task-level vs job-level | Skipped/canceled filtering must be set at both levels | 22 |
| Absence-based alert | Alert when job hasnt run successfully in N hours; use SQL Alert on system tables | 22 |
| Legacy alerts | Older separate-steps alert experience; still exists side-by-side | 22 |
| Unified alert editor | Current Databricks alert setup | 22 |
