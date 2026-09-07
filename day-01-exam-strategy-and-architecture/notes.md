# Day 1 — Exam Strategy + Databricks Architecture + Spark Deep-Dive

## Objectives (Exam Guide)

This day covers foundational knowledge that underpins all 10 exam sections. No single exam objective maps to this day, but it addresses the prerequisite knowledge every Professional-level question assumes. Specifically:

- Exam Guide Section 6 (Cost & Performance Optimization, 13%) — Spark architecture, driver/executor behavior, memory, shuffle — all require a solid mental model before you can reason about performance.
- Exam Guide Section 5 (Monitoring and Alerting, 10%) — Spark UI, Query Profile interpretation requires knowing what Jobs/Stages/Tasks are and how they relate.
- Exam Guide Section 9 (Debugging and Deploying, 10%) — troubleshooting OOMs, driver failures, executor errors requires distinguishing driver vs. executor roles.

---

## Part 1 — Exam Strategy

### Exam Format

| Attribute | Detail |
|---|---|
| Questions | 59 scored MCQs |
| Time | 120 minutes |
| Delivery | Online proctored or test center |
| Test aides | None — no API docs, no external references |
| Passing threshold | Databricks does not publish a fixed passing score; estimates range 65–72% based on community reports |

### Time Management Strategy

- **59 questions / 120 minutes = ~2 minutes per question** — this is a hard cap.
- Do NOT spend more than 2.5 minutes on any single question. Flag and move on.
- Easy questions (definition/recall): 30–60 seconds
- Medium questions (application): 60–90 seconds
- Hard/Professional questions (multi-layered scenarios): 90–120 seconds
- Flag any question you second-guess and return at the end only if time allows.

### Question Taxonomy

Most exam questions fall into one of four types:

**Type 1 — Definition/Recall** (Easy, ~20% of exam)
> "Which Spark component is responsible for scheduling tasks on executors?"

**Type 2 — Application/Selection** (Medium, ~40% of exam)
> "Given a skewed join scenario, which optimization approach should be used?"

**Type 3 — Troubleshooting/Diagnosis** (Hard, ~25% of exam)
> "A Spark job has one task running 10x longer than all others. What is the most likely cause and how would you confirm it?"

**Type 4 — Architecture/Design Decision** (Professional, ~15% of exam)
> "A team needs to share live data with an external partner on a different cloud. Which approach minimizes operational overhead while meeting governance requirements?"

**Key insight:** The exam rewards reasoning from first principles, not memorization. A question about a failed merge operation tests your understanding of how Delta transactions work, not a specific syntax command.

### Keyword-Driven Triage

When reading a scenario question, identify the constraint first:

| Keyword(s) in scenario | Likely correct answer cluster |
|---|---|
| "sla", "latency", "processing time", "interval" | Streaming configuration, trigger settings, cluster sizing |
| "slow task", "one task", "imbalanced", "skew" | Data skew, partition imbalance, shuffle issues |
| "OOM", "out of memory", "driver failed" | Memory misconfiguration, driver vs executor |
| "PII", "masking", "row filter", "anonymize" | Unity Catalog security features |
| "live data", "external platform", "share" | Delta Sharing, D2O, open protocol |
| "quarantine", "bad data", "rejected" | Auto Loader quarantine, Lakeflow expectations |
| "governance", "discoverable", "descriptions" | Unity Catalog metadata, lineage |
| "cost", "optimize", "file size", "small files" | Delta optimization, Liquid Clustering, file sizing |

### Certification Trap Patterns

Databricks exam writers repeatedly use these patterns:

1. **"What happens when..." questions about DDL** — Often the answer is "the metastore is updated" even if the data files are untouched. Don't assume file-level operations.
2. **"Which is better, X or Y?" questions** — The correct answer almost always includes a constraint or condition (e.g., "X is better when the dataset is small enough to fit in memory"). Binary "X is always better" answers are almost always wrong.
3. **"Identify the bottleneck" questions** — You must pick the specific bottleneck, not a general solution. If the scenario says "one task takes 10x longer," the answer is "data skew," not "increase partitions."
4. **"What will happen when this code runs?" questions** — These test exact execution semantics. Read the code literally. Don't infer intent.
5. **Terminology trap** — Old names are still present in older study materials but not on the current exam. If you see "APPLY CHANGES," "DLT," or "Asset Bundles," treat them as correct answers but note the rename for future reference (see glossary.md in 00-resources/).

### The Night Before and Morning Of

- Do NOT study new material the night before. Light review of cheat sheets only.
- Read 3–5 previously answered questions you got wrong — re-explain why the correct answer is correct.
- Bring nothing to a test center (not even a watch — they'll provide one).
- For online proctoring: ensure your desk is clear, camera is on, no second screens.

---

## Part 2 — Databricks Platform Architecture

### Platform Layers

The Databricks Data Intelligence Platform operates in two conceptual layers:

**CONTROL PLANE (Managed by Databricks)**
- Web Application (Workspace UI)
- Notebook / DBSQL / Git Folders (formerly Repos)
- Job Scheduler
- Unity Catalog (Governance Layer)
- REST API / CLI

**DATA PLANE (Customer cloud account)**
- AWS/Azure/GCP Storage (S3/ADLS/GCS)
- Compute (All-purpose clusters, Job clusters)
- Delta Lake (data in cloud storage)
- Unity Catalog (metastore + security policies)

**Critical distinction for the exam:** The control plane manages orchestration and metadata, but data never flows through it. Data reads and writes go directly between compute (executors) and cloud storage. This matters for latency, cost, and security architecture questions.

### Unity Catalog Architecture

Unity Catalog replaces the legacy Hive metastore as the unified governance layer:

```
Unity Catalog (metastore)
├── Metastore (root level)
│     ├── Catalog 1
│     │     ├── Schema 1
│     │     │     └── Tables/Views
│     │     └── Schema 2
│     └── Catalog 2
└── External locations (linked)
```

**Key architectural distinction:** Unity Catalog is a workspace-level (or account-level) service, not a cluster-level one. Permissions set in Unity Catalog apply across all clusters that have Unity Catalog enabled. This is a common exam trap — legacy cluster-level ACLs do not apply when UC is enabled.

### Compute Types

| Compute Type | When to Use | Billed As |
|---|---|---|
| All-Purpose Interactive | Ad-hoc exploration, notebooks, DBSQL | DBUs + cloud instance while running |
| Job Cluster | Scheduled Jobs (starts on trigger, terminates after) | DBUs only during job run |
| Serverless (AWS/Azure) | Serverless compute — no cluster management | Higher DBU rate, no instance wait time |
| SQL Warehouse (Serverless or Pro) | DBSQL only | Separate billing from classic compute |

**Exam trap:** Job clusters start faster than all-purpose clusters but still have startup time. For very short jobs (<30 seconds), this startup overhead can dominate. This is why the exam has a question about choosing "job cluster" vs "triggered streaming" — you need to know the trade-off.

### Workspace Organization

```
Workspace
├── Objects:
│   ├── Notebooks (.dbc, exported .py/.sql)
│   ├── Git Folders (formerly Repos) — linked to Git remote
│   ├── Dashboards (DBSQL)
│   ├── Experiments (MLflow)
│   └── Libraries (installed on clusters)
└── Storage:
    ├── DBFS (Databricks File System) — mounted to cloud storage
    └── Unity Catalog manages paths to cloud storage
```

**Exam trap:** DBFS is not a separate storage system — it is a view/mount layer over cloud storage (S3/ADLS/GCS). When a question says "write to DBFS," the underlying data is actually in cloud storage.

---

## Part 3 — Spark Architecture Deep-Dive

### The Application Hierarchy

Spark applications follow this containment hierarchy:

```
Application (one spark-submit / one notebook session)
└── Job(s) (triggered by an action)
     └── Stage(s) (determined by shuffle boundaries)
          └── Task(s) (one per partition, runs on one executor)
```

**Why this matters for the exam:** When you see a Spark UI showing 12 stages for one query, you're seeing 12 shuffle boundaries. Questions about "why did this create multiple stages" are testing your understanding of wide vs. narrow transformations.

### Driver

The Driver is a JVM process that:
1. Runs the SparkContext (the entry point to Spark)
2. Converts user code into a Directed Acyclic Graph (DAG)
3. Schedules tasks onto executors via the Cluster Manager
4. Collects results from executors
5. Hosts the Spark UI (available at port 4040/4041 on the driver)

**In Databricks:** The driver runs on one node of the cluster. For single-node clusters (e.g., Community Edition), the driver and executor share a JVM or run side by side.

**Common exam question about the driver:** What happens if the driver fails?
- Application crashes
- All in-flight tasks are lost
- Spark UI is unavailable
- **Recovery depends on deployment mode:** Cluster mode can recover (supervises the driver), client mode cannot

**Driver OOM scenarios:**
- Collecting large DataFrames (`df.collect()`) — driver pulls all data to driver JVM
- Broadcasting large tables — driver receives broadcast data
- Very large `groupBy` results — output of aggregation flows to driver
- Large broadcast join — if broadcast threshold is exceeded, driver holds metadata

### Executors

Executors are JVM processes (one per worker node by default, but can be multiple per node):
1. Execute tasks assigned by the driver
2. Store results in memory or disk as directed by Spark
3. Report heartbeat and task status back to the driver
4. Each executor has its own memory region: `spark.executor.memory`

**Executor failure behavior:**
- If an executor fails, the driver reschedules failed tasks on other executors
- If `spark.task.maxFailures` is exceeded, the stage/job fails
- By default, Spark retries failed stages (not just tasks) up to `spark.stage.maxAttempts`

**Executor OOM scenarios:**
- Too many partitions with data that doesn't fit in memory per partition
- Large shuffle read (many partitions, each with data)
- Cache bloat (caching more data than executor memory)
- Deeply recursive operations

### Worker vs. Executor (Common Confusion Point)

| Concept | What it actually is |
|---|---|
| **Worker** | A process that manages physical resources on a node. Not Spark-specific. |
| **Executor** | A Spark process that runs tasks and stores data. Spark-specific. |
| **Node** | A single machine (VM or physical) in a cluster |

**Exam trap:** The exam may use "worker node" to mean "the machine that runs executors." Do not confuse this with the YARN concept of a Worker daemon.

### Cluster Manager

Spark supports four cluster managers:

| Cluster Manager | Databricks Support |
|---|---|
| Standalone | Community Edition uses this |
| YARN | Azure HDInsight, legacy deployments |
| Kubernetes | GCP Databricks, advanced configs |
| Mesos | Deprecated |

**In Databricks:** The cluster manager is abstracted away. You choose "AWS," "Azure," or "GCP" and Databricks manages the underlying resource provisioning. The exam tests your understanding of Spark behavior, not which cluster manager is configured.

### Partitioning

Partitions are the fundamental unit of parallelism in Spark:

- **One partition = one task** (in most cases)
- Number of partitions = default parallelism (controlled by `spark.sql.shuffle.partitions`, default 200)
- Each task runs on one executor, processing one partition
- Partition size is not fixed — determined by data volume and distribution

**For read operations:**
- `spark.read.csv(path)` — one partition per input file by default
- `spark.read.parquet(path)` — one partition per file group, respecting `maxPartitionBytes`
- `spark.read.format("avro")` — same as Parquet

**For write operations:**
- Number of output files ≈ number of output partitions
- `df.repartition(n)` — produces exactly n partitions (triggers shuffle)
- `df.coalesce(n)` — reduces to n partitions (no shuffle, only reduces)

**Exam trap:** `coalesce(n)` cannot increase partition count. `repartition(n)` always triggers a shuffle but can go up or down.

### Jobs

A Job is created every time Spark encounters an Action:
- `df.collect()` → one Job
- `df.write.save()` → one Job
- `df.count()` → one Job
- Multiple actions in sequence → multiple Jobs

**Jobs are not nested** — each action creates a new Job independent of prior ones.

**Stage structure:** A Job is broken into one or more Stages. Stages are delimited by shuffle boundaries.

### Stages

| Stage Type | Trigger Condition |
|---|---|
| Shuffle map stage | Precedes a shuffle write |
| Result stage | Final stage, runs on driver |

**Stage boundaries = shuffle operations = expensive.** This is fundamental to performance reasoning.

### Tasks

A Task is the smallest unit of work in Spark:
- One Task processes one Partition
- Tasks run in parallel across all available executors
- Task count = total number of partitions in the current stage

**Exam trap:** "Task" in Spark is not the same as "Task" in a Databricks Job. A Databricks Job task (in the Jobs UI) is a higher-level concept — it can be a notebook run, a JAR run, or a pipeline run. Each Databricks Job task may internally create multiple Spark Jobs/Stages/Tasks.

### Memory Model (Executor-Level)

Executor memory is divided into regions:

```
Executor Memory
├── Execution Memory (~40%)
│   ├── Shuffle sort
│   ├── Hash join
│   └── Internal rows
├── Storage Memory (~60%)
│   ├── Cache (RDD/DataFrame)
│   ├── Broadcast variables
│   └── Internal metadata
├── User Memory (~30% of total)
│   └── User variables, UDFs, data structures
└── System Memory (~10% of total)
    └── JVM overhead
```

**Key exam insight:** Execution memory and Storage memory share a pool (unified memory). However, when Execution is full, it cannot evict from Storage.

**Python UDF memory:** Python UDFs run in a separate Python process, not in the JVM. Their memory does not count against `spark.executor.memory`. Instead, constrained by `spark.python.worker.memory` (default 512MB per worker) and `spark.python.worker.reuse=true`.

### Exam-Specific Memory Patterns

| Scenario | Likely Memory Issue |
|---|---|
| Driver fails mid-job with OOM | Driver collected too much data or broadcast too large a table |
| Some executors OOM, others idle | Data skew — one partition much larger than others |
| Python UDF causes OOM | Python worker memory limit exceeded |
| Query fine on small data, OOM on large | Partition count too low — not enough parallelism |
| Tasks succeed but cluster fills over time | Memory leak — cached data not evicted, GC not reclaiming |

---

## Part 4 — Key Databricks-Specific Concepts

### Databricks Runtime (DBR) Versions

DBR versions are dated. Current stable is DBR 15.x:
- DBR 14.x ships Spark 3.5
- DBR 15.x ships Spark 4.0
- Features like Liquid Clustering require DBR 14.3+

**For the exam:** Use the exam guide's feature names (Liquid Clustering, AUTO CDC, etc.) rather than DBR version numbers.

### Databricks-Specific Spark Properties

| Property | Databricks Default | Common Override |
|---|---|---|
| `spark.sql.shuffle.partitions` | 200 | Lower for small data, higher for large shuffles |
| `spark.sql.adaptive.enabled` | true | May disable for deterministic testing |
| `spark.sql.adaptive.coalescePartitions.enabled` | true | — |
| `spark.sql.files.maxPartitionBytes` | 128MB | Increase for large file reads |
| `spark.executor.memory` | Set by cluster config | Use cluster UI, not spark-defaults.conf |
| `spark.databricks.delta.autoOptimize.enabled` | true | May disable for testing |

**Important:** Properties in `spark-defaults.conf` are cluster-scoped. Jobs can override cluster-level properties at the task level (Task Settings → Advanced Parameters). This is explicitly tested in Section 1.

### Terminology Remappings (Current Exam Guide, July 2026)

| Old Name | Current Official Name |
|---|---|
| Delta Live Tables (DLT) | Lakeflow Spark Declarative Pipelines |
| Databricks Asset Bundles (DABs) | Declarative Automation Bundles |
| APPLY CHANGES | AUTO CDC |
| Databricks Repos | Git Folders |

All four of these are actively tested. The old names are also technically valid on the platform, but the exam guide uses the new names. Use the current names in your reasoning.

---

## Cross-References

- **Day 5 (Spark Execution):** Goes deeper into DAG, lazy evaluation, action vs. transformation, wide vs. narrow.
- **Day 6 (Shuffle):** Detailed explanation of shuffle read/write, partitioner strategies, and spill.
- **Day 7 (Spark Memory):** Detailed GC behavior, memory pressure scenarios, OOM root causes.
- **Day 8 (Spark UI + Query Profile):** Practical diagnosis using the interfaces built on this architecture.
- **Day 15 (Lakeflow Pipelines):** How Spark Structured Streaming integrates with Databricks pipeline semantics.
