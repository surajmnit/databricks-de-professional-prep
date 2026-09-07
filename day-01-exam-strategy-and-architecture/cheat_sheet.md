# Day 1 — Cheat Sheet: Exam Strategy + Architecture + Spark Basics

## Exam Time Management

| Question Type | Time per Question | % of Exam |
|---|---|---|
| Definition/Recall | 30–60 sec | ~20% |
| Application/Selection | 60–90 sec | ~40% |
| Troubleshooting/Diagnosis | 90–120 sec | ~25% |
| Architecture/Design | 90–120 sec | ~15% |

**Total: 59 questions in 120 minutes = ~2 min/question max.** Flag anything >2.5 min and move on.

---

## Keyword Triage

| Keyword(s) | Likely Answer Cluster |
|---|---|
| sla, latency, processing time, interval | Trigger settings, streaming config |
| slow task, one task, imbalanced, skew | Data skew, partition imbalance |
| OOM, out of memory, driver failed | Memory config, driver vs executor |
| PII, masking, row filter, anonymize | Unity Catalog security |
| live data, external platform, share | Delta Sharing, D2O |
| quarantine, bad data, rejected | Auto Loader quarantine, Lakeflow |
| cost, optimize, file size, small files | Delta optimization, Liquid Clustering |
| governance, discoverable, descriptions | Unity Catalog metadata |

---

## Spark Application Hierarchy

```
Application
└── Job (triggered by ACTION)
     └── Stage (shuffle boundary)
          └── Task (one per partition)
```

**Wide transformation → new Stage boundary** (groupBy, join, distinct, repartition, etc.)
**Narrow transformation → same Stage** (filter, withColumn, select, etc.)

---

## Driver vs Executor OOM

| What Failed | Most Likely Cause |
|---|---|
| **Driver** | `collect()` of large result; large broadcast; large `groupBy` output |
| **Executor** | Partition too large; cache bloat; Python UDF memory; shuffle read overflow |
| **One executor (others idle)** | Data skew — one partition much larger than others |
| **All executors at limit** | Partition count too low for data volume |

---

## Repartition vs Coalesce

| Operation | Shuffle? | Can Increase? | Can Decrease? |
|---|---|---|---|
| `repartition(n)` | Yes (always) | Yes | Yes |
| `coalesce(n)` | No | No | Yes (to n) |

`coalesce(1000)` on 10-partition DataFrame → returns 10 (unchanged, not an error)

---

## Spark Memory Regions (Executor)

| Region | Purpose | Config |
|---|---|---|
| Execution Memory | Shuffle sort, hash join, internal rows | ~40% of heap |
| Storage Memory | Cache, broadcasts | ~60% of heap (shared with execution) |
| User Memory | UDF variables, Python data | ~30% of total |
| System Memory | JVM overhead | ~10% of total |

Python UDF memory = off-heap (`spark.python.worker.memory`, default 512MB/worker), NOT in executor heap.

---

## Platform Architecture — Key Points

| Concept | Key Fact |
|---|---|
| Control Plane | Manages metadata, jobs, UI — NOT data path |
| Data Plane | Executors read/write directly to cloud storage |
| DBFS | Mount layer over S3/ADLS/GCS — NOT separate storage |
| Unity Catalog | Account-level, not cluster-level; overrides cluster ACLs when enabled |
| Job Cluster | Starts on trigger, terminates after — billed only during run |
| All-Purpose Cluster | Billed while running (DBU + cloud instance) |

---

## Terminology Remappings (July 2026 Exam Guide)

| Old Name | Current Official Name |
|---|---|
| Delta Live Tables (DLT) | Lakeflow Spark Declarative Pipelines |
| Databricks Asset Bundles (DABs) | Declarative Automation Bundles |
| APPLY CHANGES | AUTO CDC |
| Databricks Repos | Git Folders |

---

## Certification Trap Patterns

1. DDL questions → often "metastore is updated," not "files are rewritten"
2. "Which is better?" → answer is conditional, not absolute
3. "One slow task" → data skew, not "increase partitions" (the root cause, not the solution)
4. "What happens when code runs?" → read literally, don't infer intent
5. UC enabled + legacy ACLs → UC wins; cluster ACLs are ignored

---

## Databricks Job Tasks vs Spark Tasks

| Concept | Scope | Tracked Where |
|---|---|---|
| **Databricks Job Task** | High-level orchestration unit (notebook, JAR, pipeline) | Jobs UI |
| **Spark Task** | Lowest-level unit — one per partition per stage | Spark UI |

One Databricks Job Task can contain multiple Spark Jobs/Stages/Tasks internally.

---

## Databricks Spark Property Defaults

| Property | Default | Exam Relevance |
|---|---|---|
| `spark.sql.shuffle.partitions` | 200 | Controls parallelism for shuffles |
| `spark.sql.adaptive.enabled` | true | AQE — changes query plans at runtime |
| `spark.sql.files.maxPartitionBytes` | 128MB | File chunking for reads |
| `spark.databricks.delta.autoOptimize.enabled` | true | Auto file compaction for Delta |
| `spark.python.worker.memory` | 512MB | Python UDF memory limit |
