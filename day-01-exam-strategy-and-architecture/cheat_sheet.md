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

**Recovery on Databricks:** driver failure → recover via **Jobs-level `max_retries`**, not an OSS Spark "cluster mode/client mode" concept (Databricks abstracts that away — don't reach for it on a Databricks-flavored question).

---

## Repartition vs Coalesce

| Operation | Shuffle? | Can Increase? | Can Decrease? |
|---|---|---|---|
| `repartition(n)` | Yes (always) | Yes | Yes |
| `coalesce(n)` | No | No | Yes (to n) |

`coalesce(1000)` on 10-partition DataFrame → returns 10 (unchanged, not an error)

---

## Spark Memory Regions (Executor)

Nested, not flat — percentages are shares of the *remaining* heap after a small fixed reservation, and they sum to 100%:

| Region | Purpose | Sizing |
|---|---|---|
| Reserved Memory | Fixed JVM reservation | ~300MB, fixed |
| User Memory | UDF variables (non-Pandas), Python-adjacent data | ~40% of remaining heap |
| Spark Memory | Shared pool for Execution + Storage | ~60% of remaining heap (`spark.memory.fraction`) |
| ↳ Storage Memory | Cache, broadcasts | `spark.memory.storageFraction` (default 0.5) of Spark Memory |
| ↳ Execution Memory | Shuffle sort, hash join, internal rows | Remainder of Spark Memory |

**Key rule:** Execution can evict Storage's cached data (LRU) when it needs more room; **Storage can never evict Execution**. If Execution is still short after evicting everything from Storage, it spills to disk instead of erroring.

Python UDF memory = off-heap (`spark.python.worker.memory`, default 512MB/worker), **NOT** in this executor heap diagram at all.

---

## Platform Architecture — Key Points

| Concept | Key Fact |
|---|---|
| Control Plane | Manages metadata, jobs, UI, **and Unity Catalog's metastore/governance service** — NOT the data path |
| Data Plane | Executors read/write directly to cloud storage; this is where UC-governed data files and compute actually live |
| DBFS | Mount layer over S3/ADLS/GCS — NOT separate storage |
| Unity Catalog | Account-level, not cluster-level; overrides cluster ACLs when enabled — lives in the control plane, don't list it as a data-plane component |
| Cluster Manager | Fully abstracted on Databricks — don't assume a 1:1 mapping to OSS Standalone/YARN/Kubernetes per cloud |
| Job Cluster | Cold-starts on every trigger (several minutes) — NOT inherently "faster" than all-purpose |
| All-Purpose Cluster | Fast only if already running and attached to; cold-starts otherwise |

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
6. DBR version ≠ Spark version you might assume — verify the current mapping (e.g., Spark 4.0 arrives at DBR 17, not 15) rather than trusting a memorized table

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
