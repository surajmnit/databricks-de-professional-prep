# Weak Topics Tracker — Databricks Certified Data Engineer Professional

> Updated after each day's quiz and each mock exam. Topics with repeated failures automatically escalate to higher priority.

## Priority Key

| Priority | Meaning |
|---|---|
| 🔴 Critical | Repeatedly wrong across multiple assessments — needs immediate remediation |
| 🟠 High | Missed on first attempt; understand the concept but made a reasoning error |
| 🟡 Medium | Partially correct; partial understanding or common trap pattern |
| 🟢 Low | Minor gap; confident overall |

## Master Weak Topics List

| Topic | Priority | First Wrong | Repeated? | Last Reviewed | Notes |
|---|---|---|---|---|---|
| Spark Driver vs Executor OOM | 🔴 | Day 1 Q3, Q6 | — | — | Need more practice distinguishing memory failure points |
| Spark Task vs Databricks Job Task | 🟠 | Day 1 Q2 | — | — | Scope difference not fully internalized |
| Coalesce cannot increase partitions | 🟠 | Day 1 Q5 | — | — | Common trap — confused with repartition |
| UC vs legacy cluster ACLs | 🟠 | Day 1 Q7 | — | — | Need to memorize: UC overrides cluster ACLs when enabled |
| Python UDF off-heap memory | 🟠 | Day 1 Q8 | — | — | Need to remember it's NOT in executor JVM heap |
| Shuffle → Stage boundary | 🟡 | Day 1 Q1 | — | — | Understand it but need more practice identifying from code |
| Trigger interval in streaming | 🟡 | Day 1 Q9 | — | — | Keyword recognition needs strengthening |
| spark.stage.maxAttempts | 🟡 | Day 1 Q10 | — | — | Stage retry vs task retry distinction not fully clear |

---

## Pattern Analysis (Updated After Each Assessment)

### Common Wrong-Answer Patterns

| Pattern | Description | Occurrences | Priority |
|---|---|---|---|
| Driver/Executor confusion | Choosing wrong node for OOM cause | 2+ so far | 🔴 |
| Terminology confusion | Using old name for renamed feature | 1 so far | 🟡 |
| Scope mismatch | Confusing Spark Task with Job Task | 1 so far | 🟠 |
| Coalesce vs Repartition | Assuming coalesce can increase partitions | 1 so far | 🟠 |

---

## Section-Level Confidence Tracker

> Rate 1–5 after completing each section's study day. 1 = no confidence, 5 = exam-ready.

| Section | Self-Rating | Target | Gap | Action |
|---|---|---|---|---|
| 1. Python + SQL Development (22%) | — | 5 | — | |
| 2. Data Ingestion (7%) | — | 4 | — | |
| 3. Data Transformation (10%) | — | 4 | — | |
| 4. Data Sharing & Federation (5%) | — | 3 | — | |
| 5. Monitoring & Alerting (10%) | — | 4 | — | |
| 6. Cost & Performance (13%) | — | 4 | — | |
| 7. Security & Compliance (10%) | — | 4 | — | |
| 8. Data Governance (7%) | — | 3 | — | |
| 9. Debugging & Deploying (10%) | — | 4 | — | |
| 10. Data Modeling (6%) | — | 3 | — | |

---

## Mock Exam Weak-Area Summary

### Mock Exam #1 (Day 27)

| Section | Questions Wrong | Topic Focus | Priority |
|---|---|---|---|
| (Pending) | — | — | — |

### Mock Exam #2 (Day 29)

| Section | Questions Wrong | Topic Focus | Priority |
|---|---|---|---|
| (Pending) | — | — | — |

---

## Micro-Drill Queue

> Questions generated for weak areas — complete these on weak-area remediation days.

| Topic | Questions in Queue | Status |
|---|---|---|
| Driver vs Executor OOM | 5 | Pending |
| UC permission inheritance | 3 | Pending |
| Spark shuffle behavior | 3 | Pending |
| Liquid Clustering vs Partitioning vs Z-Ordering | 3 | Pending |
| Delta Sharing vs Lakehouse Federation | 3 | Pending |

---

## How to Use This Tracker

1. **After each quiz:** Update the "First Wrong" column with the question number
2. **If repeated:** Mark "Repeated? = Yes" — auto-escalates priority
3. **After mock exams:** Update section-level confidence and mock exam tables
4. **Before remediation days:** Generate micro-drills for all topics rated priority ≥ High
5. **The night before the exam:** Review only 🔴 Critical topics from this file


---

## Day 2 Quiz Analysis

| Q# | Topic | Priority | Key Insight |
|---|---|---|---|
| Q1 | Python import in Workspace | High | Files in /Workspace need pip install or sys.path - not automatic |
| Q2 | pip install scope | Critical | Driver only; UDFs need cluster-level install |
| Q3 | Pandas UDF GROUPED_MAP | Medium | Per-group ops need GROUPED_MAP type annotation |
| Q4 | DAB targets pattern | Medium | Single databricks.yml, multiple targets - not multiple YAML files |
| Q5 | DBR bundled package shadowing | Medium | DBR system packages can shadow pip-installed packages |
| Q6 | DAB --force vs --overwrite | Medium | The flag is --force, not --overwrite |
| Q7 | Pandas UDF return type | Medium | Use StringType (class), NOT StringType() (instance) |
| Q8 | run vs import | Medium | run merges global scope; proper packages prevent namespace conflicts |
| Q9 | DAB path validation | Medium | Bundle deploy writes path as-is; no existence check |
| Q10 | UDF optimization | Medium | SQL > Pandas UDF > Python UDF; always check if SQL suffices first |


---

## Day 3 Quiz Analysis

| Q# | Topic | Priority | Key Insight |
|---|---|---|---|
| Q1 | ROWS vs RANGE with duplicates | Medium | Same result here; differ when ORDER BY values differ slightly |
| Q2 | ROW_NUMBER deduplication | High | Only ROW_NUMBER guarantees 1 row per partition; RANK/DENSE_RANK allow ties |
| Q3 | LEFT SEMI vs LEFT ANTI | Low | SEMI=exists in right; ANTI=does NOT exist in right |
| Q4 | LAST_VALUE default frame | High | Default frame returns current row for every row; need ROWS UNBOUNDED FOLLOWING |
| Q5 | Broadcast join target | Low | Broadcast SMALL table only; large table broadcast causes OOM |
| Q6 | DataFrame.transform() | Low | .transform() is the exam-relevant API; .pipe() is RDD-based |
| Q7 | assertDataFrameEqual order | High | Order-sensitive by default; need checkRowOrder=False |
| Q8 | ROLLUP vs CUBE vs GROUPING SETS | Medium | ROLLUP= hierarchical; CUBE=all combos; GROUPING SETS=explicit |
| Q9 | Join ordering / early filter | Medium | Filter before join reduces join volume |
| Q10 | PIVOT vs UNPIVOT | Medium | PIVOT=rows to cols; UNPIVOT=cols to rows |
| Q11 | Skew join / salted join | High | AQE handles automatically; manual fix is salted join |
| Q12 | Control flow in pipelines | Medium | Classic jobs=Python if/else; Lakeflow=declarative operators |


---

## Day 4 Quiz Analysis

| Q# | Topic | Priority | Key Insight |
|---|---|---|---|
| Q1 | Job/Stage/Task hierarchy | Medium | Read+filter=same Stage; groupBy=new Stage; 1 action=1 Job |
| Q2 | Narrow vs wide transformations | Low | filter=narrow (no Stage); join/repartition/sort=wide (new Stage) |
| Q3 | Driver OOM from collect() | High | Check spark.driver.maxResultSize |
| Q4 | Shuffle map stage task count | Hard | Task count = input partition count, not shuffle partition count |
| Q5 | Executor heartbeat/loss | Medium | 3 missed heartbeats (30s) marks executor lost |
| Q6 | coalesce cannot increase | Low | No-op when n > current; returns current count |
| Q7 | Data skew signature | High | One executor 100%, others idle = skew |
| Q8 | executor.cores concurrency | Low | cores x executors = total concurrent tasks |
| Q9 | Dynamic allocation max | Low | maxExecutors is the ceiling |
| Q10 | Lost task = executor failure | Hard | Lost task messages come from executors, not driver |
| Q11 | Shuffle partition sizing | Hard | TB / partitions = partition size; 1TB/50 = 20GB = OOM |
| Q12 | stage.maxAttempts vs task.maxFailures | Medium | Stage retries all tasks; task retries individual task |


---

## Day 5 Quiz Analysis

| Q# | Topic | Priority | Key Insight |
|---|---|---|---|
| Q1 | Lazy evaluation timing | Low | Execution starts at FIRST action, not at transformations |
| Q2 | Narrow vs wide | Low | join = wide (shuffle); filter/select/withColumn = narrow |
| Q3 | DAG with multiple downstream ops | Hard | Two groupBys = two shuffle boundaries = 4 stages total |
| Q4 | explain() order | Low | Bottom to top = execution order |
| Q5 | Exchange = stage boundary | Medium | Exchange operator is where new Stage starts |
| Q6 | Catalyst column pruning | Medium | Pruning removes unused columns; pushdown moves filters |
| Q7 | Whole-stage codegen prefix | Low |  prefix = codegen active |
| Q8 | orderBy = shuffle | Medium | Global ordering requires all data = shuffle boundary |
| Q9 | Multiple actions without cache | Low | Each action re-reads from source |
| Q10 | Unnecessary shuffle pattern | Medium | repartition(500).coalesce(20) wastes the 500-partition shuffle |
| Q11 | BroadcastExchange | Low | Small table broadcast = BroadcastExchange, not large table |
| Q12 | Filter above vs below Exchange | Hard | Filter below Exchange = filter after shuffle = wasted shuffle volume |


---

## Day 6 Quiz Analysis

| Q# | Topic | Priority | Key Insight |
|---|---|---|---|
| Q1 | What triggers shuffle | Low | repartition = shuffle; filter/withColumn/select = narrow |
| Q2 | Shuffle write file count | Hard | map_tasks x shuffle_partitions = product, not sum |
| Q3 | Shuffle read mechanics | Medium | Each reduce reads from ALL map tasks for its partition |
| Q4 | Shuffle partition sizing | Medium | 20 GB / 10 = 2 GB per partition = OOM/skew risk |
| Q5 | Spark UI skew diagnosis | Low | One task 30s vs others 2-3s = skew signature |
| Q6 | Broadcast which table | Low | Broadcast small table only; large table broadcast = OOM |
| Q7 | AQE skew join behavior | Medium | Splits skewed partition into sub-partitions (not retry or memory increase) |
| Q8 | Shuffle spill terminology | Low | Spill = disk write from memory pressure; not normal shuffle write |
| Q9 | Broadcast threshold edge case | Medium | At threshold Spark estimates; not automatic |
| Q10 | Unnecessary shuffle pattern | Medium | repartition(1000).coalesce(10) wastes the 1000-partition shuffle |
| Q11 | Local vs remote shuffle read | Medium | Local = same executor = fast; Remote = other executor = network |
| Q12 | Shuffle partition reasoning | Hard | 2.5 GB per partition fits memory but skew risk exists |


---

## Day 7 Quiz Analysis

| Q# | Topic | Priority | Key Insight |
|---|---|---|---|
| Q1 | Memory regions | Low | Cached DataFrames use storage memory, not execution memory |
| Q2 | Unified memory eviction | High | Execution cannot evict storage; spills to disk instead |
| Q3 | Python UDF subprocess memory | Hard | Python OOM with JVM fine = Python worker exhausted, not JVM |
| Q4 | Driver vs executor OOM | Low | collect() = driver OOM; "Lost connection to driver" |
| Q5 | GC pressure from Python UDFs | Medium | Pandas UDFs (JVM/Arrow) reduce GC, not Python UDFs |
| Q6 | MEMORY_AND_DISK behavior | Low | Never errors; spills to disk when memory full |
| Q7 | Storage LRU eviction | Low | LRU eviction when storage is full |
| Q8 | spark.python.worker.memory | Low | Controls Python subprocess heap, not JVM heap |
| Q9 | Partition size OOM risk | Medium | 50GB/10 partitions = 5GB per partition = skew/OOM risk |
| Q10 | Broadcast storage memory | Low | Each executor holds full broadcast in storage memory |
| Q11 | Cache only reused DataFrames | Medium | One-time DataFrames should not be cached |
| Q12 | SER memory/CPU trade-off | Medium | SER uses less memory but more CPU for deserialization |


---

## Day 7 Quiz Analysis

| Q# | Topic | Priority | Key Insight |
|---|---|---|---|
| Q1 | Unified memory eviction rules | Medium | Execution CAN evict Storage; Storage CANNOT evict Execution |
| Q2 | Python UDF off-heap memory | Medium | Python OOM = executor failure but JVM heap looks fine |
| Q3 | spark.driver.maxResultSize | Easy | Limits collect() result, not driver.memory |
| Q4 | Cache bloat OOM mechanism | Medium | Cache fills storage, execution starved → OOM |
| Q5 | Shuffle spill vs shuffle write | Low | Spill = overflow, not normal shuffle |
| Q6 | Spill reduction via partitions | Medium | More partitions = smaller per-partition data |
| Q7 | GC pressure and Pandas UDFs | Medium | Pandas UDFs reduce object churn and GC |
| Q8 | Memory fraction calculation | Hard | (64 - 0.3) x 0.6 = ~38.1 GB |
| Q9 | Driver vs executor failure mode | Medium | Lost task = executor; driver disconnected = driver |
| Q10 | When to call unpersist() | Easy | Spark auto-evicts under pressure but not proactively |
| Q11 | Python worker memory fix | Medium | spark.python.worker.memory, not executor.memory |
| Q12 | Python UDF vs Pandas UDF memory | Easy | Python = off-heap subprocess; Pandas = JVM heap via Arrow |


---

## Day 20 Quiz Analysis

| Q# | Topic | Priority | Key Insight |
|---|---|---|---|
| Q1 | Delta Sharing architecture | Medium | Open protocol = any platform; live data; no copy |
| Q2 | D2D vs D2O | Medium | D2D=Unity Catalog identity; D2O=REST+token |
| Q3 | Lakehouse Federation use case | Easy | Query external DB in place; no copy |
| Q4 | Federated query performance | Medium | Pushes computation to source; only results returned |
| Q5 | Direction of data flow | Hard | DELTA SHARING = OUT; FEDERATION = IN |
| Q6 | Governance on federated tables | Medium | Row filters/masks work identically |
| Q7 | Recipient type selection | Medium | OPEN=non-DB; Account=Databricks workspace |
| Q8 | Access revocation | Medium | Immediate; data never copied |
| Q9 | Supported federation sources | Easy | MySQL, PG, Redshift, Snowflake, BigQuery; NOT Excel |
| Q10 | Scenario pattern selection | Medium | Auditor+Power BI = Delta Sharing OPEN recipient |
