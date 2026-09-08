# Day 7 — Cheat Sheet: Spark Memory, Spill, and GC

## Executor Memory Architecture

```
Executor JVM Heap (e.g., 64GB)
+------------------------------------------------------------+
| Execution + Storage Pool (38.1 GB = (64 - 0.3) * 0.6)      |
|   Execution: shuffles, sorts, joins, aggregation              |
|   Storage: cache, broadcasts                                 |
+------------------------------------------------------------+
| User Memory (12.7 GB = (64 - 0.3) * 0.2)                   |
|   UDF variables, data structures                            |
+------------------------------------------------------------+
| Reserved (0.3 GB)                                          |
+------------------------------------------------------------+
```

**Execution can evict Storage. Storage CANNOT evict Execution.**

---

## Driver vs Executor OOM

| Who Failed | Root Cause | Fix |
|---|---|---|
| Driver | collect() of large result; large broadcast; groupBy output | spark.driver.maxResultSize; write to storage |
| One executor | Data skew / one large partition | More partitions; AQE skew join; salt join |
| All executors | Cache bloat; partitions too large | Unpersist; increase partitions |
| Python subprocess | Python UDF memory exceeded | spark.python.worker.memory; Pandas UDF |

---

## Python UDF vs Pandas UDF Memory

| | Python UDF | Pandas UDF |
|---|---|---|
| Process | Separate Python subprocess | Within JVM |
| Memory source | spark.python.worker.memory (off-heap) | Executor JVM heap |
| GC pressure | Python GC | Java GC (low) |
| Default limit | 512MB per worker | No separate limit |

**Key: Python UDF OOM shows as executor failure but JVM heap looks fine.**

---

## Spill

- **What:** Data written to disk because executor memory insufficient
- **When:** Sort, hash aggregation, groupBy with large partitions
- **Spark UI:** "Spilled Records" and "Spilled Size" in Stage metrics
- **Fix:** Increase spark.sql.shuffle.partitions; unpersist cache; reduce Python UDF memory

---

## Key Memory Properties

| Property | Default | Use |
|---|---|---|
| spark.executor.memory | Config | Total JVM heap per executor |
| spark.driver.maxResultSize | 1GB | Max collect() result at driver |
| spark.python.worker.memory | 512MB | Python subprocess heap (per worker) |
| spark.memory.fraction | 0.6 | Fraction of heap for execution + storage |
| spark.memory.storageFraction | 0.5 | Fraction of memory fraction for storage |

---

## Cache Management

```python
df.cache()        # Cache DataFrame
df.unpersist()     # Free executor memory
spark.catalog.clearCache()  # Clear all cache
```

**Unpersist() when done — Spark doesn't auto-clear.**

---

## Key Exam Traps

1. **Execution can evict Storage. Storage CANNOT evict Execution.** (Q1)
2. **Python UDF OOM = executor failure but JVM heap fine** (Q2)
3. **spark.driver.maxResultSize** limits collect(), not driver.memory (Q3)
4. **Cache bloat** fills storage memory → execution memory starved → OOM (Q4)
5. **Spill ≠ shuffle write** — spill is overflow from memory pressure (Q5)
6. **38.1 GB** = (64 - 0.3) × 0.6 — remember the 300MB reserved (Q8)
7. **"Lost task" = executor failure** — driver failures say "driver disconnected" (Q9)
8. **unpersist() must be called** — Spark doesn't auto-evict proactively (Q10)
9. **Python UDF fix = spark.python.worker.memory** — not executor.memory (Q11)
