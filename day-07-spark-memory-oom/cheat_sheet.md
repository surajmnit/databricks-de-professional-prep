# Day 7 — Cheat Sheet: Spark Memory, GC, and OOM

## Executor Memory Regions

```
Executor Memory (spark.executor.memory)
├─ Spark Memory (spark.memory.fraction = 0.6 of heap)
│   ├─ Execution Memory (~60%) — shuffle sort, hash join, aggregation
│   └─ Storage Memory (~40%) — cached DataFrames, broadcasts
├─ User Memory (~30%) — Python code, UDF data, string interning
└─ System Memory (~10%) — JVM internal structures
```

---

## Unified Memory Model (Spark 1.6+)

Execution and Storage share a pool. **Execution cannot evict Storage.**
- Execution full → **spill to disk** (shuffle spill)
- Storage full → **evict LRU cached data** (normal)

---

## Python UDF Memory

Python UDFs run in **separate Python subprocess**, NOT in JVM heap.
- Property: spark.python.worker.memory (default 512MB per worker)
- Fix for Python OOM: Use Pandas UDFs (JVM/Arrow) or increase spark.python.worker.memory

---

## Driver vs Executor OOM

| Who | Cause | Fix |
|---|---|---|
| Driver | collect() of large result; large broadcast | Write to storage; increase maxResultSize |
| Executor (one at 100%) | Data skew | AQE skew join, salted join |
| Executor (all high) | Cache bloat; too few partitions | Unpersist cache; increase partitions |
| Python subprocess (JVM fine) | Python UDF memory exceeded | Pandas UDFs; increase spark.python.worker.memory |

---

## Cache Levels

| Level | Memory | CPU | Disk |
|---|---|---|---|
| MEMORY_ONLY | Full data in heap | Low | None |
| MEMORY_AND_DISK | Memory then spill | Low | Spill if needed |
| MEMORY_ONLY_SER | Less (serialized) | High (deserialize) | None |
| DISK_ONLY | None | Low | Full data |

**Key:** SER uses less memory but more CPU. MEMORY_AND_DISK is the default.

---

## GC Pressure

- High GC time (> 10% of executor time) = too many short-lived objects
- Fix: Replace Python UDFs with Pandas UDFs; reduce string operations; cache at appropriate level
- Spark UI Executors tab shows GC Time and GC Count per executor

---

## Key Exam Traps

1. **Python UDF OOM with JVM heap fine** = Python subprocess exhausted, not JVM
2. **Execution cannot evict storage** in unified memory = when full, execution spills to disk
3. **collect() on large data = driver OOM**, not executor OOM
4. **Broadcast uses storage memory** on every executor, not just driver
5. **MEMORY_AND_DISK never errors** on large data = spills to disk
6. **Storage memory uses LRU eviction** when full
7. **Shuffle partitions too few = large per-task memory = OOM/skew risk**
