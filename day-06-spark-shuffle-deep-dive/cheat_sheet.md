# Day 6 — Cheat Sheet: Spark Shuffle Deep-Dive

## What Triggers a Shuffle?

| Triggers Shuffle | Does NOT Trigger Shuffle |
|---|---|
| groupBy() | filter() |
| join() | withColumn() |
| repartition() | select() |
| sort() / orderBy() | drop() |
| distinct() | coalesce(n) where n < current |
| window functions | cast() |

---

## Shuffle Write

- Each map task writes to local disk: **map_tasks × shuffle_partitions = shuffle files**
- 100 map tasks × 200 shuffle partitions = **20,000 shuffle files** (too many = overhead)
- Data written to local disk first, then read by reduce tasks

## Shuffle Read

- Each reduce task reads from ALL map tasks for its partition
- Local read = data on same executor (fast)
- Remote read = data on other executor (network cost)

---

## Shuffle Partition Count

`spark.sql.shuffle.partitions` (default 200)

**Rule:** 128–256 MB per shuffle partition target.
- 10 GB data / 200 partitions = 50 MB per partition (fine)
- 10 GB data / 10 partitions = 1 GB per partition (OOM risk)

| Too Few | Too Many |
|---|---|
| Large per-partition data | Excessive small files |
| OOM risk | Task scheduling overhead |
| Less parallelism | GC pressure |
| Skew more likely | Slow job startup |

**AQE coalesces small partitions automatically** (enabled by default).

---

## Data Skew

**Signature:** One task Duration >> others; one task Shuffle Write Bytes >> others

**Fix:**
1. AQE skew join (default: enabled) — auto-splits skewed partitions
2. Manual salted join — replicate small table rows by salt key
3. Increase shuffle partitions — more partitions = smaller skewed partition

---

## Broadcast vs Shuffle

| | Threshold | Behavior |
|---|---|---|
| Auto-broadcast | < 10 MB | No shuffle — small table sent to all executors |
| No broadcast | > 10 MB × 1.1 | Sort-merge join — both tables shuffled |
| At threshold | ~10 MB | Spark estimates |

**Broadcast the SMALL table only.** Broadcasting a large table causes executor OOM.

---

## Shuffle Spill

- Triggered when executor memory insufficient for sort/aggregation
- Spark writes data to disk → **slow**
- Spark UI: "Spilled Records", "Spilled Size" metrics
- Fix: increase partitions, unpersist cache, reduce Python UDF memory

---

## Key Exam Traps

1. **Shuffle write file count = map_tasks × shuffle_partitions** (product, not sum)
2. **coalesce(500) on 100-partition DF = 100** (cannot increase)
3. **Broadcast the small table** — large table broadcast = executor OOM
4. **AQE skew join = split skewed partition** into sub-partitions (not memory increase or retry)
5. **Filter AFTER groupBy** — runs after shuffle (expensive). Filter BEFORE groupBy — runs before shuffle (cheaper)
6. **Remote read** = data on another executor (network). **Local read** = data on same executor (fast)
7. **Shuffle spill** = memory insufficient → disk write (not the same as normal shuffle write)
