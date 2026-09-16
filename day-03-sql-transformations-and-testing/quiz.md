# Day 3 — Quiz: SQL Transformations and Testing

**Objective coverage:** Section 1 (22%) and Section 3 (10%)

---

## Question 1

**Objective:** Understand ROWS vs RANGE window frame distinction.

A query computes a running sum of revenue per customer. The customer has three orders on the same date (duplicate ORDER BY value), with revenues of 100, 200, and 300. The query uses:

```sql
SUM(revenue) OVER (
    PARTITION BY customer_id ORDER BY order_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS running_sum
```

What is the running_sum value for the third row (revenue = 300)?

A. 300  |  B. 500  |  C. 600  |  D. Only the prior physical row counts

---

## Question 2

**Objective:** Apply window functions for deduplication.

A table has duplicate records per email. You need to keep only the most recent record per email. Which approach is most reliable?

A. GROUP BY email with MAX(last_updated)
B. DENSE_RANK() OVER (PARTITION BY email ORDER BY last_updated DESC), filter rank = 1
C. ROW_NUMBER() OVER (PARTITION BY email ORDER BY last_updated DESC), filter row_num = 1
D. RANK() OVER (PARTITION BY email ORDER BY last_updated DESC), filter rank = 1

---

## Question 3

**Objective:** Distinguish LEFT SEMI from LEFT ANTI joins.

Which SQL returns customers who have NEVER placed an order?

A. SELECT * FROM customers LEFT SEMI JOIN orders ON customers.id = orders.customer_id
B. SELECT * FROM customers LEFT ANTI JOIN orders ON customers.id = orders.customer_id
C. SELECT * FROM customers JOIN orders ON customers.id = orders.customer_id
D. SELECT * FROM customers WHERE customer_id IN (SELECT customer_id FROM customers)

---

## Question 4

**Objective:** Understand LAST_VALUE default frame behavior.

A query uses LAST_VALUE(salary) OVER (PARTITION BY dept ORDER BY hire_date). It returns the current row salary instead of the highest salary in the partition. What is the correct fix?

A. Add ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING to the window
B. Use FIRST_VALUE instead of LAST_VALUE
C. Remove the ORDER BY clause
D. Add RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING

---

## Question 5

**Objective:** Apply broadcast join optimization.

A 500 GB fact table is joined with a 5 MB dimension table. Which approach is most correct?

A. Always use broadcast() on the large fact table
B. Use broadcast() on the dimension table; Spark auto-broadcasts below threshold
C. Disable AQE to force broadcast joins
D. Increase autoBroadcastJoinThreshold to 1GB

---

## Question 6

**Objective:** Develop tests using DataFrame.transform.

A team wants each ETL transformation function to be independently testable. Which DataFrame method supports this pattern?

A. DataFrame.pipe()  |  B. DataFrame.transform()  |  C. DataFrame.apply()  |  D. DataFrame.map()

---

## Question 7

**Objective:** Understand assertDataFrameEqual's default row-order behavior.

A test calls `assertDataFrameEqual(actual, expected)` where `actual` and `expected` contain exactly the same rows, but in a different order. Neither `checkRowOrder` nor any other optional parameter is passed. What is the outcome?

A. The test fails, because `assertDataFrameEqual` checks row order by default
B. The test passes, because `checkRowOrder` defaults to `False` — row order is not checked unless explicitly requested
C. The test fails with a schema mismatch error, unrelated to row order
D. The result is non-deterministic and may pass or fail randomly between runs

---

## Question 8

**Objective:** Understand GROUPING SETS vs ROLLUP vs CUBE.

GROUP BY ROLLUP(region, product_category) produces (region,product), (region), and grand total. Which GROUPING SETS produces the same output?

A. GROUPING SETS ((region, product_category), (region))
B. GROUPING SETS ((region, product_category), (region), ())
C. GROUPING SETS ((region, product_category), ())
D. GROUPING SETS ((region), (product_category), ())

---

## Question 9

**Objective:** Diagnose join ordering performance issues.

A query joins 100M fact rows with 10M dim rows, filtering dim to 100 rows before joining. What is the primary performance benefit?

A. CTEs use less memory by materializing temp tables
B. Filtering before the join reduces data volume entering the join operation
C. Spark SQL automatically optimizes the first version
D. Filter pushdown is disabled by default in Spark 3.x

---

## Question 10

**Objective:** Distinguish PIVOT from UNPIVOT.

A table has columns: region, Q1_revenue, Q2_revenue, Q3_revenue, Q4_revenue. Which transforms it to region, quarter, revenue (rows)?

A. GROUP BY region with SUM of all quarterly columns
B. PIVOT with FOR quarter IN ('Q1','Q2','Q3','Q4')
C. UNPIVOT using LATERAL VIEW EXPLODE(MAP(...))
D. ARRAY and STRUCT construction

---

## Question 11

**Objective:** Apply skew join optimization.

A join between a 50 GB fact table and a 5 MB dim table fails with executor OOM. One join key ('UNKNOWN') appears in 40% of fact rows. Which approach directly addresses the skew?

A. Increase executor memory to 64GB
B. Set spark.sql.adaptive.enabled = false
C. Salt the join: add a random salt column and replicate dim rows
D. Partition the fact table by join key before joining

---

## Question 12

**Objective:** Apply the salted-join pattern correctly (a common implementation mistake).

A data engineer fixes a skewed join between a 200 GB fact table and a 50 MB dimension table by salting the fact table's join key with a random integer 0–9 (e.g., `key-7`). They leave the dimension table's join key completely unchanged. After running the join, the result has far fewer rows than the unsalted version produced.

What is the most likely cause?

A. The salt value should be generated deterministically, not with `rand()`
B. The dimension table's join key was never replicated across the salt values, so a fact row salted to `key-7` has no matching dimension row to join against
C. Adaptive Query Execution automatically reverses manually written salting logic
D. Salting only works correctly when the two tables are close in size

---

## Answer Key

### Q1: C — 600
ROWS counts physical rows. All three rows are in the window frame. 100+200+300=600. RANGE would also give 600 here. The difference emerges when ORDER BY values differ slightly.

**Why others wrong:** A(300) = no window. B(500) = only 2 rows. D = describes RANGE, not ROWS.

### Q2: C — ROW_NUMBER() + filter row_num = 1
ROW_NUMBER guarantees exactly one row per partition. RANK and DENSE_RANK assign rank 1 to all rows with the top timestamp, so neither removes duplicates when timestamps tie.

**Why others wrong:** A=loses other columns. B/D=same timestamp = multiple rank-1 rows = no deduplication.

### Q3: B — LEFT ANTI JOIN
LEFT ANTI returns left rows where the key does NOT exist in right. LEFT SEMI returns rows that DO match.

**Why others wrong:** A=returns customers WITH orders. C=INNER, same as SEMI. D=subquery logic error.

### Q4: A — ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
LAST_VALUE defaults to RANGE UNBOUNDED PRECEDING TO CURRENT ROW. With ORDER BY, this returns the current row for each row. ROWS UNBOUNDED PRECEDING TO UNBOUNDED FOLLOWING spans the entire partition.

**Why others wrong:** B=FIRST_VALUE returns first, not last. C=removes ordering = arbitrary result. D=RANGE still groups by ORDER BY value.

### Q5: B — Use broadcast() on the dimension table
Broadcasting a 500 GB fact table causes OOM on every executor. The 5 MB dim is auto-broadcasted below the 10 MB default threshold.

**Why others wrong:** A=causes OOM. C=AQE does not force broadcasts. D=5 MB is below default threshold, no change needed.

### Q6: B — DataFrame.transform()
.transform() applies a function returning a DataFrame. Each stage is independently testable. This is the idiomatic ETL testing pattern.

**Why others wrong:** A=pipeline to RDD-like iterator. C=not a DataFrame method. D=works on RDDs, not DataFrames.

### Q7: B — The test passes, because `checkRowOrder` defaults to `False`
`assertDataFrameEqual`'s `checkRowOrder` parameter defaults to `False` (verified against current PySpark documentation) — meaning row order is **not** checked unless you explicitly pass `checkRowOrder=True`. This is a deliberate design choice, since PySpark DataFrame row ordering is non-deterministic in general unless explicitly sorted.

**Why others wrong:** A states the opposite of the documented default — a common and costly thing to memorize backwards. C invents an unrelated schema-mismatch mechanic; same schema, same rows in a different order does not trigger a schema error. D is false — the outcome is fully deterministic given the `checkRowOrder` setting, not a coin flip.

### Q8: B — GROUPING SETS ((region, product_category), (region), ())
ROLLUP(a,b) produces: (a,b), (a), grand total. This matches the three GROUPING SETS levels in option B.

**Why others wrong:** A=missing grand total. C=missing region-level. D=adds product_category alone which ROLLUP does not produce.

### Q9: B — Filtering before the join reduces data volume
Joins process all rows from both inputs. Reducing the dimension from 10M to 100 rows before the join dramatically reduces join cost.

**Why others wrong:** A=CTEs are not materialized. C=Spark does not auto-reorder joins in complex queries. D=filter pushdown is generally enabled — Catalyst does push simple, deterministic filters below joins automatically in many cases; the explicit rewrite matters most for filters that can't be pushed down automatically.

### Q10: C — UNPIVOT using LATERAL VIEW EXPLODE(MAP(...))
Wide-to-long transformation requires UNPIVOT. PIVOT does the opposite (rows to columns).

**Why others wrong:** A=collapses rows. B=pivots rows to columns, not columns to rows. D=not standard unpivot approach.

### Q11: C — Salt the join
Skew causes one partition to receive 40% of fact rows. Salting distributes skewed keys across partitions by replicating dim rows across every salt value.

**Why others wrong:** A=memory increase does not fix partition imbalance. B=disabling AQE removes automatic skew handling, which doesn't help and isn't the "direct" fix requested. D=partitioning by key does not fix the skew itself — the skewed key's rows would still all land in one partition.

### Q12: B — The dimension table's join key was never replicated across the salt values
Salting only works if **both** sides are made consistent: the large side gets a random salt appended to its key, and the small side must be exploded/replicated once per possible salt value so a matching row exists for every salt a large-side row could have been assigned. Leaving the dimension table's key unsalted means the salted fact key (e.g., `key-7`) can never match the dimension table's plain key (`key`) — most rows are silently dropped, with no error raised.

**Why others wrong:** A — determinism of the salt value is irrelevant; the bug is that only one side was salted. C — AQE does not inspect or reverse manually written join logic; it only affects Spark's own automatic query planning. D — salting is specifically useful when table sizes are very different (a large skewed table + a small dimension table), which is exactly this scenario, not a case where it "only works" for similarly-sized tables.

---

## Difficulty Ratings

| Q# | Difficulty | Topic |
|---|---|---|
| 1 | Medium | ROWS vs RANGE frame distinction |
| 2 | Medium | ROW_NUMBER deduplication vs RANK/DENSE_RANK |
| 3 | Easy | LEFT SEMI vs LEFT ANTI distinction |
| 4 | Hard | LAST_VALUE default frame behavior |
| 5 | Easy | Broadcast join — which table to broadcast |
| 6 | Easy | DataFrame.transform() method |
| 7 | Hard | assertDataFrameEqual is order-independent by default |
| 8 | Medium | ROLLUP vs CUBE vs GROUPING SETS |
| 9 | Medium | Join ordering and early filtering |
| 10 | Medium | PIVOT vs UNPIVOT distinction |
| 11 | Hard | Skew join handling — salted join |
| 12 | Hard | Salted-join implementation mistake — unsalted dimension side |
