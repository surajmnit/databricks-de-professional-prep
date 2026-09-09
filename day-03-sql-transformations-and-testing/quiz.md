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

**Objective:** Use assertDataFrameEqual with correct options.

A test uses assertDataFrameEqual with df.orderBy('name') and an expected DataFrame in a different row order. The test fails. What is needed?

A. Use assertSchemaEqual instead
B. Pass checkRowOrder=False to make comparison order-independent
C. Remove orderBy from both DataFrames
D. Use checkSchema=False

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

**Objective:** Use control flow in pipeline contexts.

A pipeline should run silver transform only if bronze has more than 10,000 records. Which pattern is correct?

A. if/else using spark.read against bronze table count (classic jobs)
B. for/each loop over bronze table rows
C. Lakeflow pipeline with if/else control flow operators
D. Both A and C are valid; A for classic jobs, C for Lakeflow

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

### Q7: B — Pass checkRowOrder=False
assertDataFrameEqual is order-dependent by default. Use checkRowOrder=False for order-independent comparison.

**Why others wrong:** A=different error for schema mismatch. C=removing orderBy does not help if data order differs. D=schema is not the issue.

### Q8: B — GROUPING SETS ((region, product_category), (region), ())
ROLLUP(a,b) produces: (a,b), (a), grand total. This matches the three GROUPING SETS levels in option B.

**Why others wrong:** A=missing grand total. C=missing region-level. D=adds product_category alone which ROLLUP does not produce.

### Q9: B — Filtering before the join reduces data volume
Joins process all rows from both inputs. Reducing the dimension from 10M to 100 rows before the join dramatically reduces join cost.

**Why others wrong:** A=CTEs are not materialized. C=Spark does not auto-reorder joins in complex queries. D=filter pushdown is generally enabled.

### Q10: C — UNPIVOT using LATERAL VIEW EXPLODE(MAP(...))
Wide-to-long transformation requires UNPIVOT. PIVOT does the opposite (rows to columns).

**Why others wrong:** A=collapses rows. B=pivots rows to columns, not columns to rows. D=not standard unpivot approach.

### Q11: C — Salt the join
Skew causes one partition to receive 40% of fact rows. Salting distributes skewed keys across partitions by replicating dim rows.

**Why others wrong:** A=memory increase does not fix partition imbalance. B=disabling AQE removes automatic skew handling. D=partitioning by key does not fix the skew itself.

### Q12: D — Both A and C are valid
Classic jobs: Python if/else using spark.read table counts. Lakeflow pipelines: declarative if/else operators. Both are correct in their respective contexts.

**Why others wrong:** A/C alone are each incomplete. B=for/each is for iteration, not conditional checks.

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
| 7 | Medium | assertDataFrameEqual order sensitivity |
| 8 | Medium | ROLLUP vs CUBE vs GROUPING SETS |
| 9 | Medium | Join ordering and early filtering |
| 10 | Medium | PIVOT vs UNPIVOT distinction |
| 11 | Hard | Skew join handling — salted join |
| 12 | Medium | Control flow in classic jobs vs Lakeflow |