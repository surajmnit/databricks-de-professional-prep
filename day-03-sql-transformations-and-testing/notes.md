# Day 3 — SQL Transformations, Advanced Operations, and Testing

## Exam Objectives (Exam Guide, July 2026)

Maps to **Section 1 (22%)** and **Section 3 (10%)**:
- "Write efficient Spark SQL and PySpark code to apply advanced data transformations, including window functions, joins, and aggregations"
- "Develop unit and integration tests using assertDataFrameEqual, assertSchemaEqual, DataFrame.transform, and testing frameworks"

---

## Part 1 — Advanced SQL Transformations

### Window Functions

Window functions operate on a set of rows defined by OVER. Unlike GROUP BY, rows retain their identity.

**Syntax skeleton:**
```sql
function_name() OVER (
    PARTITION BY column(s)    -- window boundaries
    ORDER BY column(s)        -- ordering within partition
    ROWS/RANGE BETWEEN ...    -- frame spec
)
```

### ROWS vs RANGE — Critical Distinction

- **ROWS:** Physical row count back from current row
- **RANGE:** Logical grouping by value; DIFFERENT when ORDER BY has duplicate values

```sql
-- ROWS: exactly 3 preceding + current = always 4 rows in window
SUM(amount) OVER (
    PARTITION BY customer_id ORDER BY order_date
    ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
) AS rolling_sum_4_rows

-- RANGE: all rows with same ORDER BY value as current row
SUM(amount) OVER (
    PARTITION BY customer_id ORDER BY order_date
    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS cumulative_by_value
```

**Exam trap:** ROWS and RANGE produce DIFFERENT results when ORDER BY column has duplicates. This is tested in scenarios with "tied values" or "duplicate keys."

### Ranking Window Functions

```sql
-- ROW_NUMBER: unique, no gaps (1, 2, 3, 4)
ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rn

-- RANK: ties share rank, gaps after (1, 2, 2, 4)
RANK() OVER (PARTITION BY dept ORDER BY salary DESC) as rnk

-- DENSE_RANK: ties share rank, no gaps (1, 2, 2, 3)
DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary DESC) as dense_rnk

-- PERCENT_RANK: rank as fraction 0.0 to 1.0
PERCENT_RANK() OVER (PARTITION BY dept ORDER BY salary DESC) as pct_rnk
```

**Common use — deduplication:**
```sql
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rn
    FROM employees
) t WHERE rn = 1
```

### Lag, Lead, FIRST_VALUE, LAST_VALUE

```sql
-- LAG: previous row value (with default)
LAG(revenue, 1, 0) OVER (PARTITION BY region ORDER BY month) as prev_revenue

-- LEAD: next row value
LEAD(revenue, 1, NULL) OVER (PARTITION BY region ORDER BY month) as next_revenue

-- FIRST_VALUE: first row in frame
FIRST_VALUE(salary) OVER (PARTITION BY dept ORDER BY hire_date) as first_salary

-- LAST_VALUE: last row in frame
LAST_VALUE(salary) OVER (PARTITION BY dept ORDER BY hire_date) as last_salary
```

**LAST_VALUE exam trap:** Default frame is RANGE UNBOUNDED PRECEDING TO CURRENT ROW — which returns the CURRENT row for every row (not the partition's last row). Use:
```sql
LAST_VALUE(salary) OVER (
    PARTITION BY dept ORDER BY hire_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
) as true_last_salary
```

### Aggregate Functions in Windows

```sql
-- Moving average (7-day)
AVG(revenue) OVER (
    PARTITION BY product ORDER BY date
    ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
) as ma_7

-- Running count
COUNT(*) OVER (
    PARTITION BY session_id ORDER BY timestamp
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) as event_sequence
```

---

## Part 2 — Advanced Joins

### Join Types in Spark

| Join Type | Behavior | Use When |
|---|---|---|
| INNER | Only matching keys | Default |
| LEFT OUTER | All left + matched right | Preserve left |
| RIGHT OUTER | All right + matched left | Preserve right |
| FULL OUTER | All rows | Union both sides |
| LEFT SEMI | Left rows matching right | Filter left, no right columns |
| LEFT ANTI | Left rows NOT in right | Exclusion |
| CROSS | Cartesian product | Pair everything |

### LEFT SEMI vs LEFT ANTI — Most Confused Pair

```sql
-- LEFT SEMI: customers who HAVE ordered (equivalent to IN subquery)
SELECT * FROM customers LEFT SEMI JOIN orders ON customers.id = orders.customer_id

-- LEFT ANTI: customers who have NOT ordered (equivalent to NOT IN subquery)
SELECT * FROM customers LEFT ANTI JOIN orders ON customers.id = orders.customer_id
```

**Exam trap:** LEFT SEMI does NOT return columns from the right table. It only filters the left. This is a frequent exam pattern.

### Broadcast Joins

Small table sent to all executors, avoiding shuffle entirely:

```python
from pyspark.sql.functions import broadcast
result = df_large.join(broadcast(df_small), "key")
```

```sql
-- SQL broadcast hint
SELECT /*+ BROADCAST(dim_table) */ *
FROM fact_table JOIN dim_table ON fact_table.key = dim_table.key
```

Threshold: `spark.sql.autoBroadcastJoinThreshold` (default 10MB).

**Exam trap:** Broadcasting a large table causes memory pressure on all executors. Broadcast is for SMALL tables. For large tables, sort-merge join is faster.

### Skewed Joins — Salted Join Pattern

When one key has vastly more rows than others (skew):

```python
from pyspark.sql.functions import expr, rand

# Salt the large table
n_salt = 10
df_large_salted = df_large     .withColumn("salt", (rand() * n_salt).cast("int"))     .withColumn("join_key", expr("concat(join_key, '-', salt)"))

# Add salt to small table
salted_keys = [(f"key-{i}",) for i in range(n_salt)]
df_small_salted = spark.createDataFrame(salted_keys, ["join_key"])

# Join
result = df_large_salted.join(df_small_salted, "join_key")
```

Spark 3.x AQE handles skew automatically: `spark.sql.adaptive.skewJoin.enabled` (default: true).

### Join Ordering

Always filter BEFORE joining to reduce data volume:

```sql
-- Bad: full join THEN filter
SELECT * FROM large JOIN small ON ... WHERE small.category = 'active'

-- Good: filter first
WITH filtered AS (SELECT * FROM small WHERE category = 'active')
SELECT * FROM large JOIN filtered ON large.key = filtered.key
```

---

## Part 3 — Advanced Aggregations

### GROUPING SETS

```sql
SELECT region, product_category, SUM(revenue)
FROM orders
GROUP BY GROUPING SETS (
    (region, product_category),  -- granular
    (region),                     -- by region
    ()                           -- grand total
)
```

### ROLLUP vs CUBE

```sql
-- ROLLUP: hierarchical (a,b) > (a) > grand total
GROUP BY ROLLUP(region, product_category)

-- CUBE: all combinations (a,b) > (a) > (b) > grand total
GROUP BY CUBE(region, product_category)
```

### PIVOT

```sql
SELECT * FROM sales PIVOT (SUM(revenue) FOR quarter IN ('Q1','Q2','Q3','Q4'))
```

PySpark:
```python
df.groupBy("region","product").pivot("quarter").agg(sum("revenue").alias("revenue"))
```

---

## Part 4 — DataFrame.transform and Testing

### DataFrame.transform

.apply() a chain of transformation functions where each is independently testable:

```python
from pyspark.sql import DataFrame

def clean_names(df: DataFrame) -> DataFrame:
    from pyspark.sql.functions import upper, trim, col
    return df.withColumn("name", upper(trim(col("name"))))

def add_metadata(df: DataFrame) -> DataFrame:
    from pyspark.sql.functions import current_timestamp
    return df.withColumn("processed_at", current_timestamp())

# Each transform is independently testable
result = (
    raw_df
    .transform(clean_names)
    .transform(add_metadata)
)
```

**Key advantage over inline chains:** Each function can be unit tested in isolation without running the full pipeline.

### assertDataFrameEqual and assertSchemaEqual (Spark 3.4+)

```python
from pyspark.testing import assertDataFrameEqual, assertSchemaEqual

# Order-independent data comparison
expected = spark.createDataFrame([("Alice",30),("Bob",25)], ["name","age"])
assertDataFrameEqual(actual.orderBy("name"), expected)

# Schema comparison
assertSchemaEqual(actual, "name STRING, age INT")
```

Behaviors: order-independent by default, does not compare metadata (nullability, comments), raises AssertionError with diff on mismatch.

### Unit Testing Pattern

```python
import pytest
from pyspark.sql import SparkSession
from pyspark.testing import assertDataFrameEqual
from pyspark.sql.functions import col

@pytest.fixture
def spark():
    return SparkSession.builder.master("local[2]").appName("tests").getOrCreate()

def test_clean_names(spark):
    input_df = spark.createDataFrame([("  alice  ", 30)], ["name", "age"])
    result = clean_names(input_df)
    expected = spark.createDataFrame([("ALICE", 30)], ["name", "age"])
    assertDataFrameEqual(result, expected)

def test_add_timestamp(spark):
    input_df = spark.createDataFrame([("test",)], ["col"])
    result = add_metadata(input_df)
    assert "processed_at" in result.columns
    assert result.filter(col("processed_at").isNull()).count() == 0
```

### Integration Testing

```python
def test_full_pipeline(spark, tmp_path):
    test_path = str(tmp_path / "source")
    spark.createDataFrame([("bob", 100)], ["name","amount"]) \
        .write.mode("overwrite").parquet(test_path)
    
    result = run_pipeline(spark, test_path)
    expected = spark.createDataFrame([("BOB", 100)], ["name","amount"])
    assertDataFrameEqual(result.orderBy("name"), expected)
```

### Built-in Debugger

Databricks: click the line number gutter to set breakpoints, click Debug (bug icon) to step through Python cells line by line. Inspect variable values in the Variables panel. SQL cells: use DISPLAY() or .show() for debugging.

---

## Cross-References

- Day 5: Wide vs narrow transformations — groupBy creates shuffle boundaries
- Day 6: Shuffle read/write during joins
- Day 8: Identifying inefficient join strategies via Query Profile
- Day 15: Control flow operators in Lakeflow pipelines
- Day 17: Quarantine patterns and data quality validation
