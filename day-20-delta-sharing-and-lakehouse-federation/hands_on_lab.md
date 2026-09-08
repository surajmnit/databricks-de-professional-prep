# Day 20 — Hands-On Lab: Delta Sharing and Lakehouse Federation

## Lab Objectives

1. Create a Delta Sharing share and grant access (simulated)
2. Query a federated external table (simulated with a mock connector)
3. Compare Delta Sharing vs Lakehouse Federation patterns
4. Observe Unity Catalog integration for both features

**Note:** Full hands-on with real external systems requires cloud credentials. This lab demonstrates the patterns using Databricks features where available and provides read-only demonstrations of the syntax.

---

## Step 1 — Delta Sharing: Create a Share (Provider Side)

**Objective:** Understand how to create and manage a Delta Sharing share.

### 1a. Create a sample share (syntax demonstration)

```python
# Provider: Create a share
spark.sql("CREATE SHARE IF NOT EXISTS analytics_share")

# Add tables to the share
spark.sql("""
    ALTER SHARE analytics_share
    ADD TABLE prod.sales AS sales_data
""")

spark.sql("""
    ALTER SHARE analytics_share
    ADD TABLE prod.customers
""")

# List shares
shares = spark.sql("SHOW SHARES")
shares.display()
```

### 1b. Create a recipient (D2D pattern)

```python
# Create a recipient (Databricks account recipient)
spark.sql("""
    CREATE RECIPIENT IF NOT EXISTS analytics_team_account
    USING ACCOUNT ID 'abcd-1234-efgh-5678'
""")

# Grant share access
spark.sql("""
    GRANT SELECT ON SHARE analytics_share
    TO RECIPIENT analytics_team_account
""")

# List recipients
recipients = spark.sql("SHOW RECIPIENTS")
recipients.display()
```

### 1c. Revoke access

```python
# Revoke access
spark.sql("""
    REVOKE SELECT ON SHARE analytics_share
    FROM RECIPIENT analytics_team_account
""")
```

**What to observe:** After revocation, the recipient can no longer see the shared tables in their Unity Catalog.

---

## Step 2 — Delta Sharing: Access Shared Tables (Recipient Side)

**Objective:** Understand how recipients access shared data.

### 2a. Recipient sees shared tables (D2D)

```python
# Recipient workspace: shared tables appear in a shared catalog
shared_catalogs = spark.sql("SHOW CATALOGS")
shared_catalogs.filter("catalogName LIKE '%shared%'").display()

# Access the shared table
shared_sales = spark.sql("SELECT * FROM shared_catalog.sales_data WHERE year = 2024")
shared_sales.display()

# Shared tables are read-only
# This will fail:
# spark.sql("DELETE FROM shared_catalog.sales_data WHERE year = 2020")
```

**What to observe:** Shared tables are read-only. No INSERT, UPDATE, DELETE, or MERGE operations are allowed on shared data.

---

## Step 3 — Lakehouse Federation: Create External Connection

**Objective:** Configure an external database connection in Unity Catalog.

### 3a. Create an external connection (syntax — requires cloud credentials)

```python
# Admin: Create a PostgreSQL connection
spark.sql("""
    CREATE CONNECTION IF NOT EXISTS analytics_postgres
    TYPE postgresql
    OPTIONS (
        host 'analytics-db.example.com',
        port '5432',
        database 'warehouse',
        user 'federated_user',
        password 'password'
    )
""")
```

**Note:** On Community Edition, this requires a real connection. In a production workspace, you would configure secrets for credentials.

### 3b. Create external schema

```python
# Admin: Create external schema from the connection
spark.sql("""
    CREATE EXTERNAL SCHEMA IF NOT EXISTS federated_postgres
    FROM postgresql
    CONNECTION analytics_postgres
    OPTIONS (schema 'public')
""")

# View external tables
external_tables = spark.sql("SHOW TABLES IN federated_postgres")
external_tables.display()
```

### 3c. Query federated table

```python
# User: Query live data from external database
federated_result = spark.sql("""
    SELECT customer_id, SUM(amount) as total_revenue
    FROM federated_postgres.orders
    WHERE status = 'completed'
    GROUP BY customer_id
""")
federated_result.display()
```

**What to observe:** The query runs against the external database. No data is copied to Databricks storage. Performance depends on the external database.

---

## Step 4 — Compare Patterns: Delta Sharing vs Federation

**Objective:** Choose the right pattern for a scenario.

### 4a. Scenario 1: Share analytics with a partner company

```
Requirement: Partner needs access to current sales data. They use Snowflake.
Solution: Delta Sharing (D2O via open protocol)
```

```python
# Provider: Create share and add open recipient type
spark.sql("""
    CREATE SHARE partner_analytics_share
    ADD TABLE prod.sales
""")

spark.sql("""
    CREATE RECIPIENT partner_company
    TYPE OPEN
""")

spark.sql("""
    GRANT SELECT ON SHARE partner_analytics_share
    TO RECIPIENT partner_company
""")

# Partner uses Delta Sharing REST API to access data
# GET /api/2.0/delta-sharing/shares/partner_analytics_share/tables
```

### 4b. Scenario 2: Query Snowflake data from Databricks for a report

```
Requirement: Analytics team needs to join Databricks data with Snowflake data.
Solution: Lakehouse Federation
```

```python
# Admin: Create Snowflake connection
spark.sql("""
    CREATE CONNECTION IF NOT EXISTS snowflake_conn
    TYPE snowflake
    OPTIONS (
        sfUrl 'account.snowflakecomputing.com',
        sfUser 'analytics_user',
        sfPassword 'password',
        sfDatabase 'ANALYTICS',
        sfSchema 'PUBLIC'
    )
""")

# Create external schema
spark.sql("""
    CREATE EXTERNAL SCHEMA sf_analytics
    FROM snowflake
    CONNECTION snowflake_conn
""")

# Join Databricks data with Snowflake data
report = spark.sql("""
    SELECT
        d.region,
        d.revenue AS databricks_revenue,
        s.revenue AS snowflake_revenue
    FROM prod.sales d
    JOIN sf_analytics.sales s ON d.order_id = s.order_id
    WHERE d.year = 2024
""")
report.display()
```

---

## Step 5 — Unity Catalog Governance on Federated Data

**Objective:** Apply Unity Catalog security to federated tables.

### 5a. Apply row filter to federated table

```python
# Admin: Create row filter on federated table
spark.sql("""
    CREATE ROW FILTER IF NOT EXISTS region_filter
    ON federated_postgres.orders
    WHERE region = current_user_region()
""")

spark.sql("GRANT SELECT ON federated_postgres.orders TO analyst_role")

# User with analyst_role only sees their region's data
result = spark.sql("SELECT * FROM federated_postgres.orders")
```

**What to observe:** Row filters work identically on federated and native Delta tables.

### 5b. Check system tables for usage

```python
# Query system table for sharing audit trail
sharing_audit = spark.sql("""
    SELECT * FROM system.default.access_logs
    WHERE action IN ('share_access', 'federated_query')
    ORDER BY timestamp DESC
    LIMIT 100
""")
sharing_audit.display()
```

**Note:** System tables are in the `system` schema of the system catalog (may vary by workspace configuration).

---

## Lab Checklist

- [ ] Created Delta Sharing share and added tables
- [ ] Created recipient and granted access (syntax)
- [ ] Understood read-only nature of shared tables
- [ ] Created external connection and schema for federation
- [ ] Queried federated table (syntax)
- [ ] Compared Delta Sharing (D2D/D2O) vs Lakehouse Federation
- [ ] Applied row filter to federated table
- [ ] Checked system tables for audit trail
