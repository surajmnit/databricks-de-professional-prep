# Day 20 — Delta Sharing and Lakehouse Federation

## Exam Objectives (Exam Guide, July 2026)

This day maps to **Section 4: Data Sharing and Federation (5%)**

> "Demonstrate delta sharing securely between Databricks deployments using Databricks to Databricks Sharing (D2D) or to external platforms using the open sharing protocol (D2O)."
> "Configure Lakehouse Federation with proper governance across the supported source systems."
> "Use Delta Share to share live data from Lakehouse to any computing platform."

---

## Part 1 — Delta Sharing Overview

### What Delta Sharing Is

Delta Sharing is a **secure, open protocol** for sharing data from a Delta Lake table with any computing platform — including non-Databricks systems. It enables:

- **Live data sharing**: recipients see up-to-date data without copying
- **Fine-grained access control**: share specific tables, not entire databases
- **No data duplication**: data remains in the provider's cloud account
- **Cross-platform**: Databricks-to-Databricks, Databricks-to-other

### Key Architecture

```
PROVIDER (Databricks workspace)
  Delta table in cloud storage (S3/ADLS/GCS)
  Delta Sharing Server (managed by Databricks)
  |
  | REST API / protocol
  |
RECIPIENT (any platform: Databricks, Snowflake, Amazon Redshift, etc.)
```

**Important:** The data never leaves the provider's cloud storage. Recipients access it through a secure protocol — they don't get a copy unless they explicitly load it.

### Delta Sharing vs Other Sharing Methods

| Method | Data Location | Real-time? | Cross-platform? |
|---|---|---|---|
| Delta Sharing | Provider's storage | Yes (live) | Yes |
| Deep Clone | Recipient's storage | No (snapshot) | No (Delta only) |
| CTAS | Recipient's storage | No (snapshot) | Limited |
| Spark DataFrame write | Recipient's storage | No (snapshot) | Limited |

---

## Part 2 — Databricks-to-Databricks Sharing (D2D)

### How D2D Works

When both sender and recipient are Databricks workspaces:

1. Provider creates a **share** (a named collection of tables)
2. Provider grants access to a **recipient** (another Databricks account/workspace)
3. Recipient accesses shared tables via Unity Catalog

### Key Concepts

**Share:** A named container holding tables and views to share. Created by provider.
```python
# Create a share
spark.sql("CREATE SHARE my_share")

# Add tables
spark.sql("ALTER SHARE my_share ADD TABLE prod.sales AS sales_data")

# Grant to recipient
spark.sql("CREATE RECIPIENT account_123")
spark.sql("GRANT SELECT ON SHARE my_share TO RECIPIENT account_123")
```

**Recipient:** An identity (Databricks account) that receives access. Defined by account ID or email.

**Shared table access:** Recipient sees shared tables in their Unity Catalog under a special shared catalog:
```
Recipient's Unity Catalog
  └── Shared_Catalog (auto-created by Delta Sharing)
       └── my_share
            └── sales_data (read-only view of provider's table)
```

### D2D Governance

- Provider can revoke access at any time
- Recipients see a read-only view — cannot modify shared data
- Shared tables are automatically visible in recipient's Unity Catalog
- Data access is audited via system tables on both sides

---

## Part 3 — Databricks-to-Other Sharing (D2O)

### How D2O Works

When the recipient is NOT on Databricks (Snowflake, Redshift, Power BI, etc.):

1. Provider creates a share with an **open sharing protocol** endpoint
2. Recipient uses the **Delta Sharing REST API** to access the data
3. Recipient can query live data or create a snapshot

### Open Sharing Protocol

Delta Sharing uses a REST API for non-Databricks recipients:

```bash
# Get share metadata
GET /api/2.0/delta-sharing/shares/{share_name}/tables

# Read table data
GET /api/2.0/delta-sharing/shares/{share_name}/tables/{table_name}/query
```

Recipients authenticate using a **personal access token** provided by the provider.

### Supported Recipient Platforms

AWS: Amazon Redshift, QuickSight, SageMaker, EMR, Snowflake
Azure: Synapse, Power BI, ADLS applications
GCP: BigQuery, Looker, Dataproc
Others: Any platform that can call REST APIs or read Parquet

### D2O vs D2D Comparison

| Aspect | D2D | D2O |
|---|---|---|
| Recipient Platform | Databricks only | Any platform |
| Access Method | Unity Catalog | REST API / open protocol |
| Authentication | Databricks account identity | Personal access token |
| Table Visibility | Auto-appears in Unity Catalog | Manual via REST or SDK |
| Real-time | Yes | Yes |

---

## Part 4 — Lakehouse Federation

### What Lakehouse Federation Is

Lakehouse Federation allows you to **query external databases from Databricks** using Unity Catalog — without moving data. It's a federated query capability:

```
Unity Catalog (Databricks)
  └── my_catalog
       └── external_schema (federated connection)
            ├── customers (external MySQL — live query)
            ├── orders (external PostgreSQL — live query)
            └── products (external Snowflake — live query)
```

### Supported Source Systems

| Source | Connection Type |
|---|---|
| MySQL | JDBC |
| PostgreSQL | JDBC |
| Amazon Redshift | JDBC |
| Snowflake | Native (Databricks-provided connector) |
| Google BigQuery | Native |
| Azure Synapse / SQL DW | JDBC |
| Databricks | Unity Catalog |

### How Federation Works

1. Admin creates an **external data connection** (credentials + endpoint)
2. Admin creates an **external schema** mapped to the connection
3. Users query the external tables using Spark SQL — no data movement

```python
# Admin: Create external connection
spark.sql("""
    CREATE CONNECTION IF NOT EXISTS my_postgres
    TYPE postgresql
    OPTIONS (
        host 'my-db.example.com',
        port '5432',
        database 'analytics',
        user 'federated_reader',
        password 'secret'
    )
""")

# Admin: Create external schema
spark.sql("""
    CREATE EXTERNAL SCHEMA fed_schema
    FROM postgresql
    CONNECTION my_postgres
    OPTIONS (schema 'public')
""")

# User: Query live data
result = spark.sql("SELECT * FROM fed_schema.customers LIMIT 10")
```

### Federation vs Ingestion

| | Federation | Ingestion |
|---|---|---|
| Data Location | Remains in source | Copied to Delta Lake |
| Freshness | Live (real-time) | Snapshot (stale between runs) |
| Cost | Query cost on source DB | Storage cost in cloud |
| Performance | Depends on source DB | Fast (local) |
| Governance | Via Unity Catalog | Full Unity Catalog |

**Use Federation when:** data is large/updated frequently; you don't need historical versions; cost of moving data is prohibitive.

**Use Ingestion when:** you need Delta Lake features (ACID, time travel, Z-Ordering); you need to join with other Databricks data; source is batch-oriented.

### Federation Governance

When Unity Catalog is enabled:
- External tables are registered in Unity Catalog
- ACLs apply to federated tables — row filters, column masks, column-level security all work
- Lineage is tracked for federated queries
- System tables capture federated query usage

```python
# Row filter on federated table works the same as Delta tables
spark.sql("""
    CREATE ROW FILTER ON fed_schema.customers
    WHERE region = current_user_region()
""")
```

---

## Part 5 — Comparing Delta Sharing vs Lakehouse Federation

| | Delta Sharing | Lakehouse Federation |
|---|---|---|
| Purpose | Share Delta tables externally | Query external databases from Databricks |
| Data Direction | Databricks → Outside | Outside → Databricks |
| Data Movement | None (live access) | None (federated query) |
| Unity Catalog | Yes (recipient sees shared tables) | Yes (external tables registered) |
| Supported Sources | Any platform (open protocol) | MySQL, PG, Redshift, Snowflake, BigQuery, etc. |
| Authentication | Databricks identity (D2D) or token (D2O) | JDBC / native connectors |
| Governance | ACLs, audit via system tables | ACLs, row filters, column masks |

**Key distinction:**
- **Delta Sharing:** Share your data with others
- **Lakehouse Federation:** Query others' data from your Databricks

---

## Part 6 — Security and Governance

### Delta Sharing Security

- **Encryption:** All data in transit is encrypted (TLS 1.2+)
- **Token-based auth:** Recipients authenticate via Databricks tokens (D2D) or personal access tokens (D2O)
- **Fine-grained shares:** Share specific tables, not entire databases
- **Revocable access:** Provider can revoke at any time
- **Audit logging:** All access logged in system tables

### Unity Catalog Integration

Both Delta Sharing and Lakehouse Federation integrate with Unity Catalog:

**Delta Sharing (provider side):**
- Shares are created and managed via Unity Catalog
- Permissions flow through UC ACLs
- System tables track sharing activity

**Lakehouse Federation:**
- External connections are registered in Unity Catalog
- External schemas and tables appear in UC
- Standard UC permissions apply

### Compliance Considerations

- **Data residency:** Data remains in provider's cloud account — compliance advantage
- **PII handling:** Row filters and column masks apply to shared and federated tables
- **Audit trails:** System tables capture all access events
- **GDPR:** Right to access/revoke is immediate via Delta Sharing revocation

---

## Glossary Updates

Add to glossary.md:

| Term | Definition | Day |
|---|---|---|
| Delta Sharing | Open protocol for sharing live Delta Lake data with any platform | 20 |
| D2D (Databricks-to-Databricks) | Delta Sharing between two Databricks workspaces | 20 |
| D2O (Databricks-to-Other) | Delta Sharing with non-Databricks platforms via open protocol | 20 |
| Share (Delta Sharing) | Named collection of tables to share with recipients | 20 |
| Recipient (Delta Sharing) | Identity granted access to a share | 20 |
| Lakehouse Federation | Query external databases from Databricks via Unity Catalog | 20 |
| External connection | Unity Catalog object with credentials to an external database | 20 |
| External schema | Unity Catalog schema mapped to an external database | 20 |
| Federated query | Query executed on source database, not Databricks storage | 20 |
