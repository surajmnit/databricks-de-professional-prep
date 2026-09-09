# Day 20 — Quiz: Delta Sharing and Lakehouse Federation

**Objective coverage:** Section 4 (5%)

---

## Question 1

**Objective:** Understand Delta Sharing architecture.

A company wants to share live sales data with an external partner company that uses Amazon Redshift. The partner needs real-time access to the data. Data must not leave the company's cloud storage.

Which approach best meets these requirements?

A. Deep clone the table to the partner's storage
B. Delta Sharing using the open sharing protocol (D2O)
C. Lakehouse Federation from the partner's Redshift to Databricks
D. Export to S3 and grant the partner IAM access to the bucket

---

## Question 2

**Objective:** Distinguish D2D from D2O sharing.

A data engineer is setting up data sharing between two Databricks workspaces in the same organization.

Which statement is correct?

A. D2D uses the open sharing REST API; D2O uses Unity Catalog
B. D2D requires a Databricks account identity for the recipient; D2O uses a personal access token
C. D2D and D2O are the same mechanism with different names
D. D2D uses Unity Catalog for recipient identity; D2O uses the open REST protocol

---

## Question 3

**Objective:** Apply Lakehouse Federation correctly.

An analytics team needs to join Databricks customer data with a PostgreSQL database containing transaction history. The PostgreSQL data is large (500 GB) and updated in real time. The team wants to avoid copying data to Databricks.

Which solution is most appropriate?

A. Deep clone the PostgreSQL table to Databricks
B. Lakehouse Federation
C. Auto Loader ingestion to Delta Lake
D. Export to Parquet and mount via DBFS

---

## Question 4

**Objective:** Understand federated query performance.

A user runs a federated query against an external PostgreSQL database:

```python
result = spark.sql("SELECT * FROM fed_postgres.orders WHERE order_date > '2024-01-01'")
```

Which statement about this query is most accurate?

A. Data is copied to Databricks storage during query execution
B. The query executes entirely within the PostgreSQL database; only results are returned to Databricks
C. Spark reads all data from PostgreSQL into executor memory for processing
D. The query creates a Delta table in Databricks automatically

---

## Question 5

**Objective:** Understand Delta Sharing vs Lakehouse Federation direction.

A data platform team is architecting a data sharing solution.

Which describes the direction of data flow correctly?

A. Delta Sharing: Outside → Databricks. Lakehouse Federation: Databricks → Outside.
B. Delta Sharing: Databricks → Outside. Lakehouse Federation: Databricks → Outside.
C. Delta Sharing: Outside → Databricks. Lakehouse Federation: Outside → Databricks.
D. Both: Databricks → Outside.

---

## Question 6

**Objective:** Apply governance to federated tables.

A security team requires that users can only see data from their own region in a federated table that joins external PostgreSQL orders with Databricks customer data.

Which Unity Catalog feature applies to federated tables?

A. Column-level security only (row filters do not apply to external tables)
B. Row filters and column masks work identically on federated tables
C. Only Delta tables support row filters; federated tables do not
D. Governance must be enforced at the external database level

---

## Question 7

**Objective:** Configure Delta Sharing recipient types.

A data provider wants to share with two recipients: one on Databricks (internal team) and one on Snowflake (external partner).

Which recipient type should be created for each?

A. Both recipients: OPEN recipient type
B. Databricks recipient: account-based recipient. Snowflake recipient: OPEN recipient
C. Both recipients: account-based recipient
D. Databricks recipient: OPEN recipient. Snowflake recipient: account-based recipient

---

## Question 8

**Objective:** Understand Delta Sharing access control.

A recipient has SELECT access to a shared Delta table. The provider wants to revoke access immediately.

What happens when the provider revokes SELECT access on the share?

A. The shared table is deleted from the recipient's storage
B. The recipient can no longer see the table in their Unity Catalog; data remains on provider's storage
C. A copy of the data is automatically created in the recipient's account before revocation
D. The recipient can still read the data until the next batch refresh

---

## Question 9

**Objective:** Understand supported Lakehouse Federation sources.

Which external database system is NOT supported by Lakehouse Federation in Unity Catalog?

A. MySQL
B. Amazon Redshift
C. Microsoft Excel files
D. Snowflake

---

## Question 10

**Objective:** Apply the correct pattern for a scenario.

A financial services company needs to share regulatory reporting data with an external auditor. The auditor uses Power BI. Data must be live (real-time). The company wants full audit trails of who accessed what data.

Which solution meets all requirements?

A. Export to S3 and send credentials to auditor
B. Lakehouse Federation from auditor to Databricks
C. Delta Sharing with an OPEN recipient for Power BI access
D. Deep clone to a separate Databricks workspace for the auditor

---

## Answer Key

### Q1: B — Delta Sharing using the open sharing protocol (D2O)

Delta Sharing's open protocol allows any platform (including Redshift) to access live Delta tables via REST API. Data remains in the provider's storage. This is the only option that shares live data without copying.

**Why others are wrong:** A (deep clone) copies data to partner storage (not live). C (Lakehouse Federation) is for querying external databases FROM Databricks, not sharing FROM Databricks. D (S3 IAM) provides raw file access, not structured table access with governance.

---

### Q2: D — D2D uses Unity Catalog for recipient identity; D2O uses the open REST protocol

D2D sharing uses Databricks account identity (recipient is a Databricks account). D2O uses the open protocol with REST API and personal access tokens for non-Databricks recipients.

**Why others are wrong:** A reverses the technology assignments. B misstates the authentication mechanisms. C is incorrect — they are different mechanisms.

---

### Q3: B — Lakehouse Federation

Lakehouse Federation queries the PostgreSQL database in place — no data copy, live data, no storage cost. This is exactly the use case for federation.

**Why others are wrong:** A (deep clone) copies data (500 GB) — expensive and stale. C (Auto Loader) ingests to Delta — not live, adds storage cost. D (Parquet + DBFS) is not real-time.

---

### Q4: B — The query executes entirely within the PostgreSQL database; only results are returned to Databricks

Federated queries push computation to the source database. Spark sends the query to PostgreSQL, PostgreSQL executes it, and returns results. This is why performance depends on the source DB's capacity.

**Why others are wrong:** A (data copied) — no data is copied to Databricks storage. C (all data into memory) — only filtered results are returned. D (creates Delta table) — federation does not create Delta tables automatically.

---

### Q5: C — Delta Sharing: Outside → Databricks. Lakehouse Federation: Outside → Databricks.

This is a trick question. Both feature names use "to Databricks" or from "Databricks" in their descriptions but Delta Sharing is actually about sharing FROM Databricks (provider) TO outside (recipients). The correct relationship is:
- Delta Sharing: Databricks → Outside (share your data out)
- Lakehouse Federation: Outside → Databricks (query their data in)

**Why others are wrong:** Both A, B, and D have the direction wrong for at least one feature.

---

### Q6: B — Row filters and column masks work identically on federated tables

Unity Catalog governance applies uniformly — row filters, column masks, and column-level security work on federated tables the same as native Delta tables.

**Why others are wrong:** A (column security only) — row filters do apply. C (Delta only) — false. D (enforce at source) — UC enforces independently.

---

### Q7: B — Databricks recipient: account-based recipient. Snowflake recipient: OPEN recipient

Databricks workspace recipients use account identity (account-based recipient). External platforms (Snowflake) use the open protocol with an OPEN recipient type.

**Why others are wrong:** A (both OPEN) — OPEN is for non-Databricks platforms. C (both account-based) — Snowflake is not a Databricks workspace. D (reversed) — OPEN is for Snowflake, not Databricks.

---

### Q8: B — The recipient can no longer see the table in their Unity Catalog; data remains on provider's storage

Delta Sharing is access-based, not copy-based. Revoking SELECT removes access immediately. The data was never copied to the recipient's storage — it remains on the provider's cloud storage.

**Why others are wrong:** A (deleted from storage) — data is in provider's storage, not deleted. C (copy created) — no automatic copy. D (read until refresh) — access is immediate, not deferred.

---

### Q9: C — Microsoft Excel files

Lakehouse Federation supports structured databases via JDBC or native connectors: MySQL, PostgreSQL, Redshift, Snowflake, BigQuery, Azure Synapse. Excel is not a supported external database type.

**Why others are wrong:** A (MySQL), B (Redshift), D (Snowflake) are all supported federation sources.

---

### Q10: C — Delta Sharing with an OPEN recipient for Power BI access

Power BI is a non-Databricks platform, so OPEN recipient type is appropriate. Delta Sharing provides live access, audit trails (system tables), and full governance.

**Why others are wrong:** A (S3 IAM) lacks structured table access and governance. B (Lakehouse Federation) is for querying FROM Databricks, not sharing TO auditor. D (deep clone) is not live and creates a separate copy.

---

## Difficulty Ratings

| Q# | Difficulty | Topic |
|---|---|---|
| 1 | Medium | Delta Sharing architecture |
| 2 | Medium | D2D vs D2O distinction |
| 3 | Easy | Lakehouse Federation use case |
| 4 | Medium | Federated query performance |
| 5 | Hard | Direction of data flow |
| 6 | Medium | Governance on federated tables |
| 7 | Medium | Recipient type selection |
| 8 | Medium | Delta Sharing access revocation |
| 9 | Easy | Supported federation sources |
| 10 | Medium | Scenario-based pattern selection |
