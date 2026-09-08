# Databricks Certified Data Engineer Professional — Study Program

**Exam Date:** Day 31 | **Exam Version:** July 3, 2026 | **Questions:** 59 MCQ | **Time:** 120 min

---

## About This Program

This is a 30-day structured study program for the Databricks Certified Data Engineer Professional exam.
It is designed to complement the Udemy course "Databricks Certified Data Engineer Professional - Preparation" by Derar Alhussein, not replace it.

Each day covers specific exam objectives from the official July 3, 2026 Exam Guide with:
- Deep-dive reference notes mapped to exact objective bullets
- Hands-on labs runnable on Databricks Community Edition or a paid workspace
- 10–15 scenario-based quiz questions
- A one-page cheat sheet for last-week review

---

## 30-Day Schedule

| Day | Topic | Key Focus Areas |
|---|---|---|
| 1 | Exam Strategy + Architecture + Spark Basics | Exam format, triage, platform overview |
| 2 | Python Dev + Declarative Automation Bundles | Project structure, dependencies, UDFs |
| 3 | SQL Transformations + Testing | assertDataFrameEqual, transform, testing frameworks |
| 4 | Spark Architecture Deep-Dive | Driver, executor, worker, cluster manager |
| 5 | Spark Execution: DAG, Stages, Tasks | Lazy eval, wide vs narrow, action vs transform |
| 6 | Spark Shuffle | Shuffle read/write, partitioners, spill |
| 7 | Spark Memory + OOM | Executor memory, GC, spill, Driver vs Executor OOM |
| 8 | Spark UI + Query Profile | Bottleneck identification, AQE, skew joins |
| 9 | Delta Lake Fundamentals | Transaction log, ACID, Z-Ordering, partitioning |
| 10 | MERGE, CDC, SCD + CDF | MERGE/UPSERT, Change Data Feed, CDC patterns |
| 11 | Delta Optimization | Deletion vectors, Liquid Clustering, data skipping |
| 12 | Data Ingestion Formats | Parquet, ORC, Avro, JSON, CSV, XML, Binary |
| 13 | Auto Loader | Configuration, schema evolution, quarantine |
| 14 | Streaming Foundations | Structured Streaming, watermarking, state |
| 15 | Lakeflow Declarative Pipelines | Expectations, control flow, AUTO CDC |
| 16 | Streaming Tables vs Materialized Views | Comparison, use cases, latency trade-offs |
| 17 | Data Transformation + Cleansing | Window functions, joins, quarantine pattern |
| 18 | Unity Catalog ACLs + Permissions | Workspace security, permission inheritance |
| 19 | Security + Compliance + PII | Row filters, column masks, anonymization, purging |
| 20 | Delta Sharing + Lakehouse Federation | D2D, D2O, open protocol, federation governance |
| 21 | Monitoring: System Tables | Observability, cost, auditing |
| 22 | Alerting | SQL Alerts, Lakeflow notifications, performance alerts |
| 23 | Debugging + Troubleshooting | Spark UI, cluster logs, query profiles, job repair |
| 24 | Jobs API + REST + CLI | Parameter overrides, pipeline failure remediation |
| 25 | Declarative Automation Bundles + CI/CD | Git folders, environment promotion, DABs |
| 26 | Data Modeling | Dimensional models, fact/dim tables, analytical workloads |
| 27 | **Mock Exam #1** | Full 59-question simulation |
| 28 | Weak-Area Remediation | Targeted micro-drills based on Mock Exam #1 |
| 29 | **Mock Exam #2** | Full 59-question simulation |
| 30 | Final Revision | Cheat sheets only + light drilling |
| **31** | **EXAM DAY** | |

---

## Exam Section Weights

| Section | Weight |
|---|---|
| Section 1: Developing Code for Data Processing (Python + SQL) | 22% |
| Section 2: Data Ingestion & Acquisition | 7% |
| Section 3: Data Transformation, Cleansing, and Quality | 10% |
| Section 4: Data Sharing and Federation | 5% |
| Section 5: Monitoring and Alerting | 10% |
| Section 6: Cost & Performance Optimization | 13% |
| Section 7: Ensuring Data Security and Compliance | 10% |
| Section 8: Data Governance | 7% |
| Section 9: Debugging and Deploying | 10% |
| Section 10: Data Modeling | 6% |

---

## Progress Tracker

- [ ] Day 1 — Exam Strategy + Architecture + Spark Basics
- [x] Day 2 — Python Dev + Declarative Automation Bundles
- [x] Day 3 — SQL Transformations + Testing
- [x] Day 4 — Spark Architecture Deep-Dive
- [x] Day 5 — Spark Execution: DAG, Stages, Tasks
- [x] Day 6 — Spark Shuffle
- [x] Day 7 — Spark Memory + OOM
- [ ] Day 8 — Spark UI + Query Profile
- [ ] Day 9 — Delta Lake Fundamentals
- [ ] Day 10 — MERGE, CDC, SCD + CDF
- [ ] Day 11 — Delta Optimization
- [ ] Day 12 — Data Ingestion Formats
- [ ] Day 13 — Auto Loader
- [ ] Day 14 — Streaming Foundations
- [ ] Day 15 — Lakeflow Declarative Pipelines
- [ ] Day 16 — Streaming Tables vs Materialized Views
- [ ] Day 17 — Data Transformation + Cleansing
- [ ] Day 18 — Unity Catalog ACLs + Permissions
- [ ] Day 19 — Security + Compliance + PII
- [x] Day 20 — Delta Sharing + Lakehouse Federation
- [ ] Day 21 — Monitoring: System Tables
- [ ] Day 22 — Alerting
- [ ] Day 23 — Debugging + Troubleshooting
- [ ] Day 24 — Jobs API + REST + CLI
- [ ] Day 25 — Declarative Automation Bundles + CI/CD
- [ ] Day 26 — Data Modeling
- [ ] Day 27 — Mock Exam #1
- [ ] Day 28 — Weak-Area Remediation
- [ ] Day 29 — Mock Exam #2
- [ ] Day 30 — Final Revision

---

## Resources

- [Official Exam Guide (July 3, 2026)](00-resources/exam-guide.md)
- [Terminology Glossary](00-resources/glossary.md)
- [Study Tracker](00-resources/study-tracker.md)
- [Weak Topics Tracker](00-resources/weak-topics.md)

---

## Environment Setup

- **Primary:** Databricks paid workspace (recommended for full feature access)
- **Alternative:** Databricks Community Edition (limited to DBR versions, no serverless, no Unity Catalog on CE)
- **Code:** PySpark (Python), Spark SQL, YAML (for DABs), Databricks CLI

## Exam Tips

1. No test aids allowed — no API docs, no external references
2. Flag questions you second-guess and return only if time permits
3. Read code literally — don't infer intent from context
4. Watch for terminology remappings: DLT → Lakeflow, DABs → DABs (still), APPLY CHANGES → AUTO CDC, Repos → Git Folders
5. The passing score is not published; aim for consistent performance across all sections
