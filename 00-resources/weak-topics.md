# Weak Topics Tracker — Databricks Certified Data Engineer Professional

> Updated after each day's quiz and each mock exam. Topics with repeated failures automatically escalate to higher priority.

## Priority Key

| Priority | Meaning |
|---|---|
| 🔴 Critical | Repeatedly wrong across multiple assessments — needs immediate remediation |
| 🟠 High | Missed on first attempt; understand the concept but made a reasoning error |
| 🟡 Medium | Partially correct; partial understanding or common trap pattern |
| 🟢 Low | Minor gap; confident overall |

## Master Weak Topics List

| Topic | Priority | First Wrong | Repeated? | Last Reviewed | Notes |
|---|---|---|---|---|---|
| Spark Driver vs Executor OOM | 🔴 | Day 1 Q3, Q6 | — | — | Need more practice distinguishing memory failure points |
| Spark Task vs Databricks Job Task | 🟠 | Day 1 Q2 | — | — | Scope difference not fully internalized |
| Coalesce cannot increase partitions | 🟠 | Day 1 Q5 | — | — | Common trap — confused with repartition |
| UC vs legacy cluster ACLs | 🟠 | Day 1 Q7 | — | — | Need to memorize: UC overrides cluster ACLs when enabled |
| Python UDF off-heap memory | 🟠 | Day 1 Q8 | — | — | Need to remember it's NOT in executor JVM heap |
| Shuffle → Stage boundary | 🟡 | Day 1 Q1 | — | — | Understand it but need more practice identifying from code |
| Trigger interval in streaming | 🟡 | Day 1 Q9 | — | — | Keyword recognition needs strengthening |
| spark.stage.maxAttempts | 🟡 | Day 1 Q10 | — | — | Stage retry vs task retry distinction not fully clear |

---

## Pattern Analysis (Updated After Each Assessment)

### Common Wrong-Answer Patterns

| Pattern | Description | Occurrences | Priority |
|---|---|---|---|
| Driver/Executor confusion | Choosing wrong node for OOM cause | 2+ so far | 🔴 |
| Terminology confusion | Using old name for renamed feature | 1 so far | 🟡 |
| Scope mismatch | Confusing Spark Task with Job Task | 1 so far | 🟠 |
| Coalesce vs Repartition | Assuming coalesce can increase partitions | 1 so far | 🟠 |

---

## Section-Level Confidence Tracker

> Rate 1–5 after completing each section's study day. 1 = no confidence, 5 = exam-ready.

| Section | Self-Rating | Target | Gap | Action |
|---|---|---|---|---|
| 1. Python + SQL Development (22%) | — | 5 | — | |
| 2. Data Ingestion (7%) | — | 4 | — | |
| 3. Data Transformation (10%) | — | 4 | — | |
| 4. Data Sharing & Federation (5%) | — | 3 | — | |
| 5. Monitoring & Alerting (10%) | — | 4 | — | |
| 6. Cost & Performance (13%) | — | 4 | — | |
| 7. Security & Compliance (10%) | — | 4 | — | |
| 8. Data Governance (7%) | — | 3 | — | |
| 9. Debugging & Deploying (10%) | — | 4 | — | |
| 10. Data Modeling (6%) | — | 3 | — | |

---

## Mock Exam Weak-Area Summary

### Mock Exam #1 (Day 27)

| Section | Questions Wrong | Topic Focus | Priority |
|---|---|---|---|
| (Pending) | — | — | — |

### Mock Exam #2 (Day 29)

| Section | Questions Wrong | Topic Focus | Priority |
|---|---|---|---|
| (Pending) | — | — | — |

---

## Micro-Drill Queue

> Questions generated for weak areas — complete these on weak-area remediation days.

| Topic | Questions in Queue | Status |
|---|---|---|
| Driver vs Executor OOM | 5 | Pending |
| UC permission inheritance | 3 | Pending |
| Spark shuffle behavior | 3 | Pending |
| Liquid Clustering vs Partitioning vs Z-Ordering | 3 | Pending |
| Delta Sharing vs Lakehouse Federation | 3 | Pending |

---

## How to Use This Tracker

1. **After each quiz:** Update the "First Wrong" column with the question number
2. **If repeated:** Mark "Repeated? = Yes" — auto-escalates priority
3. **After mock exams:** Update section-level confidence and mock exam tables
4. **Before remediation days:** Generate micro-drills for all topics rated priority ≥ High
5. **The night before the exam:** Review only 🔴 Critical topics from this file


---

## Day 2 Quiz Analysis

| Q# | Topic | Priority | Key Insight |
|---|---|---|---|
| Q1 | Python import in Workspace | High | Files in /Workspace need pip install or sys.path - not automatic |
| Q2 | pip install scope | Critical | Driver only; UDFs need cluster-level install |
| Q3 | Pandas UDF GROUPED_MAP | Medium | Per-group ops need GROUPED_MAP type annotation |
| Q4 | DAB targets pattern | Medium | Single databricks.yml, multiple targets - not multiple YAML files |
| Q5 | DBR bundled package shadowing | Medium | DBR system packages can shadow pip-installed packages |
| Q6 | DAB --force vs --overwrite | Medium | The flag is --force, not --overwrite |
| Q7 | Pandas UDF return type | Medium | Use StringType (class), NOT StringType() (instance) |
| Q8 | run vs import | Medium | run merges global scope; proper packages prevent namespace conflicts |
| Q9 | DAB path validation | Medium | Bundle deploy writes path as-is; no existence check |
| Q10 | UDF optimization | Medium | SQL > Pandas UDF > Python UDF; always check if SQL suffices first |
