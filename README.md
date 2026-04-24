# Medicare Part D Claims Analytics & Aberrant Statistics Pipeline

> **Real-world healthcare claims analytics pipeline built on 26.8 million CMS Medicare records.** Covers end-to-end ETL, SQL statistical reporting, automated data verification, Z-score aberrant statistics detection, and Excel reporting — directly mirroring the responsibilities of a healthcare Data Analyst II role.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Dataset](#2-dataset)
3. [Tech Stack](#3-tech-stack)
4. [Project Structure](#4-project-structure)
5. [Quickstart — Run the Full Pipeline](#5-quickstart--run-the-full-pipeline)
6. [Module Details](#6-module-details)
   - [ETL Ingestion](#61-etl-ingestion)
   - [SQL Analytics Layer](#62-sql-analytics-layer)
   - [Aberrant Statistics Engine](#63-aberrant-statistics-engine)
   - [Data Verification (DQ Layer)](#64-data-verification-dq-layer)
   - [Statistical Reporting](#65-statistical-reporting)
7. [Ad Hoc Reporting Guide (Non-Technical)](#7-ad-hoc-reporting-guide-non-technical)
8. [Key Business Findings](#8-key-business-findings)
9. [Performance & Optimization Notes](#9-performance--optimization-notes)
10. [DB2 Compatibility Note](#10-db2-compatibility-note)
11. [Reproducing This Project](#11-reproducing-this-project)

---

## 1. Project Overview

This pipeline ingests, verifies, analyzes, and reports on the full CMS Medicare Part D Prescriber dataset — 26.8 million rows of real provider, drug, and payment data. It was built to demonstrate production-grade healthcare data analytics skills including:

- High-speed bulk ETL using PostgreSQL's native `COPY` engine
- Advanced SQL analytics (CTEs, window functions, parameterized ad hoc queries)
- Automated Z-score aberrant statistics detection flagging extreme billing outliers
- A six-check Python data quality verification layer with timestamped audit output
- Multi-tab Excel statistical reports with embedded charts for stakeholder delivery

**Most notable finding:** A South Carolina Hematology-Oncology provider with an average cost-per-claim of **$14,998** and a **Z-score of 27.03** — 27 standard deviations above the national mean, measurably distorting state-level cost baselines.

---

## 2. Dataset

| Property | Value |
|---|---|
| **Source** | Centers for Medicare & Medicaid Services (CMS) |
| **Dataset name** | Medicare Part D Prescribers by Provider and Drug (2023) |
| **Download URL** | https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers/medicare-part-d-prescribers-by-provider-and-drug |
| **Format** | CSV (no account required) |
| **File size** | ~4 GB |
| **Row count** | 26,794,878 |
| **Table grain** | 1 row = 1 unique Provider × Drug combination |

**Key columns used:**

| Raw CMS Column | Mapped Name | Description |
|---|---|---|
| `Prscrbr_NPI` | `npi` | Provider identifier |
| `Prscrbr_Last_Org_Name` | `provider_name` | Provider or organization name |
| `Prscrbr_City` | `city` | Provider city |
| `Prscrbr_State_Abrvtn` | `state` | 2-letter state code |
| `Prscrbr_Type` | `specialty` | Medical specialty (e.g. Internal Medicine) |
| `Brnd_Name` | `brand_name` | Brand drug name |
| `Gnrc_Name` | `generic_name` | Generic drug name |
| `Tot_Clms` | `total_claims` | Total claims submitted |
| `Tot_Drug_Cst` | `total_drug_cost` | Total drug cost ($) |
| `Tot_Benes` | `total_benes` | Total beneficiaries served |

---

## 3. Tech Stack

| Layer | Technology |
|---|---|
| Database | PostgreSQL 15 |
| Language | Python 3.11 |
| ETL | psycopg2, pandas, io.StringIO, tqdm |
| SQL | Pure `.sql` files — CTEs, window functions, STDDEV, RANK, PERCENT_RANK |
| Reporting | openpyxl, Matplotlib |
| Data Quality | psycopg2, pandas, csv output |

---

## 4. Project Structure

```
medicare-claims-analytics/
│
├── README.md                        ← You are here
├── requirements.txt
├── schema.sql                       ← Database schema + index definitions
│
├── ingest.py                        ← High-speed COPY-based ETL pipeline
├── verify.py                        ← Automated 6-check DQ verification script
│
├── sql/
│   ├── 01_provider_summary.sql      ← Provider-level aggregation
│   ├── 02_state_ranking.sql         ← State spend with window function ranking
│   ├── 03_drug_cohort_trends.sql    ← Parameterized ad hoc cohort query
│   └── 04_outlier_detection.sql     ← Z-score aberrant statistics engine
│
├── reports/
│   ├── generate_reports.py          ← Auto-generates Excel workbook
│   └── medicare_claims_reports.xlsx ← Output: 3-tab statistical report
│
└── verification_reports/
    └── verification_report_*.csv    ← Timestamped DQ audit outputs
```

---

## 5. Quickstart — Run the Full Pipeline

### Prerequisites

```bash
# Python dependencies
pip install pandas psycopg2-binary tqdm openpyxl matplotlib

# PostgreSQL must be running locally
# Mac:   brew install postgresql@15 && brew services start postgresql@15
# Linux: sudo apt install postgresql-15 && sudo service postgresql start

# Create the database
createdb medicare_claims
```

### Step 1 — Create the schema

```bash
psql medicare_claims -f schema.sql
```

### Step 2 — Download the dataset

Go to: https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers/medicare-part-d-prescribers-by-provider-and-drug

Download the 2023 CSV. Place it in the project root directory.

### Step 3 — Run ingestion

```bash
python ingest.py
```

Expected output:
```
2025-04-23 10:00:01 Loaded 500,000 rows...
2025-04-23 10:00:04 Loaded 1,000,000 rows...
...
2025-04-23 10:08:11 Ingestion complete. 26,794,878 rows loaded in 8m 11s.
```

### Step 4 — Run data verification

```bash
python verify.py
```

Expected output:
```
[PASS] null_npi: 0 records
[PASS] null_total_claims: 0 records
[PASS] negative_cost: 0 records
[PASS] duplicate_npi_drug: 0 records
[PASS] extreme_cost_outliers: 42 records
[PASS] invalid_state_code: 0 records

Verification report written to: verification_reports/verification_report_20250423_1009.csv
```

### Step 5 — Run SQL analytics

Open any `.sql` file in `psql` or a GUI tool (DBeaver, pgAdmin):

```bash
psql medicare_claims -f sql/04_outlier_detection.sql
```

### Step 6 — Generate statistical reports

```bash
python reports/generate_reports.py
```

Output: `reports/medicare_claims_reports.xlsx` — 3-tab Excel workbook with embedded charts.

---

## 6. Module Details

### 6.1 ETL Ingestion

**File:** `ingest.py`

**The problem with standard INSERT:** Row-by-row or chunk-based `INSERT` statements carry full transactional overhead for each operation. At 26.8M rows, this projects to 90+ minutes.

**The solution — PostgreSQL COPY engine:**

The script uses `copy_expert()` via psycopg2 to stream each DataFrame chunk as an in-memory CSV buffer directly into PostgreSQL's bulk loader. This bypasses transaction overhead entirely.

```python
buffer = io.StringIO()
chunk.to_csv(buffer, index=False, header=False)
buffer.seek(0)
cur.copy_expert(
    "COPY claims.part_d_prescribers (...) FROM STDIN WITH CSV",
    buffer
)
```

**Key design decisions:**

| Decision | Reason |
|---|---|
| `io.StringIO` buffer | No temp files on disk; memory-efficient |
| Indexes dropped pre-load | B-tree rebalancing on every insert slows bulk loads significantly |
| Indexes rebuilt post-load | Single-pass index build is much faster than incremental updates |
| `tqdm` progress bar | Real-time visibility into load progress |
| `pd.to_numeric(errors='coerce')` | Dirty numeric strings become NULL rather than crashing the load |

**Result:** 26.8M rows loaded in **8 minutes** (≈11x faster than INSERT approach).

---

### 6.2 SQL Analytics Layer

**Files:** `sql/01_provider_summary.sql` through `sql/03_drug_cohort_trends.sql`

**Techniques used:**

```sql
-- Window function example from sql/02_state_ranking.sql
SELECT
    state,
    spend,
    RANK()         OVER (ORDER BY spend DESC)        AS spend_rank,
    PERCENT_RANK() OVER (ORDER BY spend DESC)        AS spend_percentile,
    spend / SUM(spend) OVER () * 100                 AS pct_of_national_spend
FROM state_totals
```

All queries use ANSI SQL-92 standard syntax compatible with PostgreSQL, IBM DB2, and most enterprise RDBMS platforms.

---

### 6.3 Aberrant Statistics Engine

**File:** `sql/04_outlier_detection.sql`

**Methodology:** Z-score standardization applied at the provider level.

```
z = (provider_cost_per_claim − national_mean_cpc) / national_stddev_cpc
```

**Implementation:**

```sql
WITH provider_stats AS (
    SELECT npi, provider_name, state, specialty,
           SUM(total_claims) AS claims,
           ROUND(SUM(total_drug_cost) / NULLIF(SUM(total_claims), 0), 2) AS cost_per_claim
    FROM claims.part_d_prescribers
    GROUP BY npi, provider_name, state, specialty
    HAVING SUM(total_claims) > 100   -- exclude low-volume noise
),
distribution AS (
    SELECT AVG(cost_per_claim) AS mean_cpc,
           STDDEV(cost_per_claim) AS std_cpc
    FROM provider_stats
)
SELECT p.*,
       ROUND((p.cost_per_claim - d.mean_cpc) / NULLIF(d.std_cpc, 0), 3) AS z_score,
       CASE
           WHEN ABS((...) / NULLIF(d.std_cpc, 0)) > 3 THEN 'HIGH OUTLIER'
           WHEN ABS((...) / NULLIF(d.std_cpc, 0)) > 2 THEN 'MODERATE OUTLIER'
           ELSE 'NORMAL'
       END AS aberrant_flag
FROM provider_stats p, distribution d
WHERE ABS((p.cost_per_claim - d.mean_cpc) / NULLIF(d.std_cpc, 0)) > 2
ORDER BY z_score DESC;
```

**Why `HAVING SUM(total_claims) > 100`:** A provider with 5 claims and one expensive drug would generate a high Z-score that isn't statistically meaningful. The threshold filters low-volume noise before the distribution is computed.

**Results:**

| Metric | Value |
|---|---|
| Total records flagged (|z| > 2) | 108,587 |
| As % of full dataset | 0.4% |
| Most extreme provider Z-score | 27.03 |
| That provider's avg cost-per-claim | $14,998 |
| Provider specialty | Hematology-Oncology |
| Provider state | South Carolina |

---

### 6.4 Data Verification (DQ Layer)

**File:** `verify.py`

Six automated checks run against the full 26.8M-row table:

| Check Name | What It Catches | Fail Threshold |
|---|---|---|
| `null_npi` | Providers with no identifier | 0 |
| `null_total_claims` | Records with missing claim counts | 100 |
| `negative_cost` | Billing entries with cost < $0 | 0 |
| `duplicate_npi_drug` | Same provider + drug appearing more than once | 0 |
| `extreme_cost_outliers` | Records where cost > mean + 4σ | 50 |
| `invalid_state_code` | State field not exactly 2 uppercase letters | 0 |

Output is a timestamped CSV with columns: `check, count, threshold, status, run_at`.

Designed to run as a scheduled job (cron or Airflow) after each data load.

---

### 6.5 Statistical Reporting

**File:** `reports/generate_reports.py`
**Output:** `reports/medicare_claims_reports.xlsx`

**Tab 1 — State Spend Summary**
Top 20 states by total Medicare Part D drug spend. Includes embedded Matplotlib horizontal bar chart (top 15 states). Styled with blue headers, auto column widths.

**Tab 2 — Top Drugs National**
Top 20 drugs by national spend with prescriber count and average cost-per-claim.

**Tab 3 — Aberrant Provider Monitor**
Top 100 providers sorted by Z-score magnitude, with HIGH/MODERATE severity flags. Designed to mirror a claims audit dashboard.

---

## 7. Ad Hoc Reporting Guide (Non-Technical)

> **This section is for analysts who want to run custom queries without writing code.**

You need: [DBeaver](https://dbeaver.io) (free GUI tool) connected to the local PostgreSQL database.

### "I want to see all drug spend for a specific state and specialty"

Open `sql/03_drug_cohort_trends.sql` in DBeaver. Find these two lines near the top:

```sql
WHERE state     = :state_param      -- change this
  AND specialty = :specialty_param  -- change this
```

Replace the placeholder values. Examples:

| What you want | state_param | specialty_param |
|---|---|---|
| California Internal Medicine | `'CA'` | `'Internal Medicine'` |
| Texas Oncology | `'TX'` | `'Hematology/Oncology'` |
| New York Cardiology | `'NY'` | `'Cardiology'` |

Press **Run**. The query returns the top 10 drugs by spend for that provider cohort.

### "I want to see the highest-cost providers nationally"

Run `sql/04_outlier_detection.sql` without modification. It returns all providers with Z-score above 2, sorted by severity. The `aberrant_flag` column tells you whether each is HIGH or MODERATE.

### "I want to see state-level spend rankings"

Run `sql/02_state_ranking.sql`. It returns all states ranked by total spend with their national percentile. No parameters needed.

### "I want to regenerate the Excel reports"

```bash
python reports/generate_reports.py
```

The file `reports/medicare_claims_reports.xlsx` is overwritten with fresh data. Open it in Excel.

---

## 8. Key Business Findings

These findings emerged from the pipeline running against real 2023 CMS data:

| Finding | Value | Significance |
|---|---|---|
| #1 drug by national spend | **Apixaban (Eliquis)** — $17.3B | Blood thinner dominance; expected given aging population |
| #2 drug by national spend | **Semaglutide (Ozempic/Wegovy)** — $9.6B | GLP-1 surge visible in real claims data |
| Highest Z-score provider | SC Hematology-Oncology — Z=27.03 | 27σ above mean; distorting state baselines |
| That provider's avg cost/claim | **$14,998** | Consistent with high-cost oncology biologics |
| Total aberrant records flagged | **108,587** (0.4% of dataset) | Manageable audit volume; statistically meaningful threshold |

---

## 9. Performance & Optimization Notes

### ETL: INSERT vs COPY

| Method | Projected Time | Actual Time |
|---|---|---|
| pandas chunk INSERT | ~90 minutes | — (abandoned) |
| PostgreSQL COPY engine | — | **8 minutes** |

### Query optimization: COUNT(DISTINCT) on large tables

During report generation, a `COUNT(DISTINCT npi)` inside a `GROUP BY generic_name` query across 26M rows caused the query to hang.

**Root cause:** At the Provider × Drug grain, counting distinct NPIs within a drug group forces a full sort + deduplication pass — O(n log n) across 26M rows.

**Fix:** Removed `DISTINCT` at the drug-level tab where the grain already guarantees uniqueness per drug group.

**Result:** 10x reduction in report generation time.

---

## 10. DB2 Compatibility Note

All `.sql` files use ANSI SQL-92 standard syntax. The following constructs used in this project are **fully supported in IBM DB2**:

- `WITH` clauses (CTEs)
- `RANK()`, `PERCENT_RANK()` window functions
- `STDDEV()`, `AVG()` aggregate functions
- `HAVING` clauses on aggregates
- `NULLIF()` for safe division
- `CASE WHEN` conditional logic
- `NUMERIC` data types

**DB2-specific difference:** The bulk ingestion step uses PostgreSQL's `COPY` command. The IBM DB2 equivalent is the `LOAD` utility or `IMPORT` command. The Python ingestion script would be updated to use `ibm_db` driver with equivalent bulk transfer logic.

---

## 11. Reproducing This Project

```bash
# 1. Clone
git clone https://github.com/atharva310101/medicare-claims-analytics
cd medicare-claims-analytics

# 2. Install dependencies
pip install -r requirements.txt

# 3. Set up database
createdb medicare_claims
psql medicare_claims -f schema.sql

# 4. Download CMS data (link in Section 2 above)
#    Place CSV in project root

# 5. Run full pipeline
python ingest.py
python verify.py
python reports/generate_reports.py

# 6. Run SQL analytics
psql medicare_claims -f sql/04_outlier_detection.sql
```

**Total runtime from fresh start:** ~15 minutes (8 min ingestion + 5 min reports + 2 min verification).

---

*Dataset: CMS Medicare Part D Prescribers by Provider and Drug, 2023. Public domain, no restrictions on use.*