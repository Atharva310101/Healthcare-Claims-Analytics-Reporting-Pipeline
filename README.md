# 🏥 Healthcare Claims Analytics & Reporting Pipeline
**End-to-End Analysis of 26 Million Medicare Part D Prescription Claims**

## 📊 Project Overview
This project is an automated data engineering and analytics pipeline built to process, verify, and analyze large-scale healthcare claims data. It processes 4GB of raw Centers for Medicare & Medicaid Services (CMS) data to generate statistical reports, flag aberrant billing patterns, and facilitate ad hoc reporting for non-technical stakeholders.

**Key Technical Achievements:**
* **Big Data ETL:** Built a Python ingestion script using in-memory buffering and PostgreSQL's `COPY` engine to load **26.8 million records** in under 10 minutes.
* **Aberrant Statistics Detection:** Developed an automated SQL/Python verification layer that calculates Standard Deviations and Z-Scores to flag extreme cost outliers and data quality issues.
* **Automated Statistical Reporting:** Engineered a Python script that executes complex SQL aggregations to auto-generate styled Excel dashboards (complete with embedded Matplotlib visualizations).
* **DB2/SQL Optimization:** Wrote highly optimized SQL (functionally identical to DB2 ANSI SQL) utilizing Window Functions (`RANK`, `PERCENT_RANK`), CTEs, and strategic indexing.

---

## 🛠️ Tech Stack
* **Database Engine:** PostgreSQL 15 (Relational DB / DB2 equivalent)
* **ETL & Automation:** Python 3.11 (`pandas`, `psycopg2`, `io`, `tqdm`)
* **Analytics & Ad Hoc Reporting:** Pure SQL (`.sql` files)
* **Statistical Reporting:** `openpyxl`, `matplotlib`

---

## 📂 Repository Structure
```text
medicare-claims-analytics/
├── schema.sql                 # DDL for table creation and indexing
├── ingest.py                  # High-speed bulk ETL pipeline
├── verify.py                  # Automated Data Quality & Outlier audit
├── sql/
│   ├── 01_provider_summary.sql    # Basic statistical aggregation
│   ├── 02_state_ranking.sql       # Window functions for national ranks
│   ├── 03_drug_cohort_trends.sql  # Parameterized ad hoc query
│   └── 04_outlier_detection.sql   # Z-Score math for aberrant statistics
├── reports/
│   ├── generate_reports.py        # Automated Excel formatting script
│   └── medicare_claims_reports.xlsx # Output file
```
## Ad-Hoc Reporting Guide (For Non-Technical Staff)

One of the primary goals of this project is to allow operational staff to run ad
hoc reports independently without needing to write database code.

To run a custom analysis on specific regions or medical specialties, follow
these steps:

1.  Open the file: sql/03_drug_cohort_trends.sql
2.  Locate the WHERE clause.
3.  Change the state parameter (e.g., 'SC') and the specialty parameter (e.g.,
    'Internal Medicine') to your desired targets.
4.  Execute the script in your SQL client. The report will output the Top 10
    most expensive drugs prescribed by that specific cohort, ranked by total
    spend.

## Understanding "Aberrant Statistics"

This pipeline includes a specific module (sql/04_outlier_detection.sql) designed
to identify statistical anomalies indicative of billing errors, extreme
specialty drug usage, or potential fraud.

### Methodology:

1.  The script groups 26 million claims by NPI (Provider ID).
2.  It filters out low-volume noise (HAVING SUM(total_claims) > 100).
3.  It utilizes a Common Table Expression (CTE) to calculate the National Mean
    and Standard Deviation of Cost-Per-Claim.
4.  It calculates a exact Z-Score for every provider in the database.
5.  Providers are automatically flagged as a HIGH OUTLIER if their Z-Score > 3, or MODERATE OUTLIER if > 2.

Example Output: During the initial run focused on South Carolina, the system
successfully flagged Dr. Baratam (Hematology-Oncology) with a Z-Score of 27.03
due to an average claim cost of $14,998, heavily skewing the state's baseline
statistics.