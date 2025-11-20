
**NYC Taxi Data Pipeline: Modular ETL with Medallion Architecture**

This project implements a scalable ETL pipeline for NYC taxi trip data using the **Medallion Architecture** (**Bronze → Silver → Gold**). It leverages **PostgreSQL** for storage, modular Python code for maintainability, and automated CI/CD workflows for reliability.

The pipeline is designed to be **idempotent** and **fault-tolerant**, capable of handling incremental monthly loads with automated state tracking. This is an evolution of a previous project where incremental loading was implemented purely with **SQL triggers** and **idempotency checks** to avoid duplicate data.

1. **Medallion Layers**
Layer	Role	Description
**Bronze**	Raw Ingestion	Immutable historical archive. Ingests raw Parquet files via fast bulk loading (**COPY**).
**Silver**	Cleaned & Enriched	Single Source of Truth. Deduplicates records, enforces schema types, and adds derived metrics (e.g., **trip_duration**).
**Gold**	Business Aggregates	BI-ready data. Contains dimensional models (**vendor_summary**, **zone_summary**) optimized for tools like **Power BI**.

2. **Repository Structure**
pythonnyc/
├── .github/
│   └── workflows/
│       └── ci.yml          # CI/CD workflow definition
├── .flake8                  # Linting configuration
├── .gitignore
├── **config.py**               # DB connection and basic project configuration
├── **main.py**                 # Orchestrator for the ETL pipeline
├── **queries.py**              # SQL queries to create tables
├── **requirements.txt**        # Python dependencies
├── **tasks.py**                # Core ETL logic (Extract, Transform, Load)
├── **test_etl_pipeline.py**    # Unit and integration tests
└── **utils.py**                # Logging, retry decorator, and helpers

3. **Orchestration & Retry Mechanism**
How is works.

**Workflow Logic:**

**Read State**: Retrieve the **last_successful_load_month** from **etl.pipeline_metadata**.

**Determine Next Month**: Calculate which month to process next (e.g., if Jan is done, queue Feb).

**Verify & Run**: Check for the local Parquet file and insert the file into the  **Bronze table → Silver → Gold**.

**Update Metadata**: Log run status (**SUCCESS** or **FAILED**) with timestamp.

**Fault Tolerance:**

A custom Python decorator **@with_retry** (in **utils.py**) wraps critical DB operations.

**Retries**: Up to **MAX_RETRIES** (configurable).

**Strategy**: Exponential backoff or fixed delay.

**Observability**: Logs full stack traces to the metadata table upon final failure.

4. **Loading Strategy**

**Bronze & Silver (Incremental):**

Processes only new monthly files based on the metadata high-water mark.

**Gold (Upsert & Re-aggregation):**

Uses **INSERT ... ON CONFLICT DO UPDATE** for dimensional aggregates.

daily_summary tables load incrementally as partitions are independent.

5. **CI/CD Pipeline (GitHub Actions)**

Automated quality gates ensure that bad code never reaches production.

**Triggers**: Push or Pull Request.

**Pipeline Steps:**

**Environment Setup**: Ubuntu runner initializes a temporary **PostgreSQL** container.

**Linting**: Runs **flake8** to enforce **PEP-8** style.

**Testing:**

**Unit Tests**: Mock database connections to validate transformation logic.

**Integration Tests**: Run against the live service container to ensure end-to-end SQL execution works correctly.

6. **Analytics Examples**

The **Gold** layer is optimized for analytical queries.

**Monthly Revenue Trend:**

```sql
SELECT 
    to_char(trip_date, 'YYYY-MM') AS month,
    SUM(total_revenue) AS revenue,
    SUM(total_trips) AS trips
FROM gold.daily_summary
GROUP BY 1
ORDER BY 1;
```

**Vendor Efficiency Performance:**

```sql
SELECT 
    vendor_name,
    avg_fare,
    avg_trip_distance,
    (total_revenue / NULLIF(total_distance, 0)) AS revenue_per_mile
FROM gold.vendor_summary
ORDER BY total_revenue DESC;
```
**LOGGING OUTPUT**
<img width="960" height="540" alt="AINCRE" src="https://github.com/user-attachments/assets/ea88c0a7-1f71-4252-9c9c-2096af5054aa" />



Also used the gold tables to create a dashboard 
<img width="739" height="427" alt="nyc_dashboard" src="https://github.com/user-attachments/assets/17b53a64-f27d-4b32-959e-544508a466b3" />

