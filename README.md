# 🚕 NYC Taxi Data Pipeline — Python + PostgreSQL (Medallion Architecture)
<img width="670" height="387" alt="arc de" src="https://github.com/user-attachments/assets/055cb856-3120-442f-8ca7-deff53adf4e3" />

This project is a fully-modular data pipeline that processes NYC Taxi trip data using Python and PostgreSQL.  
It follows the Medallion Architecture (Bronze → Silver → Gold) and is built with real-world engineering principles in mind: incremental loading, metadata tracking, fault tolerance, testing, and CI/CD automation.

---


### **1. Medallion Architecture**
Breaking the pipeline into Bronze, Silver, and Gold layers helps keep raw data separate from cleaned and aggregated data.  
Each layer has a clear purpose:
- **Bronze** → raw, untouched data  
- **Silver** → cleaned, validated, feature-engineered data  
- **Gold** → final business summaries  

This makes debugging easier, transformations more transparent, and analytics more reliable.

### **2. Modular Python Structure**
the code are split into 4 parts
- `tasks.py` contains the ETL steps  
- `queries.py` stores all SQL  
- `utils.py` handles helpers and retry logic  
- `main.py` runs the pipeline  

### **3. Metadata-Driven Loading**
The pipeline keeps track of the last processed month.  
This ensures:
- Only new data is loaded  
- Failed runs can resume safely  
- The pipeline is idempotent (running it twice won’t duplicate data)

### **4. CI/CD and Testing**
A GitHub Actions workflow runs tests and linting automatically to keep the project stable and production-ready.

---

## 🏛️ Architecture Overview

### **Bronze Layer — Raw Data**
The raw Parquet data is loaded into PostgreSQL with minimal changes.  
The Bronze layer acts as the ground truth of the pipeline: everything else depends on it.

### **Silver Layer — Cleaned Data**
The Silver layer standardizes and enriches the data:
- duplicates removed  
- invalid rows handled  
- timestamps normalized  
- new columns added (e.g., trip duration, speed)

### **Gold Layer — Business Tables**
The Gold layer contains aggregated results such as:
- daily revenue  
- vendor performance  
- monthly trends  
- payment behavior  
- pickup zones  

These tables are designed for dashboards and BI tools and this an example built with the result in PowerBi.
<img width="1920" height="1080" alt="Screenshot 2025-11-20 213735" src="https://github.com/user-attachments/assets/afd2bd96-fde0-417e-b332-c5bb1898ebd9" />



## ⚙️ How the Pipeline Runs (Orchestration Logic)

The orchestrator in `main.py` follows a simple flow:

1. **Check metadata**  
   Read the last successful load month.

2. **Figure out the next month to process**  
   If January was completed, the system automatically moves to February.

3. **Extract**  
   Load the raw Parquet file for that month into the Bronze layer.

4. **Transform**  
   Clean the data and apply business rules before inserting into Silver.

5. **Load**  
   Update the Gold layer using upsert method.

---

## 🔁 Retry Mechanism (Making the Pipeline Fault-Tolerant)

Real pipelines fail — network timeouts, database locks, temporary connection issues.

To handle this, the project includes a retry decorator in `utils.py`.

It allows any database operation to automatically retry with a delay.  
You can configure:
- number of retries  
- wait time  
- error logging  
- backoff strategy  

If something temporary goes wrong, the pipeline doesn’t crash, it simply retries and keeps moving.


## 🧾 Metadata Management

Metadata is stored in a dedicated table that logs:
- which month was processed  
- execution time  
- success/failure status  
- any error messages  

This enables:
- incremental loading  
- safe restarts  
- full auditability  
- monitoring of pipeline health  

Metadata is the key reason incremental loads work reliably.

## 🔄 Full Load vs Incremental Load

Here’s how the two modes differ:

### **Full Load**
Used mainly during first-time setup or historical backfills.  
It processes *all* available data from scratch.

### **Incremental Load**
The normal mode.  
Processes only the next unprocessed month based on metadata.

Incremental loads make the pipeline fast and efficient.

---

## 🛠️ CI/CD Overview
uses
- **flake8** for linting  
- **unit tests**  
- dependency installation  
- general project validation  
And without this been passed, new updates will not able to effect in the already working github code.

---

## 📁 Project Structure
pythonnyc/
├── .github/workflows/ci.yml # CI pipeline
├── .flake8 # Linter config
├── config.py # DB settings & constants
├── main.py # Pipeline entry point
├── tasks.py # ETL logic
├── queries.py # SQL statements
├── utils.py # Retry logic and helpers
├── test_etl_pipeline.py # Tests
└── requirements.txt # Dependencies



---

## 📊 Example Queries from the Gold Layer

Here are some examples of insights you can pull once the pipeline runs.

### **1. Daily Revenue Trend**
```sql
SELECT trip_date, total_revenue
FROM gold.daily_summary
ORDER BY trip_date;
````
## Example of log output doing the running of the code
<img width="960" height="540" alt="AINCRE" src="https://github.com/user-attachments/assets/448b33dc-8a29-4d02-8c38-d1e5fb3db5b6" />



