import io
import os
import pandas as pd
import psycopg2
import logging
import time
import warnings
from functools import wraps
from pandas.tseries.offsets import DateOffset
from dotenv import load_dotenv

load_dotenv()

START_YEAR = 2024
START_MONTH = 1
DATA_DIRECTORY = "."
FILE_PREFIX = "yellow_tripdata"

MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 5

DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if not logger.handlers:
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    file_handler = logging.FileHandler("pipeline.log", encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)


def with_retry(max_attempts=MAX_RETRIES, delay=RETRY_DELAY_SECONDS):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    logger.warning(f"Attempt {attempt}/{max_attempts} failed for {func.__name__}. Error: {e}")
                    if attempt < max_attempts:
                        logger.info(f"Retrying in {delay} seconds...")
                        time.sleep(delay)

            logger.error(f"All {max_attempts} attempts failed for {func.__name__}.")
            raise last_exception

        return wrapper

    return decorator


bronze_table = """
CREATE TABLE IF NOT EXISTS bronze.taxi (
    vendorid SMALLINT,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count SMALLINT,
    trip_distance NUMERIC(10,2),
    ratecodeid SMALLINT,
    store_and_fwd_flag VARCHAR(1),
    pulocationid INT,
    dolocationid INT,
    payment_type SMALLINT,
    fare_amount NUMERIC(10,2),
    extra NUMERIC(10,2),
    mta_tax NUMERIC(10,2),
    tip_amount NUMERIC(10,2),
    tolls_amount NUMERIC(10,2),
    improvement_surcharge NUMERIC(10,2),
    total_amount NUMERIC(10,2),
    congestion_surcharge NUMERIC(10,2),
    airport_fee NUMERIC(10,2),
    load_timestamp TIMESTAMP DEFAULT NOW(),
    process_month DATE
);
"""

silver_table = """
CREATE TABLE IF NOT EXISTS silver.taxi_cleaned (
    vendorid SMALLINT,
    ratecodeid SMALLINT,
    payment_type SMALLINT,
    vendor_name VARCHAR(100),
    rate_description VARCHAR(100),
    payment_description VARCHAR(100),
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    trip_duration_minutes FLOAT,
    passenger_count SMALLINT,
    trip_distance NUMERIC(10,2),
    store_and_fwd_flag VARCHAR(1),
    pulocationid INT,
    dolocationid INT,
    fare_amount NUMERIC(10,2),
    extra NUMERIC(10,2),
    mta_tax NUMERIC(10,2),
    tip_amount NUMERIC(10,2),
    tolls_amount NUMERIC(10,2),
    improvement_surcharge NUMERIC(10,2),
    total_amount NUMERIC(10,2),
    congestion_surcharge NUMERIC(10,2),
    airport_fee NUMERIC(10,2)
);
"""

metadata_table = """
CREATE TABLE IF NOT EXISTS etl.pipeline_metadata (
    pipeline_name VARCHAR(100) PRIMARY KEY,
    last_successful_load_month TIMESTAMP,
    last_run_status VARCHAR(50),
    last_run_timestamp TIMESTAMP,
    error_message TEXT
);
"""

gold_daily_summary_table = """
CREATE TABLE IF NOT EXISTS gold.daily_summary (
    trip_date DATE PRIMARY KEY,
    total_trips BIGINT,
    total_passengers FLOAT,
    total_distance_miles FLOAT,
    total_revenue FLOAT,
    total_tips FLOAT,
    avg_fare FLOAT,
    avg_trip_distance FLOAT
);
"""

gold_monthly_summary_table = """
CREATE TABLE IF NOT EXISTS gold.monthly_summary (
    month DATE PRIMARY KEY,
    total_trips BIGINT,
    total_revenue FLOAT,
    total_tips FLOAT,
    total_distance FLOAT
);
"""

gold_payment_summary_table = """
CREATE TABLE IF NOT EXISTS gold.payment_summary (
    payment_description VARCHAR PRIMARY KEY,
    trip_count BIGINT,
    total_revenue FLOAT,
    total_tips FLOAT,
    avg_tip_percent NUMERIC
);
"""

gold_vendor_summary_table = """
CREATE TABLE IF NOT EXISTS gold.vendor_summary (
    vendor_name VARCHAR PRIMARY KEY,
    total_trips BIGINT,
    total_revenue FLOAT,
    total_distance FLOAT,
    avg_trip_distance FLOAT,
    avg_fare FLOAT
);
"""

gold_zone_summary_table = """
CREATE TABLE IF NOT EXISTS gold.zone_summary (
    PULocationID INT PRIMARY KEY,
    pickups BIGINT,
    revenue_from_pickups FLOAT,
    total_tips FLOAT
);
"""

index_sql_statements = [
    "CREATE INDEX IF NOT EXISTS idx_bronze_process_month ON bronze.taxi(process_month);",
    "CREATE INDEX IF NOT EXISTS idx_silver_pickup_datetime ON silver.taxi_cleaned(tpep_pickup_datetime);",
    "CREATE INDEX IF NOT EXISTS idx_silver_pulocationid ON silver.taxi_cleaned(pulocationid);"
]


def create_schemas_and_tables(conn, cur):
    schemas = ["bronze", "silver", "gold", "etl"]
    for schema in schemas:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

    cur.execute(bronze_table)
    cur.execute(silver_table)
    cur.execute(metadata_table)

    try:
        cur.execute("ALTER TABLE etl.pipeline_metadata ADD COLUMN IF NOT EXISTS error_message TEXT;")
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.warning(f"Could not alter metadata table: {e}")

    cur.execute(gold_daily_summary_table)
    cur.execute(gold_monthly_summary_table)
    cur.execute(gold_payment_summary_table)
    cur.execute(gold_vendor_summary_table)
    cur.execute(gold_zone_summary_table)

    for statement in index_sql_statements:
        cur.execute(statement)

    conn.commit()
    logger.info("Schemas, tables, and indexes created/verified.")


@with_retry()
def load_bronze_chunked(parquet_file, conn, cur, chunksize=500_000):
    try:
        cur.execute("SELECT last_successful_load_month FROM etl.pipeline_metadata WHERE pipeline_name='bronze_taxi'")
        last_load = cur.fetchone()
        last_ts_raw = last_load[0] if last_load and last_load[0] else pd.Timestamp("1900-01-01")
        last_ts = pd.to_datetime(last_ts_raw)

        logger.info(f"Loading {parquet_file} incrementally after: {last_ts}")

        df = pd.read_parquet(parquet_file)
        df.columns = [c.lower() for c in df.columns]

        for col in ['tpep_pickup_datetime', 'tpep_dropoff_datetime']:
            df[col] = pd.to_datetime(df[col], errors='coerce')

        initial_len = len(df)
        df = df.dropna(subset=['tpep_pickup_datetime', 'tpep_dropoff_datetime'])
        dropped_rows = initial_len - len(df)
        if dropped_rows > 0:
            logger.warning(f"Dropped {dropped_rows} rows due to invalid timestamps.")

        df = df.sort_values('tpep_pickup_datetime')
        df_new = df[df['tpep_pickup_datetime'] > last_ts].copy()

        if df_new.empty:
            logger.info("No new Bronze rows to load from this file.")
            return

        max_ts = df_new['tpep_pickup_datetime'].max()
        total_rows = 0

        for start in range(0, len(df_new), chunksize):
            df_chunk = df_new.iloc[start:start + chunksize].copy()

            int_cols = ['vendorid', 'passenger_count', 'ratecodeid', 'payment_type', 'pulocationid', 'dolocationid']
            for col in int_cols:
                if col in df_chunk.columns:
                    df_chunk[col] = df_chunk[col].fillna(0.0).astype('int64')

            money_cols = ['fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
                          'improvement_surcharge', 'congestion_surcharge', 'airport_fee', 'total_amount',
                          'trip_distance']
            for col in money_cols:
                if col in df_chunk.columns:
                    df_chunk.loc[:, col] = df_chunk[col].fillna(0.0).astype(float)

            df_chunk['process_month'] = df_chunk['tpep_pickup_datetime'].dt.to_period('M').dt.to_timestamp()

            csv_buffer = io.StringIO()
            df_chunk.to_csv(csv_buffer, index=False, header=False)
            csv_buffer.seek(0)

            copy_sql = """
            COPY bronze.taxi (
                vendorid, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count,
                trip_distance, ratecodeid, store_and_fwd_flag, pulocationid, dolocationid,
                payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount,
                improvement_surcharge, total_amount, congestion_surcharge, airport_fee, process_month
            ) FROM STDIN WITH CSV;
            """
            cur.copy_expert(copy_sql, csv_buffer)
            conn.commit()
            total_rows += len(df_chunk)
            logger.info(f"Bronze chunk loaded: {len(df_chunk)} rows")

        cur.execute("""
        INSERT INTO etl.pipeline_metadata(pipeline_name,last_successful_load_month,last_run_status,last_run_timestamp, error_message)
        VALUES (%s,%s,%s,NOW(), NULL)
        ON CONFLICT(pipeline_name)
        DO UPDATE SET last_successful_load_month=EXCLUDED.last_successful_load_month,
                      last_run_status=EXCLUDED.last_run_status,
                      last_run_timestamp=EXCLUDED.last_run_timestamp,
                      error_message=NULL
        """, ("bronze_taxi", max_ts, "SUCCESS"))
        conn.commit()
        logger.info(f"Bronze metadata updated to {max_ts} after {total_rows} new rows")

    except Exception as e:
        logger.error(f"Bronze load error: {e}")
        conn.rollback()
        raise


@with_retry()
def load_silver_incremental(conn, cur):
    try:
        cur.execute("SELECT last_successful_load_month FROM etl.pipeline_metadata WHERE pipeline_name='silver_taxi'")
        last_load = cur.fetchone()
        last_silver_ts_raw = last_load[0] if last_load and last_load[0] else pd.Timestamp("1900-01-01")
        last_silver_ts = pd.to_datetime(last_silver_ts_raw)

        logger.info(f"Loading Silver incrementally after: {last_silver_ts}")

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=UserWarning)
            df_new = pd.read_sql("SELECT * FROM bronze.taxi WHERE tpep_pickup_datetime > %s", conn,
                                 params=(last_silver_ts,))

        if df_new.empty:
            logger.info("No new Bronze rows for Silver.")
            cur.execute("""
            INSERT INTO etl.pipeline_metadata(pipeline_name,last_run_status,last_run_timestamp, error_message)
            VALUES (%s,%s,NOW(), NULL)
            ON CONFLICT(pipeline_name)
            DO UPDATE SET last_run_status=EXCLUDED.last_run_status,
                          last_run_timestamp=EXCLUDED.last_run_timestamp,
                          error_message=NULL
            """, ("silver_taxi", "NO NEW DATA"))
            conn.commit()
            return

        logger.info(f"Loaded {len(df_new)} new rows from Bronze for Silver processing.")
        df_new.columns = [c.lower() for c in df_new.columns]
        max_ts = df_new['tpep_pickup_datetime'].max()

        vendor_map = {1: 'Creative Mobile Technologies, LLC', 2: 'Curb Mobility, LLC', 6: 'Myle Technologies Inc',
                      7: 'Helix'}
        rate_map = {1: 'Standard rate', 2: 'JFK', 3: 'Newark', 4: 'Nassau or Westchester', 5: 'Negotiated fare',
                    6: 'Group ride'}
        payment_map = {0: 'Flex Fare trip', 1: 'Credit card', 2: 'Cash', 3: 'No charge', 4: 'Dispute', 5: 'Unknown',
                       6: 'Voided trip'}

        df_new['vendor_name'] = df_new['vendorid'].map(vendor_map).fillna('Unknown')
        df_new['rate_description'] = df_new['ratecodeid'].map(rate_map).fillna('Null/unknown')
        df_new['payment_description'] = df_new['payment_type'].map(payment_map).fillna('Unknown')
        df_new['trip_duration_minutes'] = (
                    (df_new['tpep_dropoff_datetime'] - df_new['tpep_pickup_datetime']).dt.total_seconds() / 60).round(2)

        money_cols = ['fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge',
                      'congestion_surcharge', 'airport_fee']
        for col in money_cols:
            if col in df_new.columns:
                df_new[col] = df_new[col].abs()

        df_new['total_amount'] = df_new[money_cols].sum(axis=1)

        dedup_cols = ['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'pulocationid', 'dolocationid',
                      'passenger_count', 'trip_distance']
        df_new = df_new.drop_duplicates(subset=dedup_cols)

        if df_new.empty:
            logger.info("All new rows were duplicates. No data to load into Silver.")
            return

        silver_cols = ['vendorid', 'ratecodeid', 'payment_type', 'vendor_name', 'rate_description',
                       'payment_description',
                       'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'trip_duration_minutes', 'passenger_count',
                       'trip_distance', 'store_and_fwd_flag', 'pulocationid', 'dolocationid',
                       'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
                       'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'airport_fee']

        df_to_load = df_new[silver_cols]

        csv_buffer = io.StringIO()
        df_to_load.to_csv(csv_buffer, index=False, header=False)
        csv_buffer.seek(0)

        copy_sql = """
            COPY silver.taxi_cleaned (
                vendorid, ratecodeid, payment_type, vendor_name, rate_description, payment_description,
                tpep_pickup_datetime, tpep_dropoff_datetime, trip_duration_minutes, passenger_count,
                trip_distance, store_and_fwd_flag, pulocationid, dolocationid,
                fare_amount, extra, mta_tax, tip_amount, tolls_amount,
                improvement_surcharge, total_amount, congestion_surcharge, airport_fee
            )
            FROM STDIN WITH CSV;
        """
        cur.copy_expert(copy_sql, csv_buffer)
        conn.commit()
        logger.info(f"Silver incremental load completed: {len(df_new)} rows")

        cur.execute("""
            INSERT INTO etl.pipeline_metadata(pipeline_name,last_successful_load_month,last_run_status,last_run_timestamp, error_message)
            VALUES (%s,%s,%s,NOW(), NULL)
            ON CONFLICT(pipeline_name)
            DO UPDATE SET last_successful_load_month=EXCLUDED.last_successful_load_month,
                          last_run_status=EXCLUDED.last_run_status,
                          last_run_timestamp=EXCLUDED.last_run_timestamp,
                          error_message=NULL
        """, ("silver_taxi", max_ts, "SUCCESS"))
        conn.commit()
        logger.info(f"Silver metadata updated to {max_ts}.")

    except Exception as e:
        logger.error(f"Silver load error: {e}")
        conn.rollback()
        raise


@with_retry()
def load_gold_incremental(conn, cur):
    try:
        cur.execute("""
            INSERT INTO gold.daily_summary(
                trip_date, total_trips, total_passengers, total_distance_miles,
                total_revenue, total_tips, avg_fare, avg_trip_distance
            )
            SELECT
                DATE(tpep_pickup_datetime) AS trip_date,
                COUNT(*) AS total_trips,
                SUM(passenger_count)::FLOAT AS total_passengers,
                SUM(trip_distance)::FLOAT AS total_distance_miles,
                SUM(total_amount)::FLOAT AS total_revenue,
                SUM(tip_amount)::FLOAT AS total_tips,
                AVG(fare_amount)::FLOAT AS avg_fare,
                AVG(trip_distance)::FLOAT AS avg_trip_distance
            FROM silver.taxi_cleaned
            WHERE tpep_pickup_datetime > (
                SELECT COALESCE(MAX(trip_date), '1900-01-01') FROM gold.daily_summary
            )
            GROUP BY DATE(tpep_pickup_datetime)
            ON CONFLICT (trip_date) DO UPDATE SET
                total_trips = EXCLUDED.total_trips,
                total_passengers = EXCLUDED.total_passengers,
                total_distance_miles = EXCLUDED.total_distance_miles,
                total_revenue = EXCLUDED.total_revenue,
                total_tips = EXCLUDED.total_tips,
                avg_fare = EXCLUDED.avg_fare,
                avg_trip_distance = EXCLUDED.avg_trip_distance;
        """)
        conn.commit()
        logger.info("Gold daily_summary updated.")

        cur.execute("""
            INSERT INTO gold.monthly_summary(
                month, total_trips, total_revenue, total_tips, total_distance
            )
            SELECT
                DATE_TRUNC('month', tpep_pickup_datetime)::DATE AS month,
                COUNT(*) AS total_trips,
                SUM(total_amount)::FLOAT AS total_revenue,
                SUM(tip_amount)::FLOAT AS total_tips,
                SUM(trip_distance)::FLOAT AS total_distance
            FROM silver.taxi_cleaned
            WHERE tpep_pickup_datetime > (
                SELECT COALESCE(MAX(month), '1900-01-01') FROM gold.monthly_summary
            )
            GROUP BY DATE_TRUNC('month', tpep_pickup_datetime)
            ON CONFLICT (month) DO UPDATE SET
                total_trips = EXCLUDED.total_trips,
                total_revenue = EXCLUDED.total_revenue,
                total_tips = EXCLUDED.total_tips,
                total_distance = EXCLUDED.total_distance;
        """)
        conn.commit()
        logger.info("Gold monthly_summary updated.")

        cur.execute("""
            WITH full_agg AS (
                SELECT
                    COALESCE(payment_description, 'Unknown') AS payment_description,
                    COUNT(*) AS trip_count,
                    SUM(total_amount)::FLOAT AS total_revenue,
                    SUM(tip_amount)::FLOAT AS total_tips,
                    CASE 
                        WHEN SUM(total_amount) <= 0 THEN 0
                        ELSE (SUM(tip_amount) / SUM(total_amount)) * 100 
                    END AS avg_tip_percent
                FROM silver.taxi_cleaned
                GROUP BY COALESCE(payment_description, 'Unknown')
            )
            INSERT INTO gold.payment_summary(
                payment_description, trip_count, total_revenue, total_tips, avg_tip_percent
            )
            SELECT * FROM full_agg
            ON CONFLICT (payment_description) DO UPDATE SET
                trip_count = EXCLUDED.trip_count,
                total_revenue = EXCLUDED.total_revenue,
                total_tips = EXCLUDED.total_tips,
                avg_tip_percent = EXCLUDED.avg_tip_percent;
        """)
        conn.commit()
        logger.info("Gold payment_summary updated.")

        cur.execute("""
            WITH full_agg AS (
                SELECT
                    vendor_name,
                    COUNT(*) AS total_trips,
                    SUM(total_amount)::FLOAT AS total_revenue,
                    SUM(trip_distance)::FLOAT AS total_distance,
                    AVG(trip_distance)::FLOAT AS avg_trip_distance,
                    AVG(fare_amount)::FLOAT AS avg_fare
                FROM silver.taxi_cleaned
                GROUP BY vendor_name
            )
            INSERT INTO gold.vendor_summary(
                vendor_name, total_trips, total_revenue, total_distance, avg_trip_distance, avg_fare
            )
            SELECT * FROM full_agg
            ON CONFLICT (vendor_name) DO UPDATE SET
                total_trips = EXCLUDED.total_trips,
                total_revenue = EXCLUDED.total_revenue,
                total_distance = EXCLUDED.total_distance,
                avg_trip_distance = EXCLUDED.avg_trip_distance,
                avg_fare = EXCLUDED.avg_fare;
        """)
        conn.commit()
        logger.info("Gold vendor_summary updated.")

        cur.execute("""
            WITH full_agg AS (
                SELECT
                    PULocationID,
                    COUNT(*) AS pickups,
                    SUM(total_amount)::FLOAT AS revenue_from_pickups,
                    SUM(tip_amount)::FLOAT AS total_tips
                FROM silver.taxi_cleaned
                GROUP BY PULocationID
            )
            INSERT INTO gold.zone_summary(
                PULocationID, pickups, revenue_from_pickups, total_tips
            )
            SELECT * FROM full_agg
            ON CONFLICT (PULocationID) DO UPDATE SET
                pickups = EXCLUDED.pickups,
                revenue_from_pickups = EXCLUDED.revenue_from_pickups,
                total_tips = EXCLUDED.total_tips;
        """)
        conn.commit()
        logger.info("Gold zone_summary updated.")

    except Exception as e:
        logger.error(f"Gold load error: {e}")
        conn.rollback()
        raise


def run_etl(parquet_file, conn, cur):
    try:
        logger.info("----------------------------------------------------")
        logger.info(f"Starting ETL process for {parquet_file}...")

        load_bronze_chunked(parquet_file, conn, cur)
        load_silver_incremental(conn, cur)
        load_gold_incremental(conn, cur)

        logger.info(f"Full ETL pipeline completed successfully for {parquet_file}!")
        logger.info("----------------------------------------------------")

    except Exception as e:
        raise Exception(f"ETL Pipeline FAILED for {parquet_file}: {e}")


def main_orchestrator():
    conn = None
    cur = None
    try:
        logger.info("Connecting to database...")
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USERNAME,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cur = conn.cursor()

        create_schemas_and_tables(conn, cur)

        cur.execute(
            "SELECT last_successful_load_month FROM etl.pipeline_metadata WHERE pipeline_name='orchestrator_state'")
        last_load = cur.fetchone()

        if last_load and last_load[0]:
            last_ts = pd.to_datetime(last_load[0])
            next_month_ts = (last_ts + DateOffset(months=1)).replace(day=1)
            target_year = next_month_ts.year
            target_month = next_month_ts.month
        else:
            target_year = START_YEAR
            target_month = START_MONTH

        process_month_date = pd.to_datetime(f'{target_year}-{target_month}-01')
        target_file_name = f"{FILE_PREFIX}_{target_year}-{target_month:02d}.parquet"
        target_file_path = os.path.join(DATA_DIRECTORY, target_file_name)

        if os.path.exists(target_file_path):
            logger.info(f"Found next file to process: {target_file_name}")
            try:
                run_etl(target_file_path, conn, cur)

                cur.execute("""
                    INSERT INTO etl.pipeline_metadata(pipeline_name, last_successful_load_month, last_run_status, last_run_timestamp, error_message)
                    VALUES (%s, %s, %s, NOW(), NULL)
                    ON CONFLICT(pipeline_name)
                    DO UPDATE SET last_successful_load_month=EXCLUDED.last_successful_load_month,
                                  last_run_status=EXCLUDED.last_run_status,
                                  last_run_timestamp=EXCLUDED.last_run_timestamp,
                                  error_message=NULL
                    """, ("orchestrator_state", process_month_date, "SUCCESS"))
                conn.commit()
                logger.info(f"Orchestrator state updated to {process_month_date}")

            except Exception as e:
                error_msg = str(e)
                logger.error(f"Failed to process {target_file_name}. Error: {error_msg}")
                conn.rollback()

                try:
                    cur.execute("""
                        INSERT INTO etl.pipeline_metadata(pipeline_name, last_run_status, last_run_timestamp, error_message)
                        VALUES (%s, %s, NOW(), %s)
                        ON CONFLICT(pipeline_name)
                        DO UPDATE SET last_run_status=EXCLUDED.last_run_status,
                                      last_run_timestamp=EXCLUDED.last_run_timestamp,
                                      error_message=EXCLUDED.error_message
                    """, ("orchestrator_state", "FAILED", error_msg))
                    conn.commit()
                    logger.info("Orchestrator state updated to FAILED in metadata.")
                except Exception as meta_e:
                    logger.error(f"Could not update metadata failure status: {meta_e}")
        else:
            logger.info(f"Did not find {target_file_name}. Pipeline is up-to-date.")

    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
    finally:
        if cur: cur.close()
        if conn: conn.close()
        logger.info("Database connection closed. Orchestration finished.")


if __name__ == "__main__":
    main_orchestrator()