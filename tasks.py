import io
import pandas as pd
import warnings
from utils import logger, with_retry
from queries import *

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

        cur.execute(metadata_insert_success, ("bronze_taxi", max_ts, "SUCCESS"))
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
            cur.execute(metadata_insert_no_data, ("silver_taxi", "NO NEW DATA"))
            conn.commit()
            return

        logger.info(f"Loaded {len(df_new)} new rows from Bronze for Silver processing.")
        df_new.columns = [c.lower() for c in df_new.columns]
        max_ts = df_new['tpep_pickup_datetime'].max()

        vendor_map = {1: 'Creative Mobile Technologies, LLC', 2: 'Curb Mobility, LLC', 6: 'Myle Technologies Inc', 7: 'Helix'}
        rate_map = {1: 'Standard rate', 2: 'JFK', 3: 'Newark', 4: 'Nassau or Westchester', 5: 'Negotiated fare', 6: 'Group ride'}
        payment_map = {0: 'Flex Fare trip', 1: 'Credit card', 2: 'Cash', 3: 'No charge', 4: 'Dispute', 5: 'Unknown', 6: 'Voided trip'}

        df_new['vendor_name'] = df_new['vendorid'].map(vendor_map).fillna('Unknown')
        df_new['rate_description'] = df_new['ratecodeid'].map(rate_map).fillna('Null/unknown')
        df_new['payment_description'] = df_new['payment_type'].map(payment_map).fillna('Unknown')
        df_new['trip_duration_minutes'] = ((df_new['tpep_dropoff_datetime'] - df_new['tpep_pickup_datetime']).dt.total_seconds() / 60).round(2)

        money_cols = ['fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'congestion_surcharge', 'airport_fee']
        for col in money_cols:
            if col in df_new.columns:
                df_new[col] = df_new[col].abs()

        df_new['total_amount'] = df_new[money_cols].sum(axis=1)

        dedup_cols = ['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'pulocationid', 'dolocationid', 'passenger_count', 'trip_distance']
        df_new = df_new.drop_duplicates(subset=dedup_cols)

        if df_new.empty:
            logger.info("All new rows were duplicates. No data to load into Silver.")
            return

        silver_cols = ['vendorid', 'ratecodeid', 'payment_type', 'vendor_name', 'rate_description', 'payment_description',
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

        cur.execute(metadata_insert_success, ("silver_taxi", max_ts, "SUCCESS"))
        conn.commit()
        logger.info(f"Silver metadata updated to {max_ts}.")

    except Exception as e:
        logger.error(f"Silver load error: {e}")
        conn.rollback()
        raise

@with_retry()
def load_gold_incremental(conn, cur):
    try:
        cur.execute(gold_daily_insert)
        conn.commit()
        logger.info("Gold daily_summary updated.")

        cur.execute(gold_monthly_insert)
        conn.commit()
        logger.info("Gold monthly_summary updated.")

        cur.execute(gold_payment_insert)
        conn.commit()
        logger.info("Gold payment_summary updated.")

        cur.execute(gold_vendor_insert)
        conn.commit()
        logger.info("Gold vendor_summary updated.")

        cur.execute(gold_zone_insert)
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