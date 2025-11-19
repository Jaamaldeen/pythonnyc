import os
import pandas as pd
import psycopg2
from pandas.tseries.offsets import DateOffset
from config import *
from utils import logger
from queries import metadata_insert_success, metadata_insert_fail
from tasks import create_schemas_and_tables, run_etl

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

                cur.execute(metadata_insert_success, ("orchestrator_state", process_month_date, "SUCCESS"))
                conn.commit()
                logger.info(f"Orchestrator state updated to {process_month_date}")

            except Exception as e:
                error_msg = str(e)
                logger.error(f"Failed to process {target_file_name}. Error: {error_msg}")
                conn.rollback()
                
                try:
                    cur.execute(metadata_insert_fail, ("orchestrator_state", "FAILED", error_msg))
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