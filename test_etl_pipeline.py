import unittest
from unittest.mock import MagicMock, patch, ANY
import pandas as pd
import os
import io
import logging
import psycopg2
import config
import utils
import tasks
import main as orchestrator

# --- Configure Logging for Tests ---
# We set it to CRITICAL so the expected "WARNING" logs from the retry tests don't clutter your screen
logging.basicConfig(level=logging.CRITICAL)

# --- SAFETY CONFIGURATION ---
TEST_DB_NAME = "testin_qa"

class TestRetryMechanism(unittest.TestCase):
    def test_retry_success_on_first_try(self):
        mock_func = MagicMock(return_value="Success")
        mock_func.__name__ = "test_func_success" 
        
        decorated_func = utils.with_retry(max_attempts=3, delay=0)(mock_func)
        result = decorated_func()
        
        self.assertEqual(result, "Success")
        self.assertEqual(mock_func.call_count, 1)

    def test_retry_eventual_success(self):
        mock_func = MagicMock(side_effect=[Exception("Fail 1"), Exception("Fail 2"), "Success"])
        mock_func.__name__ = "test_func_eventual"
        
        decorated_func = utils.with_retry(max_attempts=3, delay=0)(mock_func)
        result = decorated_func()
        
        self.assertEqual(result, "Success")
        self.assertEqual(mock_func.call_count, 3)

    def test_retry_failure_max_attempts(self):
        mock_func = MagicMock(side_effect=Exception("Persistent Failure"))
        mock_func.__name__ = "test_func_fail"
        
        decorated_func = utils.with_retry(max_attempts=3, delay=0)(mock_func)
        with self.assertRaises(Exception):
            decorated_func()
        self.assertEqual(mock_func.call_count, 3)

class TestMetadataHandling(unittest.TestCase):
    @patch('main.psycopg2.connect')
    @patch('main.run_etl')
    @patch('os.path.exists')
    def test_orchestrator_calculates_next_date_correctly(self, mock_exists, mock_run_etl, mock_connect):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cur
        
        mock_cur.fetchone.return_value = (pd.Timestamp("2024-01-01"),)
        mock_exists.return_value = True

        orchestrator.main_orchestrator()

        expected_file = f"{config.FILE_PREFIX}_2024-02.parquet"
        full_path = os.path.join(config.DATA_DIRECTORY, expected_file)
        mock_exists.assert_called_with(full_path)
        mock_run_etl.assert_called_once()

class TestSilverTransformations(unittest.TestCase):
    # FIX: Removed the unnecessary @patch('tasks.psycopg2.connect') 
    def test_silver_calculations(self):
        data = {
            'vendorid': [1],
            'tpep_pickup_datetime': [pd.Timestamp('2024-01-01 10:00:00')],
            'tpep_dropoff_datetime': [pd.Timestamp('2024-01-01 10:15:00')], 
            'passenger_count': [1],
            'trip_distance': [2.5],
            'ratecodeid': [1],
            'store_and_fwd_flag': ['N'],
            'pulocationid': [100],
            'dolocationid': [101],
            'payment_type': [1],
            'fare_amount': [10.0],
            'extra': [1.0],
            'mta_tax': [0.5],
            'tip_amount': [2.0],
            'tolls_amount': [0.0],
            'improvement_surcharge': [0.3],
            'congestion_surcharge': [2.5],
            'airport_fee': [0.0]
        }
        df_mock_bronze = pd.DataFrame(data)
        
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchone.return_value = (pd.Timestamp('2023-12-31'),)
        
        # We intentionally patch pandas inside 'tasks' because that is where read_sql is called
        with patch('tasks.pd.read_sql', return_value=df_mock_bronze):
             tasks.load_silver_incremental(mock_conn, mock_cur)

        self.assertTrue(mock_cur.copy_expert.called)
        _, args, _ = mock_cur.copy_expert.mock_calls[0] 
        csv_buffer = args[1]
        csv_content = csv_buffer.getvalue()
        
        self.assertIn("15.0", csv_content)
        self.assertIn("16.3", csv_content)

class TestFullPipelineIntegration(unittest.TestCase):
    TEST_FILE = "test_dummy_data.parquet" 

    def setUp(self):
        if TEST_DB_NAME == config.DB_NAME:
             self.skipTest("Skipping integration test: Test DB name matches Production DB name")

        data = {
            'VendorID': [1, 2],
            'tpep_pickup_datetime': [pd.Timestamp('2024-01-01 12:00:00'), pd.Timestamp('2024-01-01 12:30:00')],
            'tpep_dropoff_datetime': [pd.Timestamp('2024-01-01 12:10:00'), pd.Timestamp('2024-01-01 12:45:00')],
            'passenger_count': [1.0, 2.0],
            'trip_distance': [1.5, 3.0],
            'RatecodeID': [1.0, 1.0],
            'store_and_fwd_flag': ['N', 'N'],
            'PULocationID': [161, 162],
            'DOLocationID': [163, 164],
            'payment_type': [1, 2],
            'fare_amount': [10.0, 20.0],
            'extra': [0.0, 0.0],
            'mta_tax': [0.5, 0.5],
            'tip_amount': [2.0, 0.0],
            'tolls_amount': [0.0, 0.0],
            'improvement_surcharge': [0.3, 0.3],
            'total_amount': [12.8, 20.8], 
            'congestion_surcharge': [0.0, 0.0],
            'Airport_fee': [0.0, 0.0]
        }
        df = pd.DataFrame(data)
        df.to_parquet(self.TEST_FILE)

        try:
            self.conn = psycopg2.connect(
                dbname=TEST_DB_NAME,
                user=config.DB_USERNAME,
                password=config.DB_PASSWORD,
                host=config.DB_HOST,
                port=config.DB_PORT
            )
            self.cur = self.conn.cursor()
            
            tables = [
                "bronze.taxi", "silver.taxi_cleaned", 
                "gold.daily_summary", "gold.monthly_summary", "gold.payment_summary", 
                "gold.vendor_summary", "gold.zone_summary",
                "etl.pipeline_metadata"
            ]
            
            for schema in ["bronze", "silver", "gold", "etl"]:
                self.cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

            for t in tables:
                try:
                    self.cur.execute(f"TRUNCATE TABLE {t} RESTART IDENTITY CASCADE;")
                except psycopg2.Error:
                    self.conn.rollback()
            self.conn.commit()
            
        except psycopg2.OperationalError:
            self.skipTest(f"Skipping integration test: Could not connect to database '{TEST_DB_NAME}'.")
        except Exception as e:
            self.fail(f"Integration test setup failed: {e}")

    def tearDown(self):
        if os.path.exists(self.TEST_FILE):
            try:
                os.remove(self.TEST_FILE)
            except PermissionError:
                pass
        
        if hasattr(self, 'cur') and self.cur: self.cur.close()
        if hasattr(self, 'conn') and self.conn: self.conn.close()

    def test_end_to_end_pipeline(self):
        tasks.create_schemas_and_tables(self.conn, self.cur)
        tasks.load_bronze_chunked(self.TEST_FILE, self.conn, self.cur)
        tasks.load_silver_incremental(self.conn, self.cur)
        tasks.load_gold_incremental(self.conn, self.cur)

        self.cur.execute("SELECT COUNT(*) FROM bronze.taxi")
        self.assertEqual(self.cur.fetchone()[0], 2)

        self.cur.execute("SELECT COUNT(*) FROM silver.taxi_cleaned")
        self.assertEqual(self.cur.fetchone()[0], 2)
        
        self.cur.execute("SELECT trip_duration_minutes FROM silver.taxi_cleaned WHERE vendorid=1")
        self.assertEqual(self.cur.fetchone()[0], 10.0)

if __name__ == '__main__':
    unittest.main()