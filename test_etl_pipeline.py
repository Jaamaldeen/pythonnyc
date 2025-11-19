import unittest
from unittest.mock import MagicMock, patch, ANY
import pandas as pd
import os
import io
import logging
import psycopg2
from datetime import datetime
import etl_pipeline

# --- Configure Logging for Tests ---
logging.basicConfig(level=logging.ERROR)

# --- SAFETY CONFIGURATION ---
# The integration test will ONLY run against this database.
# It will NOT touch your main 'testin' database.
TEST_DB_NAME = "testin_qa"


class TestRetryMechanism(unittest.TestCase):
    """
    Unit tests for the retry logic decorator.
    """

    def test_retry_success_on_first_try(self):
        mock_func = MagicMock(return_value="Success")
        # FIX: Give the mock a name so the logger doesn't crash
        mock_func.__name__ = "test_func_success"

        decorated_func = etl_pipeline.with_retry(max_attempts=3, delay=0)(mock_func)
        result = decorated_func()

        self.assertEqual(result, "Success")
        self.assertEqual(mock_func.call_count, 1)

    def test_retry_eventual_success(self):
        mock_func = MagicMock(side_effect=[Exception("Fail 1"), Exception("Fail 2"), "Success"])
        # FIX: Give the mock a name
        mock_func.__name__ = "test_func_eventual"

        decorated_func = etl_pipeline.with_retry(max_attempts=3, delay=0)(mock_func)
        result = decorated_func()

        self.assertEqual(result, "Success")
        self.assertEqual(mock_func.call_count, 3)

    def test_retry_failure_max_attempts(self):
        mock_func = MagicMock(side_effect=Exception("Persistent Failure"))
        # FIX: Give the mock a name
        mock_func.__name__ = "test_func_fail"

        decorated_func = etl_pipeline.with_retry(max_attempts=3, delay=0)(mock_func)

        with self.assertRaises(Exception) as cm:
            decorated_func()

        self.assertEqual(str(cm.exception), "Persistent Failure")
        self.assertEqual(mock_func.call_count, 3)


class TestMetadataHandling(unittest.TestCase):
    @patch('etl_pipeline.psycopg2.connect')
    @patch('etl_pipeline.run_etl')
    @patch('os.path.exists')
    def test_orchestrator_calculates_next_date_correctly(self, mock_exists, mock_run_etl, mock_connect):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cur

        # Mock returning a specific date so we know what "next" should be
        mock_cur.fetchone.return_value = (pd.Timestamp("2024-01-01"),)
        mock_exists.return_value = True

        etl_pipeline.main_orchestrator()

        # Verify it looked for the FEB file (Jan + 1 month)
        expected_file = f"{etl_pipeline.FILE_PREFIX}_2024-02.parquet"
        full_path = os.path.join(etl_pipeline.DATA_DIRECTORY, expected_file)
        mock_exists.assert_called_with(full_path)
        mock_run_etl.assert_called_once()

        # Verify it tried to update metadata with the correct date
        expected_update_date = pd.Timestamp("2024-02-01")

        found_update = False
        # Inspect call args to find the metadata update query
        for call in mock_cur.execute.call_args_list:
            args, _ = call
            sql = args[0]
            if "INSERT INTO etl.pipeline_metadata" in sql and "orchestrator_state" in str(args):
                # args[1] is the parameters tuple
                params = args[1]
                # Params structure: (name, date, status)
                if params[0] == "orchestrator_state" and params[1] == expected_update_date:
                    found_update = True
                    break

        self.assertTrue(found_update, "Did not find metadata update for 2024-02-01")


class TestSilverTransformations(unittest.TestCase):
    @patch('etl_pipeline.psycopg2.connect')
    def test_silver_calculations(self, mock_connect):
        # Create sample input DataFrame
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

        with patch('etl_pipeline.pd.read_sql', return_value=df_mock_bronze):
            etl_pipeline.load_silver_incremental(mock_conn, mock_cur)

        # Verify the transformation logic by inspecting what was sent to COPY
        self.assertTrue(mock_cur.copy_expert.called)
        _, args, _ = mock_cur.copy_expert.mock_calls[0]
        csv_buffer = args[1]
        csv_content = csv_buffer.getvalue()

        # Check for calculated values in the CSV string
        self.assertIn("15.0", csv_content)  # Duration
        self.assertIn("16.3", csv_content)  # Total Amount
        self.assertIn("Creative Mobile Technologies", csv_content)  # Mapped Vendor


class TestFullPipelineIntegration(unittest.TestCase):
    # Using a generic name to ensure we don't touch real files
    TEST_FILE = "test_dummy_data.parquet"

    def setUp(self):
        # 1. Safety Check: Ensure we are NOT connecting to the production DB name from config
        if TEST_DB_NAME == etl_pipeline.DB_NAME:
            self.skipTest("Skipping integration test: Test DB name matches Production DB name in .env")

        # 2. Create dummy parquet file
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

        # 3. Connect to the QA database
        try:
            self.conn = psycopg2.connect(
                dbname=TEST_DB_NAME,  # Uses "testin_qa", NOT "testin"
                user=etl_pipeline.DB_USERNAME,
                password=etl_pipeline.DB_PASSWORD,
                host=etl_pipeline.DB_HOST,
                port=etl_pipeline.DB_PORT
            )
            self.cur = self.conn.cursor()

            # Reset tables in the QA database
            tables = [
                "bronze.taxi", "silver.taxi_cleaned",
                "gold.daily_summary", "gold.monthly_summary", "gold.payment_summary",
                "gold.vendor_summary", "gold.zone_summary",
                "etl.pipeline_metadata"
            ]

            # Create schemas if they don't exist in the QA db
            for schema in ["bronze", "silver", "gold", "etl"]:
                self.cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

            # Clean tables
            for t in tables:
                try:
                    self.cur.execute(f"TRUNCATE TABLE {t} RESTART IDENTITY CASCADE;")
                except psycopg2.Error:
                    self.conn.rollback()  # Table might not exist yet
            self.conn.commit()

        except psycopg2.OperationalError:
            self.skipTest(
                f"Skipping integration test: Could not connect to database '{TEST_DB_NAME}'. Please create it.")
        except Exception as e:
            self.fail(f"Integration test setup failed: {e}")

    def tearDown(self):
        if os.path.exists(self.TEST_FILE):
            try:
                os.remove(self.TEST_FILE)
            except PermissionError:
                pass  # File might be open, ignore

        if hasattr(self, 'cur') and self.cur: self.cur.close()
        if hasattr(self, 'conn') and self.conn: self.conn.close()

    def test_end_to_end_pipeline(self):
        # Run the pipeline functions against the QA connection
        etl_pipeline.create_schemas_and_tables(self.conn, self.cur)
        etl_pipeline.load_bronze_chunked(self.TEST_FILE, self.conn, self.cur)
        etl_pipeline.load_silver_incremental(self.conn, self.cur)
        etl_pipeline.load_gold_incremental(self.conn, self.cur)

        # Verify counts
        self.cur.execute("SELECT COUNT(*) FROM bronze.taxi")
        self.assertEqual(self.cur.fetchone()[0], 2)

        self.cur.execute("SELECT COUNT(*) FROM silver.taxi_cleaned")
        self.assertEqual(self.cur.fetchone()[0], 2)

        # Verify transformations in DB
        self.cur.execute("SELECT trip_duration_minutes FROM silver.taxi_cleaned WHERE vendorid=1")
        self.assertEqual(self.cur.fetchone()[0], 10.0)

        # Verify Gold Aggregation
        self.cur.execute("SELECT total_trips, total_revenue FROM gold.daily_summary WHERE trip_date='2024-01-01'")
        row = self.cur.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row[0], 2)
        self.assertAlmostEqual(row[1], 33.6, places=1)
        print("\nIntegration Test Passed: Data flowed correctly from Parquet -> Bronze -> Silver -> Gold")


if __name__ == '__main__':
    unittest.main()