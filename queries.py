# --- DDL: Create Tables ---
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

# --- DML: Insert/Copy Queries ---
metadata_insert_success = """
    INSERT INTO etl.pipeline_metadata(pipeline_name,last_successful_load_month,last_run_status,last_run_timestamp, error_message)
    VALUES (%s,%s,%s,NOW(), NULL)
    ON CONFLICT(pipeline_name)
    DO UPDATE SET last_successful_load_month=EXCLUDED.last_successful_load_month,
                  last_run_status=EXCLUDED.last_run_status,
                  last_run_timestamp=EXCLUDED.last_run_timestamp,
                  error_message=NULL
"""

metadata_insert_fail = """
    INSERT INTO etl.pipeline_metadata(pipeline_name, last_run_status, last_run_timestamp, error_message)
    VALUES (%s, %s, NOW(), %s)
    ON CONFLICT(pipeline_name)
    DO UPDATE SET last_run_status=EXCLUDED.last_run_status,
                  last_run_timestamp=EXCLUDED.last_run_timestamp,
                  error_message=EXCLUDED.error_message
"""

metadata_insert_no_data = """
    INSERT INTO etl.pipeline_metadata(pipeline_name,last_run_status,last_run_timestamp, error_message)
    VALUES (%s,%s,NOW(), NULL)
    ON CONFLICT(pipeline_name)
    DO UPDATE SET last_run_status=EXCLUDED.last_run_status,
                  last_run_timestamp=EXCLUDED.last_run_timestamp,
                  error_message=NULL
"""

gold_daily_insert = """
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
"""

gold_monthly_insert = """
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
"""

gold_payment_insert = """
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
"""

gold_vendor_insert = """
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
"""

gold_zone_insert = """
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
"""