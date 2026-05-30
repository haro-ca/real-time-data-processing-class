-- Lesson 3: NYC Taxi trips table for OLAP benchmarks
-- Schema matches the 2025 parquet superset (20 columns). Older years are loaded
-- with NULLs for columns that didn't exist yet (e.g. cbd_congestion_fee).
CREATE TABLE IF NOT EXISTS trips (
    vendor_id              INT,
    pickup_datetime        TIMESTAMP,
    dropoff_datetime       TIMESTAMP,
    passenger_count        INT,
    trip_distance          NUMERIC(10, 2),
    rate_code_id           INT,
    store_and_fwd_flag     CHAR(1),
    pickup_location_id     INT,
    dropoff_location_id    INT,
    payment_type           INT,
    fare_amount            NUMERIC(10, 2),
    extra                  NUMERIC(10, 2),
    mta_tax                NUMERIC(10, 2),
    tip_amount             NUMERIC(10, 2),
    tolls_amount           NUMERIC(10, 2),
    improvement_surcharge  NUMERIC(10, 2),
    total_amount           NUMERIC(10, 2),
    congestion_surcharge   NUMERIC(10, 2),
    airport_fee            NUMERIC(10, 2),
    cbd_congestion_fee     NUMERIC(10, 2)
);

-- Index is created AFTER the COPY in load_postgres.py (much faster than maintaining
-- it during a 128M-row bulk load).

