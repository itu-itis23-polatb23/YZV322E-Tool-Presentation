-- Schema for NYC Yellow Taxi trip data
-- Matches the official TLC Parquet schema (Jan 2024 onwards)
-- This file runs automatically on first `docker-compose up` thanks to
-- the volume mount to /docker-entrypoint-initdb.d/

CREATE TABLE IF NOT EXISTS taxi_trips (
    vendor_id              SMALLINT,
    pickup_datetime        TIMESTAMP,
    dropoff_datetime       TIMESTAMP,
    passenger_count        REAL,
    trip_distance          REAL,
    rate_code_id           REAL,
    store_and_fwd_flag     CHAR(1),
    pu_location_id         INTEGER,
    do_location_id         INTEGER,
    payment_type           BIGINT,
    fare_amount            REAL,
    extra                  REAL,
    mta_tax                REAL,
    tip_amount             REAL,
    tolls_amount           REAL,
    improvement_surcharge  REAL,
    total_amount           REAL,
    congestion_surcharge   REAL,
    airport_fee            REAL
);

-- Indexes that make the demo queries faster on the Pandas side too
-- (so the Polars vs Pandas comparison is fair, not artificially skewed)
CREATE INDEX IF NOT EXISTS idx_pickup_datetime ON taxi_trips (pickup_datetime);
CREATE INDEX IF NOT EXISTS idx_pu_location ON taxi_trips (pu_location_id);

-- Verify table exists
SELECT 'taxi_trips table created successfully' AS status;
