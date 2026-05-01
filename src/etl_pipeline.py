"""
Run:
    docker compose exec app python src/05_etl_pipeline.py
"""
import logging
import os
import sys
import time
from contextlib import contextmanager
from datetime import datetime

import polars as pl
from sqlalchemy import create_engine, text


# ----------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------
SOURCE_TABLE = "taxi_trips"
TARGET_TABLE = "taxi_hourly_summary"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("etl")


def get_pg_uri() -> str:
    user = os.environ.get("POSTGRES_USER", "polars")
    pw = os.environ.get("POSTGRES_PASSWORD", "polars_demo_2026")
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "taxi")
    return f"postgresql://{user}:{pw}@{host}:{port}/{db}"


@contextmanager
def stage(name: str):
    """Log a pipeline stage with elapsed time."""
    log.info(f"━━━ {name}")
    t0 = time.time()
    try:
        yield
    finally:
        log.info(f"    done in {time.time() - t0:.2f}s")


# ----------------------------------------------------------------------
# 1. EXTRACT — pull raw rows from the source Postgres table
# ----------------------------------------------------------------------
def extract(pg_uri: str) -> pl.DataFrame:
    query = f"""
        SELECT pickup_datetime, dropoff_datetime, passenger_count,
               trip_distance, fare_amount, tip_amount, total_amount,
               pu_location_id, payment_type
        FROM {SOURCE_TABLE}
        WHERE pickup_datetime IS NOT NULL
    """
    df = pl.read_database_uri(query=query, uri=pg_uri, engine="connectorx")
    log.info(f"    extracted {len(df):,} rows "
             f"({df.estimated_size('mb'):.0f} MB in Arrow memory)")
    return df


# ----------------------------------------------------------------------
# 2. TRANSFORM — clean + enrich + aggregate using the lazy API
# ----------------------------------------------------------------------
def transform(df: pl.DataFrame) -> pl.DataFrame:
    """Build an hourly summary of taxi activity per pickup zone.

    Uses the lazy API so the optimizer can fuse the multiple filters,
    the with_columns step, and the group-by into a single pass over
    the data.
    """
    summary = (
        df.lazy()   
          .filter(pl.col("fare_amount") > 0)
          .filter(pl.col("fare_amount") < 500)          
          .filter(pl.col("trip_distance") > 0)
          .filter(pl.col("trip_distance") < 100)
          .filter(pl.col("passenger_count") >= 1)
          .filter(pl.col("passenger_count") <= 6)
          .with_columns([
              pl.col("pickup_datetime").dt.truncate("1h").alias("hour_bucket"),
              pl.col("pickup_datetime").dt.weekday().alias("day_of_week"),
              pl.when(pl.col("payment_type") == 1)
                .then(pl.col("tip_amount") / pl.col("fare_amount") * 100)
                .otherwise(None)
                .alias("tip_pct"),
          ])
          # aggregation: group by (zone, hour) → one row per bucket
          .group_by(["pu_location_id", "hour_bucket", "day_of_week"])
          .agg([
              pl.len().alias("trip_count"),
              pl.col("trip_distance").mean().round(3).alias("avg_distance"),
              pl.col("fare_amount").mean().round(2).alias("avg_fare"),
              pl.col("total_amount").sum().round(2).alias("total_revenue"),
              pl.col("tip_pct").mean().round(2).alias("avg_tip_pct"),
              pl.col("passenger_count").mean().round(2).alias("avg_passengers"),
          ])
          .filter(pl.col("trip_count") >= 5)        
          .sort(["hour_bucket", "pu_location_id"])
    )

    log.info("    optimized query plan:")
    for line in summary.explain(optimized=True).splitlines():
        log.info(f"      {line}")

    result = summary.collect()
    log.info(f"    transformed → {len(result):,} aggregated rows "
             f"({result.estimated_size('mb'):.1f} MB)")
    return result


# ----------------------------------------------------------------------
# 3. LOAD — write the result back to Postgres as a new table
# ----------------------------------------------------------------------
def load(df: pl.DataFrame, pg_uri: str) -> None:
    """Replace the target summary table with the new aggregation."""
    df.write_database(
        table_name=TARGET_TABLE,
        connection=pg_uri,
        if_table_exists="replace",
        engine="sqlalchemy",
    )
    log.info(f"    wrote {len(df):,} rows to {TARGET_TABLE}")
    
# ----------------------------------------------------------------------
# 4. VERIFY — query the target table to confirm the load worked
# ----------------------------------------------------------------------
def verify(pg_uri: str) -> None:
    engine = create_engine(pg_uri)
    with engine.connect() as conn:
        # 4a — row count + date range
        check = conn.execute(text(f"""
            SELECT COUNT(*)                AS row_count,
                   MIN(hour_bucket)        AS earliest_hour,
                   MAX(hour_bucket)        AS latest_hour,
                   COUNT(DISTINCT pu_location_id) AS unique_zones
            FROM {TARGET_TABLE}
        """)).mappings().first()

        log.info(f"    row count    : {check['row_count']:,}")
        log.info(f"    time range   : {check['earliest_hour']} → {check['latest_hour']}")
        log.info(f"    unique zones : {check['unique_zones']}")

        # 4b — top 5 busiest (zone, hour) buckets
        top = conn.execute(text(f"""
            SELECT pu_location_id, hour_bucket, trip_count, total_revenue
            FROM {TARGET_TABLE}
            ORDER BY trip_count DESC
            LIMIT 5
        """)).mappings().all()

        log.info("    top 5 busiest (zone, hour) buckets:")
        for row in top:
            log.info(f"      zone {row['pu_location_id']:>3}  "
                     f"{row['hour_bucket']}  "
                     f"trips={row['trip_count']:>4}  "
                     f"revenue=${row['total_revenue']:>10,.2f}")


# ----------------------------------------------------------------------
# Pipeline orchestrator
# ----------------------------------------------------------------------
def main():
    pg_uri = get_pg_uri()
    log.info(f"╔══════════════════════════════════════════════════════════════╗")
    log.info(f"║  Polars ETL Pipeline                                         ║")
    log.info(f"║  source:  {SOURCE_TABLE:<50} ║")
    log.info(f"║  target:  {TARGET_TABLE:<50} ║")
    log.info(f"║  started: {datetime.now().isoformat(timespec='seconds'):<50} ║")
    log.info(f"╚══════════════════════════════════════════════════════════════╝")

    pipeline_start = time.time()

    with stage("EXTRACT — read raw trips from Postgres"):
        raw = extract(pg_uri)

    with stage("TRANSFORM — clean + enrich + aggregate (lazy)"):
        summary = transform(raw)

    with stage("LOAD — write summary back to Postgres"):
        load(summary, pg_uri)
        
    with stage("VERIFY — sanity-check the loaded table"):
        verify(pg_uri)

    log.info(f"━━━ pipeline finished in {time.time() - pipeline_start:.2f}s")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.exception(f"pipeline failed: {e}")
        sys.exit(1)
