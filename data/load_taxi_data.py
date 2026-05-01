import argparse
import os
import sys
import time
from pathlib import Path

import polars as pl
import requests
from tqdm import tqdm

DATA_DIR = Path(__file__).parent
TLC_BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def get_pg_uri() -> str:
    user = os.environ.get("POSTGRES_USER", "polars")
    pw = os.environ.get("POSTGRES_PASSWORD", "polars_demo_2026")
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "taxi")
    return f"postgresql://{user}:{pw}@{host}:{port}/{db}"


def download_parquet(month: str) -> Path:
    filename = f"yellow_tripdata_{month}.parquet"
    local_path = DATA_DIR / filename

    if local_path.exists():
        print(f"[skip] {filename} already downloaded "
              f"({local_path.stat().st_size / 1e6:.1f} MB)")
        return local_path

    url = f"{TLC_BASE}/{filename}"
    print(f"[get ] {url}")
    response = requests.get(url, stream=True, timeout=60)
    response.raise_for_status()
    total = int(response.headers.get("content-length", 0))

    with open(local_path, "wb") as f, tqdm(
        total=total, unit="B", unit_scale=True, desc=filename
    ) as pbar:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
            pbar.update(len(chunk))

    print(f"[done] saved to {local_path}")
    return local_path


def load_to_postgres(parquet_path: Path, pg_uri: str, append: bool = False) -> int:
    print(f"[read] {parquet_path.name}")
    t0 = time.time()

    df = pl.read_parquet(parquet_path)

    rename_map = {
        "VendorID": "vendor_id",
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "RatecodeID": "rate_code_id",
        "PULocationID": "pu_location_id",
        "DOLocationID": "do_location_id",
        "store_and_fwd_flag": "store_and_fwd_flag",
        "Airport_fee": "airport_fee",
    }
    df = df.rename({k: v for k, v in rename_map.items() if k in df.columns})

    df = df.with_columns([
        pl.col("vendor_id").cast(pl.Int16, strict=False),
        pl.col("payment_type").cast(pl.Int64, strict=False),
        pl.col("pu_location_id").cast(pl.Int32, strict=False),
        pl.col("do_location_id").cast(pl.Int32, strict=False),
    ])

    print(f"[read] {len(df):,} rows in {time.time() - t0:.1f}s")

    print(f"[load] writing to Postgres...")
    t0 = time.time()
    if_exists = "append" if append else "replace"
    df.write_database(
        table_name="taxi_trips",
        connection=pg_uri,
        if_table_exists=if_exists,
        engine="sqlalchemy",
    )
    print(f"[load] done in {time.time() - t0:.1f}s")
    return len(df)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--month",
        default="2024-01",
        help="Month to load (YYYY-MM format, default: 2024-01)",
    )
    parser.add_argument(
        "--append",
        action="store_true",
        help="Append to existing table instead of replacing",
    )
    args = parser.parse_args()

    pg_uri = get_pg_uri()
    print(f"[init] target: {pg_uri.replace(os.environ.get('POSTGRES_PASSWORD', ''), '***')}")

    parquet_path = download_parquet(args.month)
    rows = load_to_postgres(parquet_path, pg_uri, append=args.append)

    print()
    print(f"[ok  ] loaded {rows:,} rows into taxi_trips")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[abort] interrupted by user")
        sys.exit(130)
