"""@bruin
name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default

materialization:
  type: table
  strategy: append


@bruin"""

# TODO: Add imports needed for your ingestion (e.g., pandas, requests).
# - Put dependencies in the nearest `requirements.txt` (this template has one at the pipeline root).
# Docs: https://getbruin.com/docs/bruin/assets/python


import json
import os
from datetime import datetime, date, timedelta
from io import StringIO

import pandas as pd
import requests


def _parse_date(date_str, key):
    if not date_str:
        raise ValueError(f"Environment variable {key} is required")
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def _iter_months(start, end):
    # Generator for YYYY-MM if start/end span multiple months
    current = date(start.year, start.month, 1)
    while current < end:
        yield current.year, current.month
        next_month = current.month + 1
        next_year = current.year
        if next_month > 12:
            next_month = 1
            next_year += 1
        current = date(next_year, next_month, 1)


def _local_parquet_path(taxi_type, year, month):
    base_root = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "nyc-taxi-data-pipeline", "taxi_rides_ny", "data")
    )
    return os.path.join(base_root, taxi_type, f"{taxi_type}_tripdata_{year}-{month:02d}.parquet")


def _source_url(taxi_type, year, month):
    # Public sample data (DataTalksClub mirror) in small, manageable partitions
    return (
        f"https://raw.githubusercontent.com/DataTalksClub/nyc-tlc-data/main/"
        f"{taxi_type}/{year}/{month:02d}.csv"
    )


def _download_monthly_data(taxi_type, year, month):
    local_path = _local_parquet_path(taxi_type, year, month)
    if os.path.exists(local_path):
        return pd.read_parquet(local_path)

    url = _source_url(taxi_type, year, month)
    resp = requests.get(url, timeout=60)
    if resp.status_code != 200:
        raise RuntimeError(f"Failed to download {url} (status={resp.status_code})")

    return pd.read_csv(StringIO(resp.text))


def materialize():
    # 1) Window inputs from Bruin runtime
    start_date = _parse_date(os.getenv("BRUIN_START_DATE"), "BRUIN_START_DATE")
    end_date = _parse_date(os.getenv("BRUIN_END_DATE"), "BRUIN_END_DATE")

    if start_date >= end_date:
        raise ValueError("BRUIN_START_DATE must be before BRUIN_END_DATE")

    # 2) Pipeline variables from BRUIN_VARS JSON
    bruin_vars = json.loads(os.getenv("BRUIN_VARS", "{}"))
    taxi_types = bruin_vars.get("taxi_types", ["yellow"])
    if not taxi_types:
        taxi_types = ["yellow"]

    # 3) Build full rows for the target window
    dfs = []
    for taxi_type in taxi_types:
        for year, month in _iter_months(start_date, end_date):
            try:
                raw_df = _download_monthly_data(taxi_type, year, month)
            except Exception as exc:
                # Ignore missing months for robust incremental runs
                # For strict behavior, re-raise instead.
                print(f"warning: source month missing or fetch failure for {taxi_type} {year}-{month:02d}: {exc}")
                continue

            if raw_df.empty:
                continue

            # Standardize timestamps and location IDs
            if "tpep_pickup_datetime" in raw_df.columns:
                raw_df["tpep_pickup_datetime"] = pd.to_datetime(raw_df["tpep_pickup_datetime"], errors="coerce")
                raw_df["pickup_datetime"] = raw_df["tpep_pickup_datetime"]
            elif "pickup_datetime" in raw_df.columns:
                raw_df["pickup_datetime"] = pd.to_datetime(raw_df["pickup_datetime"], errors="coerce")

            if "tpep_dropoff_datetime" in raw_df.columns:
                raw_df["tpep_dropoff_datetime"] = pd.to_datetime(raw_df["tpep_dropoff_datetime"], errors="coerce")
                raw_df["dropoff_datetime"] = raw_df["tpep_dropoff_datetime"]
            elif "dropoff_datetime" in raw_df.columns:
                raw_df["dropoff_datetime"] = pd.to_datetime(raw_df["dropoff_datetime"], errors="coerce")

            if "PULocationID" in raw_df.columns:
                raw_df["pu_location_id"] = raw_df["PULocationID"]
            if "DOLocationID" in raw_df.columns:
                raw_df["do_location_id"] = raw_df["DOLocationID"]

            raw_df = raw_df.assign(
                taxi_type=taxi_type,
                extracted_at=datetime.utcnow().isoformat(timespec="seconds") + "Z",
            )

            # Keep only window rows for the target partition interval
            if "pickup_datetime" in raw_df.columns:
                mask = (
                    (raw_df["pickup_datetime"] >= pd.Timestamp(start_date))
                    & (raw_df["pickup_datetime"] < pd.Timestamp(end_date))
                )
                raw_df = raw_df.loc[mask]

            dfs.append(raw_df)

    expected_columns = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "pickup_datetime",
        "dropoff_datetime",
        "pu_location_id",
        "do_location_id",
        "taxi_type",
        "passenger_count",
        "trip_distance",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "extracted_at",
    ]

    if not dfs:
        # Return empty dataframe with explicit schema so destination table is created on first run.
        df = pd.DataFrame(columns=expected_columns)
        df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
        df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"])
        return df

    final_df = pd.concat(dfs, ignore_index=True)

    # Add the same canonical timestamp columns for staging/time-based dedupe
    if "pickup_datetime" not in final_df.columns and "tpep_pickup_datetime" in final_df.columns:
        final_df["pickup_datetime"] = pd.to_datetime(final_df["tpep_pickup_datetime"], errors="coerce")
    if "dropoff_datetime" not in final_df.columns and "tpep_dropoff_datetime" in final_df.columns:
        final_df["dropoff_datetime"] = pd.to_datetime(final_df["tpep_dropoff_datetime"], errors="coerce")

    # Keep consistent ingestion column order, missing columns remain null.
    for col in expected_columns:
        if col not in final_df.columns:
            final_df[col] = pd.NA

    return final_df[expected_columns]


