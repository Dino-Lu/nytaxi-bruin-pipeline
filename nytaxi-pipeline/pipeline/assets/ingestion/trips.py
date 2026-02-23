"""@bruin
name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: taxi_type
    type: string
    description: "Taxi type (yellow/green)"
  - name: pickup_datetime
    type: timestamp
    description: "When the meter was engaged"
  - name: dropoff_datetime
    type: timestamp
    description: "When the meter was disengaged"
@bruin"""

import os
import json
from datetime import date
import pandas as pd

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def _env(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v


def _month_starts(start: date, end: date) -> list[date]:
    """Return list of month start dates that intersect [start, end)."""
    months = []
    cur = date(start.year, start.month, 1)
    while cur < end:
        months.append(cur)
        if cur.month == 12:
            cur = date(cur.year + 1, 1, 1)
        else:
            cur = date(cur.year, cur.month + 1, 1)
    return months


def materialize():
    # Bruin supplies these based on pipeline schedule/run window
    start_date = date.fromisoformat(_env("BRUIN_START_DATE"))
    end_date = date.fromisoformat(_env("BRUIN_END_DATE"))

    # Pipeline variables arrive as JSON in BRUIN_VARS
    vars_json = json.loads(os.environ.get("BRUIN_VARS", "{}"))
    taxi_types = vars_json.get("taxi_types", ["yellow"])

    frames: list[pd.DataFrame] = []

    for m in _month_starts(start_date, end_date):
        y = m.year
        mm = f"{m.month:02d}"

        for taxi_type in taxi_types:
            # TLC parquet naming pattern
            # yellow_tripdata_YYYY-MM.parquet / green_tripdata_YYYY-MM.parquet
            url = f"{BASE_URL}/{taxi_type}_tripdata_{y}-{mm}.parquet"

            try:
                df = pd.read_parquet(url)
            except Exception as e:
                # Keep the pipeline resilient (missing file/month/etc.)
                print(f"Skipping {url}: {e}")
                continue

            df["taxi_type"] = taxi_type

            # Standardize timestamp column names for downstream assets
            if taxi_type == "yellow":
                df = df.rename(
                    columns={
                        "tpep_pickup_datetime": "pickup_datetime",
                        "tpep_dropoff_datetime": "dropoff_datetime",
                    }
                )
            elif taxi_type == "green":
                df = df.rename(
                    columns={
                        "lpep_pickup_datetime": "pickup_datetime",
                        "lpep_dropoff_datetime": "dropoff_datetime",
                    }
                )

            # Ensure datetime types
            if "pickup_datetime" in df.columns:
                df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"], errors="coerce")
            if "dropoff_datetime" in df.columns:
                df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"], errors="coerce")

            frames.append(df)

    if not frames:
        # Return an empty DF with at least the declared columns
        return pd.DataFrame(columns=["taxi_type", "pickup_datetime", "dropoff_datetime"])

    final_df = pd.concat(frames, ignore_index=True)

    # Optional: keep only records in the window (helps if a file contains overlap)
    final_df = final_df[
        (final_df["pickup_datetime"] >= pd.Timestamp(start_date))
        & (final_df["pickup_datetime"] < pd.Timestamp(end_date))
    ]

    return final_df