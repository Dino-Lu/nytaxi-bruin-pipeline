/* @bruin
name: staging.trips
type: duckdb.sql

# Declare dependencies so `bruin run ... --downstream` and lineage work.
depends:
  - ingestion.trips
  - ingestion.payment_lookup

# - This module expects to use `time_interval` to reprocess only the requested window.
materialization:
  type: table
  # Common strategies you can choose from (see docs for full list):
  # - create+replace (full rebuild)
  # - truncate+insert (full refresh without drop/create)
  # - append (insert new rows only)
  # - delete+insert (refresh partitions based on incremental_key values)
  # - merge (upsert based on primary key)
  # - time_interval (refresh rows within a time window)
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: pickup_datetime
    type: timestamp
    primary_key: true
    checks:
      - name: not_null

custom_checks:
  - name: row_count_greater_than_zero
    description: Ensure that the staging table has at least one row
    query: |
      SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END
      FROM staging.trips
      WHERE pickup_datetime >= '{{ start_datetime }}'
        AND pickup_datetime < '{{ end_datetime }}'
    value: 1
@bruin */

SELECT
    -- timestamps
    t.pickup_datetime,
    t.dropoff_datetime,

    -- locations (rename to consistent style)
    t.pu_location_id  AS pickup_location_id,
    t.do_location_id  AS dropoff_location_id,

    -- metrics
    CAST(t.fare_amount AS DOUBLE) AS fare_amount,

    -- dimensions
    t.taxi_type,
    p.payment_type_name

FROM ingestion.trips t
LEFT JOIN ingestion.payment_lookup p
  ON t.payment_type = p.payment_type_id

-- IMPORTANT for time_interval: filter to the run window
WHERE t.pickup_datetime >= '{{ start_datetime }}'
  AND t.pickup_datetime < '{{ end_datetime }}'

-- dedupe: keep 1 row per natural key inside the window
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY
    t.pickup_datetime,
    t.dropoff_datetime,
    t.pu_location_id,
    t.do_location_id,
    t.fare_amount,
    t.taxi_type,
    t.payment_type
  ORDER BY t.index_level_0
) = 1