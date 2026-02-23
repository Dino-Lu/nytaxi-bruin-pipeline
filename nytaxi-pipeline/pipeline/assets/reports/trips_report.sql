/* @bruin
name: reports.trips_report
type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

ccolumns:
  - name: taxi_type
    type: string
    primary_key: true
    checks:
      - name: not_null

  - name: pickup_date
    type: date
    primary_key: true
    checks:
      - name: not_null

  - name: trips
    type: bigint
    checks:
      - name: non_negative

  - name: revenue
    type: double
    checks:
      - name: non_negative
@bruin */

-- Purpose of reports:
-- - Aggregate staging data for dashboards and analytics
-- Required Bruin concepts:
-- - Filter using `{{ start_datetime }}` / `{{ end_datetime }}` for incremental runs
-- - GROUP BY your dimension + date columns

SELECT
  taxi_type,
  CAST(pickup_datetime AS DATE) AS pickup_date,
  COUNT(*) AS trips,
  SUM(fare_amount) AS revenue
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY 1,2