# NYC Taxi Data Platform – Bruin Pipeline

## Overview

This project implements a complete data pipeline for NYC Taxi trip data using **Bruin** and **DuckDB**.

The objective of this project is to demonstrate how a modern data platform can:

- Ingest raw data using Python assets
- Load static reference data using seed assets
- Transform and standardize raw data
- Apply time-based incremental processing
- Enforce data quality checks
- Build analytics-ready reporting tables
- Manage asset dependencies and lineage

This project was developed as part of the DataTalksClub Data Engineering Zoomcamp – Module 05 (Data Platforms with Bruin).

---

## Architecture

The pipeline follows a layered data architecture:

Ingestion  
→ Staging  
→ Reporting  

### Ingestion Layer

- `ingestion.trips` (Python asset)
  - Downloads taxi trip data
  - Uses append materialization
  - Returns a DataFrame for Bruin to load into DuckDB

- `ingestion.payment_lookup` (Seed asset)
  - Loads a static CSV lookup table
  - Defines primary key and quality checks

### Staging Layer

- `staging.trips` (SQL asset)
  - Strategy: `time_interval`
  - Incremental key: `pickup_datetime`
  - Filters to the run window
  - Deduplicates records using `ROW_NUMBER()`
  - Enriches data via join with payment lookup
  - Applies built-in and custom quality checks

### Reporting Layer

- `reports.trips_report` (SQL asset)
  - Strategy: `time_interval`
  - Aggregates trips and revenue
  - Groups by taxi type and pickup date
  - Produces analytics-ready table

---

## Project Structure

```text
nytaxi-pipeline/
├── .bruin.yml
├── pipeline/
│   ├── pipeline.yml
│   └── assets/
│       ├── ingestion/
│       │   ├── trips.py
│       │   ├── payment_lookup.asset.yml
│       │   ├── payment_lookup.csv
│       │   └── requirements.txt
│       ├── staging/
│       │   └── trips.sql
│       └── reports/
│           └── trips_report.sql
└── README.md
```
---

## Pipeline Configuration

### pipeline.yml

- Pipeline name: `nyc_taxi`
- Schedule: daily
- Start date: 2022-01-01
- Default connection: DuckDB
- Pipeline variable: `taxi_types` (array)

Example variable override:

```bash
bruin run ./pipeline/pipeline.yml --var 'taxi_types=["yellow"]'
```
---

## Materialization Strategies

### Append (Ingestion)

The ingestion layer uses the `append` strategy.

Each pipeline run inserts new rows into the destination table.  
Deduplication and normalization are handled downstream in the staging layer.

---

### Time Interval (Staging & Reporting)

The staging and reporting layers use the `time_interval` strategy.

For each run window:

1. Rows within the specified time window (based on `pickup_datetime`) are deleted.
2. The SELECT query is executed.
3. Only records within the same time window are inserted.

This allows efficient incremental processing by reprocessing only the required date range.

---

## Data Quality

The pipeline enforces data quality using:

- `not_null`
- `unique`
- `non_negative`
- Primary key definitions
- Custom validation checks

Example custom staging check:

Ensures at least one row exists within the run window.

All quality checks pass successfully during pipeline execution.

---

## How to Run

Validate the pipeline:
```bash
bruin validate .
```

Run for a specific time window (first execution):
```bash
bruin run ./pipeline/pipeline.yml \
  --start-date 2022-01-01 \
  --end-date 2022-02-01 \
  --full-refresh
```

Run incrementally:
```bash
bruin run ./pipeline/pipeline.yml \
  --start-date 2022-02-01 \
  --end-date 2022-03-01
```

Override pipeline variables:
```bash
bruin run ./pipeline/pipeline.yml \
  --start-date 2022-01-01 \
  --end-date 2022-02-01 \
  --var 'taxi_types=["yellow"]'
```

---

## Homework Answers

### Question 1 – Required Project Structure

Correct answer:

`.bruin.yml and pipeline/ with pipeline.yml and assets/`

Explanation:

Bruin requires:
- `.bruin.yml` for environment configuration and connections (kept local)
- `pipeline/pipeline.yml` for defining the pipeline
- `pipeline/assets/` for ingestion, staging, and reporting assets

---

### Question 2 – Materialization Strategy for Staging

Correct answer:

`time_interval`

Explanation:

The staging layer processes NYC taxi data partitioned by `pickup_datetime`.  
Using `time_interval` allows incremental rebuilding of only the requested time window, which is efficient and aligns with time-based data organization.

---

### Question 3 – Overriding Pipeline Variables

Correct answer:

```bash
bruin run --var 'taxi_types=["yellow"]'
```
Explanation:

Pipeline variables are defined in pipeline.yml and overridden at runtime using the --var flag with valid JSON syntax.

---

### Question 4 – Running with Dependencies

Correct answer:

```bash
bruin run ingestion/trips.py --downstream
```
Explanation:

The --downstream flag executes the selected asset along with all assets that depend on it.

---

### Question 5 – Quality Check for pickup_datetime

Correct answer:

name: not_null

Explanation:

Adding a 'not_null' check ensures that the 'pickup_datetime' column does not contain NULL values, enforcing data integrity.

---

### Question 6 – Visualizing Dependencies

Correct answer:

```bash
bruin lineage
```
Explanation:

The bruin graph command visualizes the dependency graph between pipeline assets, allowing inspection of lineage and relationships.

---

### Question 7 – First-Time Full Rebuild

Correct answer:

--full-refresh

Explanation:

The '--full-refresh' flag forces a complete rebuild of tables and is required when running the pipeline for the first time on a new database.

---

## Technologies Used

- Bruin
- DuckDB
- Python
- Pandas
- PyArrow
- SQL
- Git

---

## What This Project Demonstrates

- End-to-end data pipeline architecture (ingestion → staging → reporting)
- Time-based incremental materialization using `time_interval`
- Asset dependency management and lineage tracking
- Built-in and custom data quality enforcement
- Seed-based dimensional enrichment
- Practical implementation of a modern data platform using Bruin

---

## Conclusion

This project demonstrates how Bruin can be used to build a structured, production-style data pipeline that handles ingestion, transformation, incremental processing, quality validation, and reporting.

The pipeline design reflects core data platform principles:

- Clear separation of layers
- Incremental processing by time window
- Explicit dependency management
- Strong data quality guarantees
- Reproducible execution via configuration and version control

This implementation provides a strong foundation for scaling toward cloud-native orchestration, distributed execution, and production-grade data platform architectures.