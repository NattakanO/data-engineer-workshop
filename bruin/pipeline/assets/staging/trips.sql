/* @bruin
name: staging.trips
type: duckdb.sql
depends:
  - ingestion.trips
  - ingestion.payment_lookup

# Materialization strategy
# NOTE: `time_interval` can issue a DELETE on the target table before insert.
#       On first run if the target table does not yet exist, this can fail with
#       "Table with name trips does not exist" for DuckDB.
#       Using create+replace ensures first-run behavior is stable.
materialization:
  type: table
  strategy: create+replace

# Basic quality checks (built-in) + one custom check for duplicate trip identifiers.
columns:
  - name: pickup_datetime
    type: timestamp
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    checks:
      - name: not_null
  - name: passenger_count
    type: integer
    checks:
      - name: non_negative
  - name: trip_distance
    type: numeric
    checks:
      - name: non_negative

custom_checks:
  - name: row_count_positive
    description: Ensure staging results in at least one row for the ingested time window.
    query: |
      SELECT COUNT(*)>0
      FROM staging.trips
    value: 1

@bruin */

WITH raw AS (
  SELECT
    tpep_pickup_datetime AS pickup_datetime,
    tpep_dropoff_datetime AS dropoff_datetime,
    pu_location_id AS pickup_location_id,
    do_location_id AS dropoff_location_id,
    taxi_type,
    passenger_count,
    trip_distance,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    extracted_at
  FROM ingestion.trips
  WHERE 1=1 AND tpep_pickup_datetime IS NOT NULL 
  AND fare_amount >= 0 
  AND total_amount >= 0

), deduped AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY
        pickup_datetime,
        dropoff_datetime, 
        pickup_location_id,
        dropoff_location_id,
        fare_amount
      ORDER BY extracted_at DESC 
    ) AS rn
  FROM raw
)

SELECT
  d.pickup_datetime,
  d.dropoff_datetime,
  d.pickup_location_id,
  d.dropoff_location_id,
  d.taxi_type,
  d.passenger_count,
  d.trip_distance,
  d.payment_type,
  COALESCE(p.payment_type_name, 'unknown') AS payment_type_name,
  d.fare_amount,
  d.extra,
  d.mta_tax,
  d.tip_amount,
  d.tolls_amount,
  d.improvement_surcharge,
  d.total_amount,
  d.extracted_at
FROM deduped d
LEFT JOIN ingestion.payment_lookup p
ON d.payment_type= p.payment_type_id
WHERE d.rn = 1;

