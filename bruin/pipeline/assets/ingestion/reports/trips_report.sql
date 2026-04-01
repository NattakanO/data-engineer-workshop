/* @bruin
name: trips_report
type: duckdb.sql
depends:
  - staging.trips

materialization:
  type: table

columns:
  - name: report_date
    type: date
    description: Date of the reporting window (pickup date)
    primary_key: true
  - name: vendorid
    type: string
    description: Taxi vendor identifier (if available)
    primary_key: true
  - name: trip_count
    type: bigint
    description: Total number of trips
    checks:
      - name: non_negative
  - name: total_distance
    type: double
    description: Total trip distance for the day
    checks:
      - name: non_negative
  - name: total_fare
    type: double
    description: Total fare amount for the day
    checks:
      - name: non_negative
  - name: average_fare
    type: double
    description: Average fare per trip
    checks:
      - name: non_negative

@bruin */

SELECT
  CAST(pickup_datetime AS DATE) AS report_date,
  taxi_type,
  payment_type_name,
  COUNT(*) AS trip_count,
  SUM(COALESCE(passenger_count, 0.0)) AS total_passengers,
  SUM(COALESCE(trip_distance, 0.0)) AS total_distance,
  SUM(COALESCE(fare_amount, 0.0)) AS total_fare,
  SUM(COALESCE(tip_amount, 0.0)) AS total_tips,

  AVG(COALESCE(fare_amount, 0)) AS average_fare,
  AVG(COALESCE(trip_distance, 0)) AS average_trip_distance,
  AVG(COALESCE(passenger_count, 0)) AS average_passengers,

FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY
  CAST(pickup_datetime AS DATE),
  taxi_type,
  payment_type_name;
