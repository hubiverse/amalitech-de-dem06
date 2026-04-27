{{ config(materialized='view') }}

SELECT
    id AS raw_id,
    airflow_run_id,
    airline,
    source AS source_code,
    TRIM(SPLIT_PART(source_name, ',', 1)) AS source_name,
    destination AS destination_code,
    TRIM(SPLIT_PART(destination_name, ',', 1)) AS destination_name,

    CAST(departure_time AS TIMESTAMP) AS departure_time,
    CAST(arrival_time AS TIMESTAMP) AS arrival_time,
    CAST(duration_hrs AS NUMERIC(10,4)) AS duration_hrs,

    stopovers,
    aircraft_type,
    travel_class,
    booking_source,

    CAST(base_fare AS NUMERIC(10,2)) AS base_fare,
    CAST(tax_and_surcharge AS NUMERIC(10,2)) AS tax_and_surcharge,
    -- We recalculate the total to guarantee math integrity in the Fact table
    (CAST(base_fare AS NUMERIC(10,2)) + CAST(tax_and_surcharge AS NUMERIC(10,2))) AS total_fare,

    seasonality,
    CAST(days_before_departure AS INTEGER) AS days_before_departure

FROM {{ source('analytics_db', 'raw_flights') }}
WHERE
    -- Fare must be realistic
    CAST(base_fare AS NUMERIC(10,2)) >= 0
    AND CAST(tax_and_surcharge AS NUMERIC(10,2)) >= 0

    -- Time must be logical (Arrival after Departure)
    AND CAST(arrival_time AS TIMESTAMP) > CAST(departure_time AS TIMESTAMP)

    -- Geography must be logical
    AND source != destination

    -- IATA length check
    AND LENGTH(source) = 3
    AND LENGTH(destination) = 3

    -- Critical strings must not be empty
    AND airline IS NOT NULL AND airline != ''
    AND travel_class IS NOT NULL
