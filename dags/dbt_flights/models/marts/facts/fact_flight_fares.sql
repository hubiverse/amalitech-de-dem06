{{ config(
    materialized='table',
    post_hook=[
      "CREATE INDEX IF NOT EXISTS idx_fact_route ON {{ this }} (route_id)",
      "CREATE INDEX IF NOT EXISTS idx_fact_date ON {{ this }} (departure_date_id)",
      "CREATE INDEX IF NOT EXISTS idx_fact_airline ON {{ this }} (airline)"
    ]
) }}

SELECT
    -- Generate Unique Fact ID
    MD5(CAST(stg.raw_id AS TEXT) || stg.airflow_run_id) AS fact_id,
    stg.airflow_run_id,

    -- Surrogate Keys
    MD5(stg.source_code || stg.destination_code) AS route_id,
    MD5(stg.aircraft_type) AS aircraft_id,
    MD5(stg.travel_class || stg.booking_source || stg.stopovers) AS booking_profile_id,

    -- Date Surrogate Keys (Format: YYYYMMDD as Integer)
    CAST(TO_CHAR(stg.departure_time, 'YYYYMMDD') AS INTEGER) AS departure_date_id,
    CAST(TO_CHAR(stg.arrival_time, 'YYYYMMDD') AS INTEGER) AS arrival_date_id,

    -- Degenerate Dimensions
    stg.airline,
    stg.departure_time,
    stg.arrival_time,

    -- Measures
    stg.duration_hrs,
    stg.days_before_departure,
    stg.base_fare,
    stg.tax_and_surcharge,
    stg.total_fare

FROM {{ ref('stg_valid_flights') }} stg
