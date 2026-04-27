{{ config(materialized='table') }}

SELECT DISTINCT
    MD5(CAST(aircraft_type AS TEXT)) AS aircraft_id,
    aircraft_type
FROM {{ ref('stg_valid_flights') }}
