{{ config(materialized='table') }}

SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['aircraft_type']) }} AS aircraft_id,
    aircraft_type
FROM {{ ref('stg_valid_flights') }}
