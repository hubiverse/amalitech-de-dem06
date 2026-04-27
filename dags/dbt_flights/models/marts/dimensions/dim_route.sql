{{ config(materialized='table') }}

SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['source_code', 'destination_code']) }} AS route_id,
    source_code,
    source_name,
    destination_code,
    destination_name,
    CAST(source_code || '-' || destination_code AS VARCHAR(7)) AS route_name
FROM {{ ref('stg_valid_flights') }}
