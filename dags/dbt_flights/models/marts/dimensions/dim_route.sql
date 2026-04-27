{{ config(materialized='table') }}

SELECT DISTINCT
    -- Postgres uses || for concatenation and requires text cast for MD5
    MD5(CAST(source_code || destination_code AS TEXT)) AS route_id,
    source_code,
    source_name,
    destination_code,
    destination_name,
    CAST(source_code || '-' || destination_code AS VARCHAR(7)) AS route_name
FROM {{ ref('stg_valid_flights') }}
