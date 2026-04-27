{{ config(materialized='table') }}

SELECT DISTINCT
    CAST(TO_CHAR(departure_time, 'YYYYMMDD') AS INTEGER) AS date_id,
    CAST(departure_time AS DATE) AS full_date,
    CAST(EXTRACT(YEAR FROM departure_time) AS INTEGER) AS year,
    CAST(EXTRACT(MONTH FROM departure_time) AS INTEGER) AS month,
    CAST(EXTRACT(DAY FROM departure_time) AS INTEGER) AS day,
    seasonality,
    CASE WHEN EXTRACT(DOW FROM departure_time) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
FROM {{ ref('stg_valid_flights') }}
