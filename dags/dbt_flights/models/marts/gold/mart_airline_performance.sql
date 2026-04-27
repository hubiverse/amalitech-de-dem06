{{ config(materialized='table') }}

SELECT
    airline,
    COUNT(fact_id) AS total_bookings,
    ROUND(AVG(total_fare), 2) AS avg_fare,
    ROUND(AVG(base_fare), 2) AS avg_base_fare,
    ROUND(AVG(tax_and_surcharge), 2) AS avg_tax_surcharge
FROM {{ ref('fact_flight_fares') }}
GROUP BY 1
ORDER BY total_bookings DESC
