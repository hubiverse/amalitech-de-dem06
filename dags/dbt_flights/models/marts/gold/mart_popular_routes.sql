{{ config(materialized='table') }}

SELECT
    r.route_name,
    r.source_name,
    r.destination_name,
    COUNT(f.fact_id) AS total_bookings,
    ROUND(AVG(f.total_fare), 2) AS avg_route_fare
FROM {{ ref('fact_flight_fares') }} f
JOIN {{ ref('dim_route') }} r ON f.route_id = r.route_id
GROUP BY 1, 2, 3
ORDER BY total_bookings DESC
