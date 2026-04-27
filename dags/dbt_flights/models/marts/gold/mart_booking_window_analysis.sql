{{ config(materialized='table') }}

SELECT
    CASE
        WHEN days_before_departure < 7 THEN 'Last Minute (0-7 days)'
        WHEN days_before_departure BETWEEN 7 AND 21 THEN 'Advanced (7-21 days)'
        ELSE 'Early Bird (21+ days)'
    END AS booking_window,
    airline,
    COUNT(*) AS booking_count,
    ROUND(AVG(total_fare), 2) AS avg_fare
FROM {{ ref('fact_flight_fares') }}
GROUP BY 1, 2
ORDER BY 1, 3 DESC
