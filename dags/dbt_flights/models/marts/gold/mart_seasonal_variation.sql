{{ config(materialized='table') }}

WITH flight_dates AS (
    SELECT
        f.total_fare,
        d.full_date,
        d.month,
        d.day,
        d.seasonality AS dataset_label
    FROM {{ ref('fact_flight_fares') }} f
    JOIN {{ ref('dim_date') }} d ON f.departure_date_id = d.date_id
),

categorized_flights AS (
    SELECT
        total_fare,
        dataset_label,
        CASE
            -- Dataset explicitly says it's a peak
            WHEN dataset_label IN ('Eid', 'Winter Holidays', 'Hajj') THEN 'Peak'

            -- Date-based overrides (Winter Holidays)
            WHEN month = 12 AND day >= 15 THEN 'Peak'
            WHEN month = 1 AND day <= 15 THEN 'Peak'

            -- Bengali New Year (Boishakh)
            WHEN month = 4 AND day BETWEEN 10 AND 20 THEN 'Peak'

            -- Everything else is Off-Peak
            ELSE 'Non-Peak'
        END AS season_type
    FROM flight_dates
)

SELECT
    season_type,
    dataset_label,
    COUNT(*) AS total_bookings,
    ROUND(AVG(total_fare), 2) AS avg_fare,
    ROUND(AVG(total_fare) - FIRST_VALUE(AVG(total_fare)) OVER (ORDER BY season_type DESC), 2) AS price_diff_vs_base
FROM categorized_flights
GROUP BY 1, 2
ORDER BY avg_fare, total_bookings DESC
