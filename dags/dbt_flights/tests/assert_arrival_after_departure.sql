SELECT *
FROM {{ ref('fact_flight_fares') }}
WHERE arrival_time <= departure_time
