{{ config(materialized='incremental') }}

SELECT
    airflow_run_id,
    JSON_BUILD_OBJECT(
        'airline', airline,
        'source', source,
        'source_name', source_name,
        'destination', destination,
        'destination_name', destination_name,
        'departure_time', departure_time,
        'arrival_time', arrival_time,
        'duration_hrs', duration_hrs,
        'stopovers', stopovers,
        'aircraft_type', aircraft_type,
        'travel_class', travel_class,
        'booking_source', booking_source,
        'base_fare', base_fare,
        'tax_and_surcharge', tax_and_surcharge,
        'total_fare', total_fare,
        'seasonality', seasonality,
        'days_before_departure', days_before_departure,
        'ingested_at', ingested_at
    ) AS row_data,
    CASE
        WHEN CAST(arrival_time AS TIMESTAMP) <= CAST(departure_time AS TIMESTAMP) THEN 'Chronological Error: Arrival before Departure'
        WHEN source = destination THEN 'Geographical Error: Source equals Destination'
        WHEN CAST(base_fare AS NUMERIC(10,2)) < 0 THEN 'Financial Error: Negative Base Fare'
        WHEN LENGTH(source) != 3 OR LENGTH(destination) != 3 THEN 'IATA Error: Invalid Code Length'
        ELSE 'General Validation Failure'
    END AS error_message,
    CAST(ingested_at AS TIMESTAMP) AS ingested_at,
    CURRENT_TIMESTAMP AS inserted_at
FROM {{ source('analytics_db', 'raw_flights') }}
WHERE
    CAST(arrival_time AS TIMESTAMP) <= CAST(departure_time AS TIMESTAMP)
    OR source = destination
    OR CAST(base_fare AS NUMERIC(10,2)) < 0
    OR LENGTH(source) != 3
    OR LENGTH(destination) != 3
    OR airline IS NULL OR airline = ''

{% if is_incremental() %}
  AND CAST(ingested_at AS TIMESTAMP) > (SELECT MAX(ingested_at) FROM {{ this }})
{% endif %}
