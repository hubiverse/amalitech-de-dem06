{{ config(materialized='table') }}

SELECT DISTINCT
    MD5(CAST(travel_class || booking_source || stopovers AS TEXT)) AS booking_profile_id,
    travel_class,
    booking_source,
    stopovers
FROM {{ ref('stg_valid_flights') }}
