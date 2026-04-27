{{ config(materialized='table') }}

SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['travel_class', 'booking_source', 'stopovers']) }} AS booking_profile_id,
    travel_class,
    booking_source,
    stopovers
FROM {{ ref('stg_valid_flights') }}
