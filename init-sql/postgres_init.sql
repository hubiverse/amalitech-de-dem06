\c analytics_db;

CREATE TABLE IF NOT EXISTS raw_flights (
    id SERIAL PRIMARY KEY,
    airflow_run_id TEXT,
    airline TEXT,
    source TEXT,
    source_name TEXT,
    destination TEXT,
    destination_name TEXT,
    departure_time TEXT,
    arrival_time TEXT,
    duration_hrs TEXT,
    stopovers TEXT,
    aircraft_type TEXT,
    travel_class TEXT,
    booking_source TEXT,
    base_fare TEXT,
    tax_and_surcharge TEXT,
    total_fare TEXT,
    seasonality TEXT,
    days_before_departure TEXT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
