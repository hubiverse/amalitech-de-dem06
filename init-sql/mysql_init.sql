USE staging_db;

-- Bronze Layer
CREATE TABLE IF NOT EXISTS raw_flights (
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
    days_before_departure TEXT
);

CREATE TABLE IF NOT EXISTS invalid_flights (
    row_data JSON,
    error_message TEXT,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Silver Layer
CREATE TABLE IF NOT EXISTS clean_flights (
    airline VARCHAR(100),
    source VARCHAR(10),
    source_name VARCHAR(150),
    destination VARCHAR(10),
    destination_name VARCHAR(150),
    departure_time DATETIME,
    arrival_time DATETIME,
    duration_hrs DECIMAL(5,2),
    stopovers VARCHAR(50),
    aircraft_type VARCHAR(100),
    travel_class VARCHAR(50),
    booking_source VARCHAR(100),
    base_fare DECIMAL(10,2),
    tax_and_surcharge DECIMAL(10,2),
    total_fare DECIMAL(10,2),
    seasonality VARCHAR(50),
    days_before_departure INT
);

-- KPI Tables
CREATE TABLE IF NOT EXISTS kpi_avg_fare (airline VARCHAR(100), avg_total_fare DECIMAL(10,2));
CREATE TABLE IF NOT EXISTS kpi_seasonal_fare (season VARCHAR(50), avg_total_fare DECIMAL(10,2));
CREATE TABLE IF NOT EXISTS kpi_booking_count (airline VARCHAR(100), booking_count INT);
CREATE TABLE IF NOT EXISTS kpi_popular_routes (route VARCHAR(20), route_count INT);
