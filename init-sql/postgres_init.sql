\c analytics_db;

-- Gold Layer
CREATE TABLE IF NOT EXISTS clean_flights (
    airline VARCHAR(100),
    source VARCHAR(10),
    source_name VARCHAR(150),
    destination VARCHAR(10),
    destination_name VARCHAR(150),
    departure_time TIMESTAMP,
    arrival_time TIMESTAMP,
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

CREATE TABLE IF NOT EXISTS kpi_avg_fare (airline VARCHAR(100), avg_total_fare DECIMAL(10,2));
CREATE TABLE IF NOT EXISTS kpi_seasonal_fare (season VARCHAR(50), avg_total_fare DECIMAL(10,2));
CREATE TABLE IF NOT EXISTS kpi_booking_count (airline VARCHAR(100), booking_count INT);
CREATE TABLE IF NOT EXISTS kpi_popular_routes (route VARCHAR(20), route_count INT);
