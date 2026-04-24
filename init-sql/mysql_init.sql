USE staging_db;

-- Bronze Layer
CREATE TABLE IF NOT EXISTS raw_flights (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    airflow_run_id VARCHAR(255),
    airline VARCHAR(255),
    source VARCHAR(10),
    source_name VARCHAR(255),
    destination VARCHAR(10),
    destination_name VARCHAR(255),
    departure_time VARCHAR(50),
    arrival_time VARCHAR(50),
    duration_hrs VARCHAR(50),
    stopovers VARCHAR(50),
    aircraft_type VARCHAR(100),
    travel_class VARCHAR(50),
    booking_source VARCHAR(100),
    base_fare VARCHAR(50),
    tax_and_surcharge VARCHAR(50),
    total_fare VARCHAR(50),
    seasonality VARCHAR(50),
    days_before_departure VARCHAR(50),
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Invalid
CREATE TABLE IF NOT EXISTS invalid_flights (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    row_data JSON NOT NULL,
    error_message VARCHAR(255) NOT NULL,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Silver Layer
CREATE TABLE IF NOT EXISTS clean_flights (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    airline VARCHAR(100) NOT NULL,
    source CHAR(3) NOT NULL,
    source_name VARCHAR(150),
    destination CHAR(3) NOT NULL,
    destination_name VARCHAR(150),
    departure_time DATETIME NOT NULL,
    arrival_time DATETIME NOT NULL,
    duration_hrs DECIMAL(5,2),
    stopovers VARCHAR(50),
    aircraft_type VARCHAR(100),
    travel_class VARCHAR(50),
    booking_source VARCHAR(100),
    base_fare DECIMAL(10,2),
    tax_and_surcharge DECIMAL(10,2),
    total_fare DECIMAL(10,2),
    seasonality VARCHAR(50),
    days_before_departure INT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_route ON clean_flights(source, destination);
CREATE INDEX idx_departure_time ON clean_flights(departure_time);


-- KPI Tables
CREATE TABLE IF NOT EXISTS kpi_avg_fare (airline VARCHAR(100), avg_total_fare DECIMAL(10,2));
CREATE TABLE IF NOT EXISTS kpi_seasonal_fare (season VARCHAR(50), avg_total_fare DECIMAL(10,2));
CREATE TABLE IF NOT EXISTS kpi_booking_count (airline VARCHAR(100), booking_count INT);
CREATE TABLE IF NOT EXISTS kpi_popular_routes (route VARCHAR(20), route_count INT);
