USE staging_db;

-- ======================================
-- Bronze Layer
-- ======================================
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

-- ==========================================
-- Invalid Flights
-- ==========================================
-- CREATE TABLE IF NOT EXISTS invalid_flights (
--     id BIGINT AUTO_INCREMENT PRIMARY KEY,
--     airflow_run_id VARCHAR(255),
--     row_data JSON NOT NULL,
--     error_message VARCHAR(255) NOT NULL,
--     inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
-- );


-- ==========================================
-- SILVER LAYER: DIMENSIONS
-- ==========================================

-- Route Dimension
-- CREATE TABLE IF NOT EXISTS dim_route (
--     route_id INT AUTO_INCREMENT PRIMARY KEY,
--     source_code CHAR(3) NOT NULL,
--     source_name VARCHAR(255),
--     destination_code CHAR(3) NOT NULL,
--     destination_name VARCHAR(255),
--     route_name CHAR(7) NOT NULL,     -- e.g., 'DAC-LHR'
--     UNIQUE KEY unq_route (source_code, destination_code)
-- );

-- Aircraft Dimension
-- CREATE TABLE IF NOT EXISTS dim_aircraft (
--     aircraft_id INT AUTO_INCREMENT PRIMARY KEY,
--     aircraft_type VARCHAR(100) NOT NULL,
--     UNIQUE KEY unq_aircraft (aircraft_type)
-- );

-- Booking Profile Dimension
-- CREATE TABLE IF NOT EXISTS dim_booking_profile (
--     booking_profile_id INT AUTO_INCREMENT PRIMARY KEY,
--     travel_class VARCHAR(50) NOT NULL,
--     booking_source VARCHAR(100) NOT NULL,
--     stopovers VARCHAR(50) NOT NULL,
--     UNIQUE KEY unq_booking_profile (travel_class, booking_source, stopovers)
-- );

-- Date Dimension
-- CREATE TABLE IF NOT EXISTS dim_date (
--     date_id INT PRIMARY KEY,         -- e.g., 20260424
--     full_date DATE NOT NULL,
--     year INT NOT NULL,
--     month INT NOT NULL,
--     day INT NOT NULL,
--     seasonality VARCHAR(50),
--     is_weekend BOOLEAN
-- );

-- ==========================================
-- SILVER LAYER: FACT TABLE
-- ==========================================

-- CREATE TABLE IF NOT EXISTS fact_flight_fares (
--     fact_id BIGINT AUTO_INCREMENT PRIMARY KEY,
--     airflow_run_id VARCHAR(255),

--     -- Foreign Keys
--     route_id INT NOT NULL,
--     aircraft_id INT NOT NULL,
--     booking_profile_id INT NOT NULL,
--     departure_date_id INT NOT NULL,
--     arrival_date_id INT NOT NULL,

--    -- Degenerate Dimensions
--     airline VARCHAR(100) NOT NULL,
--     departure_time DATETIME NOT NULL,
--     arrival_time DATETIME NOT NULL,

--     -- Measures (Fixed Precision)
--     duration_hrs DECIMAL(10,4),
--     days_before_departure INT,
--     base_fare DECIMAL(10,2),
--     tax_and_surcharge DECIMAL(10,2),
--     total_fare DECIMAL(10,2),

--     -- Indexes
--     INDEX idx_fk_route (route_id),
--     INDEX idx_fk_date (departure_date_id),
--     INDEX idx_airline (airline)
-- );
