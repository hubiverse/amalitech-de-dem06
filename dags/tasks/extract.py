import os
import kagglehub
import mysql.connector
from config import config

def load_kaggle_to_mysql(run_id: str) -> None:
    """
    Downloads the Kaggle dataset and streams it into the MySQL Bronze layer.

    Args:
        run_id (str): The Airflow run ID used for data lineage/traceability.
    """
    print(f"Starting ingestion for Airflow run: {run_id}")

    # Download Data
    dataset_path: str = kagglehub.dataset_download(
        "mahatiratusher/flight-price-dataset-of-bangladesh"
    )

    csv_file: str | None = next(
        (f for f in os.listdir(dataset_path) if f.endswith(".csv")),
        None
    )

    if csv_file is None:
        raise FileNotFoundError("No CSV file found in Kaggle download.")

    full_path: str = os.path.join(dataset_path, csv_file)

    # Connect to MySQL
    conn = mysql.connector.connect(
        host=config.mysql_host,
        user=config.mysql_user,
        password=config.mysql_password,
        database=config.mysql_database,
        allow_local_infile=True
    )
    cursor = conn.cursor()

    try:
        # Truncate for consistency
        _ = cursor.execute("TRUNCATE TABLE raw_flights;")

        load_query: str = f"""
        LOAD DATA LOCAL INFILE '{full_path}'
        INTO TABLE raw_flights
        FIELDS TERMINATED BY ','
        ENCLOSED BY '"'
        LINES TERMINATED BY '\\n'
        IGNORE 1 LINES
        (
            airline, source, source_name, destination, destination_name,
            departure_time, arrival_time, duration_hrs, stopovers,
            aircraft_type, travel_class, booking_source, base_fare,
            tax_and_surcharge, total_fare, seasonality, @var_days
        )
        SET
            airflow_run_id = '{run_id}',
            days_before_departure = REPLACE(@var_days, '\\r', '');
        """

        _ = cursor.execute(load_query)
        _ = conn.commit()
        print(f"Successfully loaded batch {run_id} to Bronze layer.")

    finally:
        _ = cursor.close()
        conn.close()
