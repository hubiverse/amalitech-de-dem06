import mysql.connector
import psycopg2
import tempfile
import csv
from config import config


def transfer_mysql_to_postgres(run_id: str) -> None:
    mysql_conn = mysql.connector.connect(
        host=config.mysql_host,
        user=config.mysql_user,
        password=config.mysql_password,
        database=config.mysql_database,
    )
    mysql_cursor = mysql_conn.cursor()

    pg_conn = psycopg2.connect(
        host=config.postgres_host,
        user=config.postgres_user,
        password=config.postgres_password,
        dbname=config.postgres_db,
    )
    pg_cursor = pg_conn.cursor()

    #SQL
    copy_sql = """
    COPY raw_flights (
        id, airflow_run_id, airline, source, source_name, destination,
        destination_name, departure_time, arrival_time, duration_hrs,
        stopovers, aircraft_type, travel_class, booking_source,
        base_fare, tax_and_surcharge, total_fare, seasonality,
        days_before_departure, ingested_at
    ) FROM STDIN WITH CSV
    """

    try:
        _ = mysql_cursor.execute( #SQL
            "SELECT * FROM raw_flights WHERE airflow_run_id = %s",
            (run_id,),
        )

        with tempfile.TemporaryFile(mode="w+t", newline="") as f:
            writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)

            while True:
                rows = mysql_cursor.fetchmany(50000)
                if not rows:
                    break
                writer.writerows(rows)

            _ = f.seek(0)

            pg_cursor.execute(
                "TRUNCATE TABLE raw_flights;"
            )

            pg_cursor.copy_expert(copy_sql, f)

        pg_conn.commit()

    except Exception:
        pg_conn.rollback()
        raise

    finally:
        _ = mysql_cursor.close()
        mysql_conn.close()
        pg_cursor.close()
        pg_conn.close()
