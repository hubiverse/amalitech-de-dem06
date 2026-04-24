from airflow import DAG
from airflow.sdk.definitions.decorators import task
from datetime import datetime, timedelta
from tasks.extract import load_kaggle_to_mysql
from airflow.sdk import get_current_context

default_args = {
    'owner': 'data_platform',
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='flight_price_pipeline',
    default_args=default_args,
    schedule='@daily',
    start_date=datetime(2026, 4, 22),
    catchup=False,
    tags=['etl', 'kaggle', 'flights'],
) as dag:

    @task()
    def extract() -> None:
        """
        Airflow injects context variables into kwargs automatically.
        We extract the run_id and pass it to our typed function.
        """
        context = get_current_context()
        current_run_id: str = context.get("run_id", "manual_run_fallback")
        load_kaggle_to_mysql(run_id=current_run_id)

    _ = extract()
