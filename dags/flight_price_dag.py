from pathlib import Path
from airflow.sdk import DAG, get_current_context
from airflow.sdk.definitions.decorators import task
from datetime import datetime, timedelta
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig

from tasks.transfer import transfer_mysql_to_postgres
from tasks.extract import load_kaggle_to_mysql

DEFAULT_DBT_ROOT_PATH = Path("/opt/airflow/dags/dbt_flights")

default_args = {
    'owner': 'data_platform',
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
}


with DAG(
    dag_id='flight_price_pipeline',
    default_args=default_args,
    schedule='@once',
    start_date=datetime(2026, 4, 22),
    catchup=False,
    tags=['etl', 'kaggle', 'flights'],
) as dag:

    @task()
    def extract() -> str:
        """
        We extract the run_id and pass it to our typed function.
        Load the Kaggle dataset into MySQL.
        """
        context = get_current_context()
        current_run_id: str = context.get("run_id", "manual_run_fallback")
        load_kaggle_to_mysql(run_id=current_run_id)
        return current_run_id

    @task()
    def transfer_to_postgres(run_id: str) -> None:
        """
        Transfer data from MySQL to PostgreSQL.
        """
        transfer_mysql_to_postgres(run_id=run_id)

    transform = DbtTaskGroup(
        group_id="dbt_postgres_transform",
        project_config=ProjectConfig(
            dbt_project_path=DEFAULT_DBT_ROOT_PATH,
        ),
        profile_config=ProfileConfig(
            profile_name="postgres_analytics",
            target_name="dev",
            profiles_yml_filepath=DEFAULT_DBT_ROOT_PATH / "profiles.yml",
        )
    )

    extract() >> transfer_to_postgres() >> transform
