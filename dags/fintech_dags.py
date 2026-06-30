from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator

from cosmos import DbtTaskGroup, ProjectConfig, ExecutionConfig
from cosmos import ClickHouseUserPasswordProfileMapping

from fintech_analytics.fintech_data_generator import generate_fintech_data
from business_logic.csv_data import run_csv_to_clickhouse_etl
from business_logic.Neta_business import run_pg_to_clickhouse_etl


DBT_PROJECT_PATH = Path("opt/airflow/fintech_dbt")


profile_config = ProjectConfig(
    profile_name="fintech_dbt",
    target_name="dev",
    profile_mapping=ClickHouseUserPasswordProfileMapping(
        clickhouse_conn_id="clickhouse_default",
    ),
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 4, 10),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="fintech_dbt_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["fintech", "dbt"],
    default_args=default_args,
) as dag:
    

    @task()
    def airflow_task():
        print("Airflow is running!")   

    @task(task_id="data_generation_task")
    def data_generation_task(n_rows: int, output_dir: str, postgres_conn_id: str = "postgres_default"):
        """
        Main function invoked by Airflow's PythonOperator.
        Generates mock fintech data and writes it to Postgres and CSV.
        """
        return generate_fintech_data(n_rows, output_dir, postgres_conn_id=postgres_conn_id)
    
    @task(task_id="pg_to_ch_etl_task")
    def pg_to_ch_etl_task(postgres_conn_id: str = "postgres_default", clickhouse_conn_id: str = "clickhouse_default", limit: int = None, offset: int = 0):
        """
        Main function invoked by Airflow's PythonOperator.
        Extracts data from Postgres and loads it into ClickHouse.
        """
        return run_pg_to_clickhouse_etl(postgres_conn_id = postgres_conn_id, 
                                        clickhouse_conn_id = clickhouse_conn_id, 
                                        limit = limit, 
                                        offset = offset)
    
    @task(task_id="csv_to_ch_etl_task")
    def csv_to_ch_etl_task(clickhouse_conn_id: str, csv_file_path: str):
        """
        Main function invoked by Airflow's PythonOperator.
        Extracts data from a CSV file and loads it into ClickHouse.
        """
        return run_csv_to_clickhouse_etl(clickhouse_conn_id = clickhouse_conn_id, csv_file_path = csv_file_path) 




    dbt_task_group = DbtTaskGroup(
        group_id="dbt_task_group",
        project_config=profile_config,
        execution_config=ExecutionConfig(),
    )   
    airflow_init = airflow_task()

    gen_data = data_generation_task(n_rows=7200, 
                                    output_dir="/opt/airflow/fintech_analytics/data", 
                                    postgres_conn_id="postgres_default")

    pg_to_ch = pg_to_ch_etl_task(postgres_conn_id="postgres_default",
                                 clickhouse_conn_id="clickhouse_default",
                                 limit=None,    
                                 offset=0)
    
    csv_to_ch = csv_to_ch_etl_task(clickhouse_conn_id="clickhouse_default",
                                   csv_file_path="/opt/airflow/fintech_analytics/data/ledger_entries.csv")
    

    airflow_init >> gen_data >> [pg_to_ch, csv_to_ch] >> dbt_task_group