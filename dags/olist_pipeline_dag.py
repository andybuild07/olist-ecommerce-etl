from datetime import timedelta
import sys
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

# Ensure Airflow workers can import scripts from /opt/airflow/src
sys.path.append('/opt/airflow/src')

from transform import run_curated_transform
from load import run_star_schema_load

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='olist_star_schema_pipeline',
    default_args=default_args,
    description='Daily Olist ETL pipeline: land (MinIO) -> transform (DuckDB/Parquet) -> load (Star Schema)',
    schedule='@daily',
    start_date=pendulum.datetime(2017, 10, 2, tz='UTC'),
    catchup=False,
) as dag:

    def transform_task(**context):
        run_curated_transform(context['ds'])

    def load_task(**context):
        run_star_schema_load(context['ds'])

    t1 = PythonOperator(
        task_id='run_curated_transform',
        python_callable=transform_task,
    )

    t2 = PythonOperator(
        task_id='run_star_schema_load',
        python_callable=load_task,
    )

    t1 >> t2