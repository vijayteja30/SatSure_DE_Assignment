from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from modules.ingestion import ingest_raw_data
from modules.transformation import transform_data
from modules.validation import validate_data
from modules.storage import load_to_iceberg
from modules.reporting import generate_report

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='telemetry_pipeline',
    default_args=default_args,
    description='Hourly DAG for telemetry and weekly config ingestion',
    schedule_interval='@hourly',
    catchup=True,
    max_active_runs=1,
    tags=['telemetry', 'iot', 'iceberg'],
) as dag:

    task_ingest = PythonOperator(
        task_id='ingest_raw_data',
        python_callable=ingest_raw_data,
    )

    task_transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    task_validate = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
    )

    task_load = PythonOperator(
        task_id='load_to_iceberg',
        python_callable=load_to_iceberg,
    )

    task_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report,
    )

    # Set task dependencies
    task_ingest >> task_transform >> task_validate >> task_load >> task_report
