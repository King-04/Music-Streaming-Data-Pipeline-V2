from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from extract_and_validate import extract_and_validate
from transform import transform
from load_to_redshift import load_all
from compute_kpis import compute_kpis


# === DAG Default Config ===
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# === DAG Definition ===
with DAG(
    dag_id="music_etl_pipeline",
    default_args=default_args,
    description="ETL for music streaming data using S3 + Redshift",
    schedule_interval="@daily",
    start_date=datetime(2025, 6, 12),
    catchup=False,
    tags=["music", "ETL", "s3"],
) as dag:

    extract_validate_task = PythonOperator(
        task_id="extract_and_validate",
        python_callable=extract_and_validate,
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform,
    )

    load_to_redshift_task = PythonOperator(
    task_id="load_to_redshift",
    python_callable=load_all,
    )

    compute_kpis_task = PythonOperator(
    task_id="compute_kpis",
    python_callable=compute_kpis,
    )



    # DAG Flow
    extract_validate_task >> transform_task >> load_to_redshift_task >> compute_kpis_task
