from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import sys
import os



# === Import your task logic ===
from validate import validate
from transform import transform
# from tasks.load_to_redshift import load_all

# Branch decision function
def validate_and_branch(**kwargs):
    try:
        validate()  # Runs validation logic; raises exception if invalid
        return "transform_data"
        
    except Exception as e:
        # Optionally log the error here
        print(f"Validation failed: {e}")
        return "end_pipeline"


# === DAG Definition ===
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="music_etl_pipeline",
    default_args=default_args,
    description="ETL for music streaming data using S3 + Redshift",
    schedule_interval="@daily",
    start_date=datetime(2025, 6, 12),
    catchup=False,
    tags=["music", "ETL", "s3"],
) as dag:

    validate_task = BranchPythonOperator(
        task_id="validation",
        python_callable=validate_and_branch,
        provide_context=True,
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform,
    )

    end_task = EmptyOperator(
        task_id="end_pipeline"
    )

    continue_task = EmptyOperator(
        task_id="continue_pipeline"
    )

    # DAG dependencies
    validate_task >> [transform_task, end_task]
    transform_task >> continue_task
