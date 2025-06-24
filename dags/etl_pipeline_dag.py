from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import boto3
import logging

# === Import your task logic ===
from validate import validate

from utils import archive_processed_files, decide_next_step

def validate_and_branch(**kwargs):
    logger = logging.getLogger(__name__)
    try:
        result = validate()  # This should return a dict with processed_files list
        
        # Check if validation was successful and files were processed
        if result and result.get('status') == 'success':
            processed_files = result.get('processed_files', [])
            
            # Store the entire result for downstream tasks
            kwargs['task_instance'].xcom_push(key='validation_result', value=result)
            
            if processed_files:  # If there are files to process
                return "transform_data"
            else:
                logger.info("No new files to process")
                return "end_pipeline"
        else:
            return "end_pipeline"
        
    except Exception as e:
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
    description="ETL for music streaming data using S3 + DynamoDB",
    schedule_interval="@daily",
    start_date=datetime(2025, 6, 12),
    catchup=False,
    tags=["music", "ETL", "s3", "dynamodb"],
) as dag:
    
    # Task 1: Pure validation task
    validate_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate,
        provide_context=True,
    )

    # Task 2: Pure branching decision task
    branch_task = BranchPythonOperator(
        task_id="decide_workflow",
        python_callable=decide_next_step,
        provide_context=True,
    )

    # Task 3: Transformation (Glue job)
    transform_task = GlueJobOperator(
        task_id="transform_data",
        job_name="music-streaming-transform",
        script_args={
            "--S3_BUCKET": "streaming-analytics-buck1",
            "--STREAMS_INPUT_PATH": "validated-data/streams/",
            "--SONGS_INPUT_PATH": "validated-data/validated_songs.parquet",
            "--USERS_INPUT_PATH": "validated-data/validated_users.parquet",
            "--OUTPUT_PATH": "transformed-data/",
            "--PROCESS_DATE": "{{ ds }}"
        },
        aws_conn_id="aws_default",
    )

    # Task 4: DynamoDB ingestion (Glue job)
    load_to_dynamodb_task = GlueJobOperator(
        task_id="load_to_dynamodb",
        job_name="music-streaming-dynamodb-ingestion",
        script_args={
            "--S3_BUCKET": "streaming-analytics-buck1",
            "--TRANSFORMED_DATA_PATH": "transformed-data/",
            "--DYNAMODB_TABLE_NAME": "music-streaming-kpis"
        },
        aws_conn_id="aws_default",
    )

    # Task 5: File archival
    archive_task = PythonOperator(
        task_id="archive_processed_files",
        python_callable=archive_processed_files,
        provide_context=True,
    )

    # Task 6: End states
    end_task = EmptyOperator(
        task_id="end_pipeline"
    )

    # === Task Dependencies ===
    validate_task >> branch_task >> [transform_task, end_task]
    transform_task >> load_to_dynamodb_task >> archive_task
