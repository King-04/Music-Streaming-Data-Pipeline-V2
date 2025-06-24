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
                logger.info("📭 No new files to process")
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

    # Task 1: S3 Sensor to detect new stream files
    s3_sensor = S3KeySensor(
        task_id="detect_new_stream_files",
        bucket_name="streaming-analytics-buck1",
        bucket_key="streams/",  # Monitor the streams directory
        wildcard_match=True,    # Match any file in streams/
        poke_interval=60,       # Check every 60 seconds
        timeout=300,            # Timeout after 5 minutes
        mode="reschedule",      # Use reschedule mode to free up workers
        soft_fail=False,        # Fail the task if timeout occurs
        aws_conn_id="aws_default",
    )
    
    # Task 2: Pure validation task
    validate_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate,
        provide_context=True,
    )

    # Task 3: Pure branching decision task
    branch_task = BranchPythonOperator(
        task_id="decide_workflow",
        python_callable=decide_next_step,
        provide_context=True,
    )

    # Task 4: Transformation (Glue job)
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

    # Task 5: DynamoDB ingestion (Glue job)
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

    # Task 6: File archival
    archive_task = PythonOperator(
        task_id="archive_processed_files",
        python_callable=archive_processed_files,
        provide_context=True,
    )

    # Task 7: End states
    end_task = EmptyOperator(
        task_id="end_pipeline"
    )

    # === Task Dependencies ===
    validate_task >> branch_task >> [transform_task, end_task]
    transform_task >> load_to_dynamodb_task >> archive_task



# from airflow import DAG
# from airflow.operators.python import PythonOperator, BranchPythonOperator
# from airflow.operators.empty import EmptyOperator
# from datetime import datetime, timedelta
# import sys
# import os



# # === Import your task logic ===
# from validate import validate
# from transform import transform
# # from tasks.load_to_redshift import load_all

# # Branch decision function
# def validate_and_branch(**kwargs):
#     try:
#         validate()  # Runs validation logic; raises exception if invalid
#         return "transform_data"
        
#     except Exception as e:
#         # Optionally log the error here
#         print(f"Validation failed: {e}")
#         return "end_pipeline"


# # === DAG Definition ===
# default_args = {
#     "owner": "airflow",
#     "retries": 1,
#     "retry_delay": timedelta(minutes=5),
# }

# with DAG(
#     dag_id="music_etl_pipeline",
#     default_args=default_args,
#     description="ETL for music streaming data using S3 + Redshift",
#     schedule_interval="@daily",
#     start_date=datetime(2025, 6, 12),
#     catchup=False,
#     tags=["music", "ETL", "s3"],
# ) as dag:

#     validate_task = BranchPythonOperator(
#         task_id="validation",
#         python_callable=validate_and_branch,
#         provide_context=True,
#     )

#     transform_task = PythonOperator(
#         task_id="transform_data",
#         python_callable=transform,
#     )

#     end_task = EmptyOperator(
#         task_id="end_pipeline"
#     )

#     continue_task = EmptyOperator(
#         task_id="continue_pipeline"
#     )

#     # DAG dependencies
#     validate_task >> [transform_task, end_task]
#     transform_task >> continue_task
