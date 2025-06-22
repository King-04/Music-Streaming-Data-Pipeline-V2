from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import boto3
import logging

# === Import your task logic ===
from validate import validate
from transform import transform

# Branch decision function
def validate_and_branch(**kwargs):
    try:
        result = validate()  # Runs validation logic
        if result and result.get('processed_files'):
            # Store processed files list for downstream tasks
            kwargs['task_instance'].xcom_push(key='processed_files', value=result['processed_files'])
            return "transform_data"
        else:
            return "end_pipeline"
        
    except Exception as e:
        print(f"Validation failed: {e}")
        return "end_pipeline"

def archive_processed_files(**kwargs):
    """Archive processed stream files to archive directory"""
    try:
        logger = logging.getLogger(__name__)
        logger.info("🗂️ Starting file archival process...")
        
        # Get processed files from validation task
        processed_files = kwargs['task_instance'].xcom_pull(key='processed_files', task_ids='validation')
        
        if not processed_files:
            logger.info("📭 No files to archive")
            return
        
        s3_client = boto3.client('s3')
        bucket = 'streaming-analytics-buck1'
        archived_count = 0
        
        for file_key in processed_files:
            try:
                # Define archive path
                archive_key = file_key.replace('streams/', 'archive/streams/')
                
                # Copy to archive directory
                copy_source = {'Bucket': bucket, 'Key': file_key}
                s3_client.copy_object(
                    CopySource=copy_source,
                    Bucket=bucket,
                    Key=archive_key
                )
                
                # Delete from original location
                s3_client.delete_object(Bucket=bucket, Key=file_key)
                
                logger.info(f"✅ Archived: {file_key} → {archive_key}")
                archived_count += 1
                
            except Exception as file_error:
                logger.error(f"❌ Failed to archive {file_key}: {str(file_error)}")
                continue
        
        logger.info(f"🎉 Successfully archived {archived_count} files")
        return {"archived_files": archived_count}
        
    except Exception as e:
        logger.exception("❌ File archival process failed")
        raise

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

    validate_task = BranchPythonOperator(
        task_id="validation",
        python_callable=validate_and_branch,
        provide_context=True,
    )

    # Replace dummy transform with actual Glue job
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

    # DynamoDB ingestion job
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

    # File archival task
    archive_task = PythonOperator(
        task_id="archive_processed_files",
        python_callable=archive_processed_files,
        provide_context=True,
    )

    end_task = EmptyOperator(
        task_id="end_pipeline"
    )

    continue_task = EmptyOperator(
        task_id="continue_pipeline"
    )

    # DAG dependencies
    validate_task >> [transform_task, end_task]
    transform_task >> load_to_dynamodb_task >> archive_task >> continue_task


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
