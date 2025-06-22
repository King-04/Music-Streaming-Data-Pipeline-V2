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


def decide_next_step(**kwargs):
    """Pure branching logic - only makes workflow decisions"""
    try:
        # Get validation result from previous task
        validation_result = kwargs['task_instance'].xcom_pull(
            task_ids='validate_data',
            key='return_value'
        )
        
        if validation_result and validation_result.get('processed_files'):
            return "transform_data"
        else:
            return "end_pipeline"
        
    except Exception as e:
        print(f"Branching decision failed: {e}")
        return "end_pipeline"

def archive_processed_files(**kwargs):
    """Archive processed stream files to archive directory"""
    try:
        logger = logging.getLogger(__name__)
        logger.info("🗂️ Starting file archival process...")
        
        # Get processed files from validation task
        validation_result = kwargs['task_instance'].xcom_pull(key='return_value', task_ids='validation')
        
        # Handle different return formats
        if isinstance(validation_result, dict):
            processed_files = validation_result.get('processed_files', [])
        elif isinstance(validation_result, list):
            processed_files = validation_result
        else:
            logger.warning(f"⚠️ Unexpected validation result type: {type(validation_result)}")
            processed_files = []
        
        # Type check and handle edge cases
        if not processed_files:
            logger.info("📭 No files to archive")
            return {"archived_files": 0}
        
        if isinstance(processed_files, int):
            logger.error(f"❌ Expected list of files, got integer: {processed_files}")
            raise ValueError(f"processed_files should be a list, not an integer: {processed_files}")
        
        if not isinstance(processed_files, list):
            logger.error(f"❌ Expected list of files, got: {type(processed_files)}")
            raise ValueError(f"processed_files should be a list, got: {type(processed_files)}")
        
        logger.info(f"📋 Found {len(processed_files)} files to archive: {processed_files}")
        
        s3_client = boto3.client('s3')
        bucket = 'streaming-analytics-buck1'
        archived_count = 0
        
        for file_key in processed_files:
            try:
                # Ensure file_key is a string
                if not isinstance(file_key, str):
                    logger.warning(f"⚠️ Skipping non-string file key: {file_key}")
                    continue
                
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

    # Task 1: Pure validation task
    validate_task = PythonOperator(
        task_id="validate_data",
        python_callable=run_validation,
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

    continue_task = EmptyOperator(
        task_id="continue_pipeline"
    )

    # === Task Dependencies ===
    validate_task >> branch_task >> [transform_task, end_task]
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
