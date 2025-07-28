import boto3
import pandas as pd
from io import StringIO
import os
import logging
from io import BytesIO


# === Setup Logging ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

s3 = boto3.client("s3")

def read_csv_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """Read a CSV file from S3 and return as a pandas DataFrame."""
    try:
        logger.info(f"Reading from s3://{bucket}/{key}")
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response["Body"].read().decode("utf-8")
        df = pd.read_csv(StringIO(content))
        logger.info(f"Loaded {len(df)} rows from {key}")
        return df
    except Exception as e:
        logger.exception(f"Failed to read s3://{bucket}/{key}")
        raise

def list_s3_files(bucket: str, prefix: str) -> list:
    """List all file keys in an S3 prefix."""
    try:
        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        files = []
        for page in pages:
            for obj in page.get("Contents", []):
                files.append(obj["Key"])
        return files
    except Exception as e:
        logger.exception(f"Failed to list files in s3://{bucket}/{prefix}")
        raise

def read_manifest(bucket: str, key: str) -> set:
    """Read processed file list from manifest."""
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response["Body"].read().decode("utf-8")
        return set(content.strip().splitlines())
    except s3.exceptions.NoSuchKey:
        return set()
    except Exception as e:
        logger.exception(f"Failed to read manifest at s3://{bucket}/{key}")
        raise

def update_manifest(bucket: str, key: str, processed_files: list):
    """Append newly processed files to the manifest."""
    try:
        existing = read_manifest(bucket, key)
        updated = existing.union(set(processed_files))
        body = "\n".join(sorted(updated))
        s3.put_object(Bucket=bucket, Key=key, Body=body)
        logger.info(f"Updated manifest at s3://{bucket}/{key}")
    except Exception as e:
        logger.exception(f"Failed to update manifest at s3://{bucket}/{key}")
        raise


def write_parquet_to_s3(df: pd.DataFrame, bucket: str, key: str, save_local: bool = True):
    """Write a DataFrame to S3 as a Parquet file."""
    try:
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].astype('datetime64[us]')
        
        # Write to parquet with explicit timestamp precision
        buffer = BytesIO()
        df.to_parquet(buffer, index=False, engine='pyarrow')  # Ensure proper timestamp handling
        buffer.seek(0)
        s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
        logger.info(f"Parquet written to s3://{bucket}/{key}")

        if save_local:
            os.makedirs("output", exist_ok=True)
            local_path = f"output/{key.replace('/', '_').replace('.parquet', '')}.parquet"
            df.to_parquet(local_path, index=False)
            logger.info(f"Also saved locally to {local_path}")

    except Exception as e:
        logger.exception(f"Failed to write Parquet to s3://{bucket}/{key}")
        raise

def read_parquet_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """Read a Parquet file from S3 and return as DataFrame."""
    try:
        logger.info(f"Reading Parquet from s3://{bucket}/{key}")
        response = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_parquet(BytesIO(response['Body'].read()))
        logger.info(f"Loaded {len(df)} rows from {key}")
        return df
    except Exception as e:
        logger.exception(f"Failed to read Parquet from s3://{bucket}/{key}")
        raise


def check_s3_object_exists(bucket: str, key: str) -> bool:
    """Check if an S3 object exists without downloading it."""
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except s3.exceptions.NoSuchKey:
        return False
    except Exception as e:
        logger.exception(f"Error checking if s3://{bucket}/{key} exists")
        return False

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
        logger.info("Starting file archival process...")
        
        # Get processed files from validation task
        validation_result = kwargs['task_instance'].xcom_pull(key='return_value', task_ids='validate_data')

        # Debug logging to see what we're getting
        logger.info(f"DEBUG - Validation result: {validation_result}")
        logger.info(f"DEBUG - Type: {type(validation_result)}")
        
        # Handle different return formats
        if isinstance(validation_result, dict):
            processed_files = validation_result.get('processed_files', [])
        elif isinstance(validation_result, list):
            processed_files = validation_result
        else:
            logger.warning(f"Unexpected validation result type: {type(validation_result)}")
            processed_files = []
        
        # Type check and handle edge cases
        if not processed_files:
            logger.info("No files to archive")
            return {"archived_files": 0}
        
        if isinstance(processed_files, int):
            logger.error(f"Expected list of files, got integer: {processed_files}")
            raise ValueError(f"processed_files should be a list, not an integer: {processed_files}")
        
        if not isinstance(processed_files, list):
            logger.error(f"Expected list of files, got: {type(processed_files)}")
            raise ValueError(f"processed_files should be a list, got: {type(processed_files)}")
        
        logger.info(f"Found {len(processed_files)} files to archive: {processed_files}")
        
        s3_client = boto3.client('s3')
        bucket = 'streaming-analytics-buck1'
        archived_count = 0
        
        for file_key in processed_files:
            try:
                # Ensure file_key is a string
                if not isinstance(file_key, str):
                    logger.warning(f"Skipping non-string file key: {file_key}")
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
                
                logger.info(f"Archived: {file_key} → {archive_key}")
                archived_count += 1
                
            except Exception as file_error:
                logger.error(f"Failed to archive {file_key}: {str(file_error)}")
                continue
        
        logger.info(f"🎉 Successfully archived {archived_count} files")
        return {"archived_files": archived_count}
        
    except Exception as e:
        logger.exception("File archival process failed")
        raise
