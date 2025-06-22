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
        buffer = BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)
        s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
        logger.info(f"✅ Parquet written to s3://{bucket}/{key}")

        if save_local:
            os.makedirs("output", exist_ok=True)
            local_path = f"output/{key.replace('/', '_').replace('.parquet', '')}.parquet"
            df.to_parquet(local_path, index=False)
            logger.info(f"💾 Also saved locally to {local_path}")

    except Exception as e:
        logger.exception(f"Failed to write Parquet to s3://{bucket}/{key}")
        raise

def read_parquet_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """Read a Parquet file from S3 and return as DataFrame."""
    try:
        logger.info(f"📥 Reading Parquet from s3://{bucket}/{key}")
        response = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_parquet(BytesIO(response['Body'].read()))
        logger.info(f"✅ Loaded {len(df)} rows from {key}")
        return df
    except Exception as e:
        logger.exception(f"❌ Failed to read Parquet from s3://{bucket}/{key}")
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
