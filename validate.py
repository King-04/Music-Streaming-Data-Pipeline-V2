import boto3
import pandas as pd
import json
import os

s3 = boto3.client('s3')

# Required schemas for validation
SCHEMAS = {
    'users': ['user_id', 'user_name', 'user_age', 'user_country', 'created_at'],
    'songs': ['id', 'track_id', 'artists', 'album_name', 'track_name', 'popularity', 'duration_ms', 'explicit', 'danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo', 'time_signature', 'track_genre'],
    'streams': ['user_id', 'track_id', 'listen_time']
}

def get_file_type(key):
    if key.startswith('incoming/users/'):
        return 'users'
    elif key.startswith('incoming/songs/'):
        return 'songs'
    elif key.startswith('incoming/streams/'):
        return 'streams'
    else:
        return None

def validate_and_update_manifest(bucket, key, validated_prefix='validated/', error_prefix='error/', manifest_prefix='manifest/'):
    file_type = get_file_type(key)
    if not file_type:
        print(f"Unknown file type for key: {key}")
        return {'status': 'failed', 'reason': 'unknown_file_type'}

    # Download file from S3 to local temp
    local_path = '/tmp/input.csv'
    s3.download_file(bucket, key, local_path)
    df = pd.read_csv(local_path)

    # Validate schema
    required_cols = SCHEMAS[file_type]
    if list(df.columns) != required_cols:
        # Move file to error folder
        error_key = f"{error_prefix}{file_type}/{os.path.basename(key)}"
        s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': key}, Key=error_key)
        s3.delete_object(Bucket=bucket, Key=key)
        print(f"File {key} failed validation and was moved to {error_key}")
        return {'status': 'failed', 'reason': 'schema_mismatch'}

    # Copy file to validated folder
    validated_key = f"{validated_prefix}{file_type}/{os.path.basename(key)}"
    s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': key}, Key=validated_key)
    s3.delete_object(Bucket=bucket, Key=key)
    print(f"File {key} passed validation and was copied to {validated_key}")

    # Update manifest
    manifest_key = f"{manifest_prefix}{file_type}_manifest.json"
    try:
        manifest_obj = s3.get_object(Bucket=bucket, Key=manifest_key)
        manifest = json.loads(manifest_obj['Body'].read())
    except s3.exceptions.NoSuchKey:
        manifest = {'validated_files': []}

    if validated_key not in manifest['validated_files']:
        manifest['validated_files'].append(validated_key)
        s3.put_object(Bucket=bucket, Key=manifest_key, Body=json.dumps(manifest))
        print(f"Manifest {manifest_key} updated.")

    return {'status': 'success', 'file_type': file_type, 'validated_key': validated_key}

# Example usage (for Airflow PythonOperator or local test)
if __name__ == "__main__":
    bucket = 'your-bucket'
    key = 'incoming/users/users_20250621.csv'
    result = validate_and_update_manifest(bucket, key)
    print(result)
