import pandas as pd
import os
import logging
from utils import (
    read_csv_from_s3,
    list_s3_files,
    read_manifest,
    update_manifest,
    write_parquet_to_s3,
    check_s3_object_exists
)

# === Setup Logging ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# === S3 Buckets and Keys ===
BUCKET = "streaming-analytics-buck1"
SONGS_KEY = "metadata/songs.csv"
USERS_KEY = "metadata/users.csv"
SONGS_OUT = "validated-data/validated_songs.parquet"
USERS_OUT = "validated-data/validated_users.parquet"
STREAMS_PREFIX = "streams/"
STREAMS_VALIDATED_PREFIX = "validated-data/streams/"
MANIFEST_KEY = "processing-metadata/processed_stream_files.txt"

# === Metadata validation flags ===
METADATA_VALIDATED_FLAG = "processing-metadata/metadata_validated.flag"

# === Configuration ===
FAIL_ON_VALIDATION_ERROR = True  # Set to False for resilient mode

def validate_columns(df, required_cols, dataset_name):
    """Validate that all required columns exist in the dataset"""
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        msg = f"{dataset_name} missing required columns: {missing}"
        logger.error(msg)
        raise ValueError(msg)
    logger.info(f"{dataset_name} passed column validation.")
    return True

def validate_users(df):
    """Validate and clean users data"""
    initial_count = len(df)
    
    # Drop rows with missing critical fields
    df.dropna(subset=["user_id", "user_name", "user_age", "created_at"], inplace=True)
    df["user_id"] = df["user_id"].astype(str)
    
    # Ensure age is numeric and within sane bounds
    df = df[pd.to_numeric(df["user_age"], errors="coerce").between(13, 120)]
    
    # Parse and validate created_at
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["user_age"] = pd.to_numeric(df["user_age"], errors="coerce").fillna(0).astype("int32")
    df.dropna(subset=["created_at"], inplace=True)
    
    logger.info(f"✅ Users validation complete: {initial_count} → {len(df)} records")
    return df

def validate_songs(df):
    """Validate and clean songs data"""
    initial_count = len(df)
    
    # Drop rows with missing critical fields
    df.dropna(subset=["track_id", "track_name", "popularity", "duration_ms", "track_genre"], inplace=True)
    df["track_id"] = df["track_id"].astype(str)
    
    # Ensure popularity and duration_ms are numeric
    df = df[pd.to_numeric(df["popularity"], errors="coerce").notna()]
    df = df[pd.to_numeric(df["duration_ms"], errors="coerce").notna()]
    
    # Cast types to optimized formats
    df["id"] = df["id"].astype("int32")
    df["popularity"] = df["popularity"].astype("int32")
    df["duration_ms"] = df["duration_ms"].astype("int32")
    df["explicit"] = df["explicit"].astype("int8")
    df["danceability"] = df["danceability"].astype("float32")
    df["energy"] = df["energy"].astype("float32")
    df["key"] = pd.to_numeric(df["key"], errors="coerce").fillna(0).astype("int32")
    df["loudness"] = df["loudness"].astype("float32")
    df["mode"] = pd.to_numeric(df["mode"], errors="coerce").fillna(0).astype("int32")
    df["speechiness"] = df["speechiness"].astype("float32")
    df["acousticness"] = df["acousticness"].astype("float32")
    df["instrumentalness"] = df["instrumentalness"].astype("float32")
    df["liveness"] = df["liveness"].astype("float32")
    df["valence"] = df["valence"].astype("float32")
    df["tempo"] = df["tempo"].astype("float32")
    df["time_signature"] = pd.to_numeric(df["time_signature"], errors="coerce").fillna(0).astype("int32")
    
    # Clean string fields
    df["track_genre"] = df["track_genre"].str.lower().str.strip()
    df["track_name"] = df["track_name"].astype(str)
    df["album_name"] = df["album_name"].astype(str)
    df["artists"] = df["artists"].astype(str)
    
    logger.info(f"✅ Songs validation complete: {initial_count} → {len(df)} records")
    return df

def validate_streams(df, stream_key):
    """Validate and clean streaming data"""
    initial_count = len(df)
    
    # Drop rows with missing critical fields
    df.dropna(subset=["user_id", "track_id", "listen_time"], inplace=True)
    df["user_id"] = df["user_id"].astype(str)
    df["track_id"] = df["track_id"].astype(str)
    
    # Parse timestamps
    df["listen_time"] = pd.to_datetime(df["listen_time"], errors="coerce")
    
    # Convert to microsecond precision to avoid TIMESTAMP(NANOS) issue
    df["listen_time"] = df["listen_time"].astype('datetime64[us]')
    dropped = df["listen_time"].isna().sum()
    if dropped > 0:
        logger.warning(f"🕒 Dropping {dropped} rows with invalid timestamps.")
    df.dropna(subset=["listen_time"], inplace=True)
    
    # Validate we have data left after cleaning
    if len(df) == 0:
        raise ValueError(f"No valid data remaining after validation for {stream_key}")
    
    logger.info(f"✅ Streams validation complete: {stream_key} → {initial_count} → {len(df)} records")
    return df

def validate_metadata_once():
    """
    Validate songs and users metadata only if not already processed.
    This ensures we don't reprocess static data on every pipeline run.
    """
    try:
        # Check if metadata has already been validated
        if check_s3_object_exists(BUCKET, METADATA_VALIDATED_FLAG):
            logger.info("📋 Metadata already validated. Skipping songs and users processing.")
            return True
        
        logger.info("🔄 First-time metadata validation starting...")
        
        # Load and validate songs
        logger.info("📥 Loading songs metadata...")
        songs_df = read_csv_from_s3(BUCKET, SONGS_KEY)
        validate_columns(songs_df, [
            "track_id", "track_name", "artists", "popularity", "duration_ms", "track_genre"
        ], "songs.csv")
        songs_df = validate_songs(songs_df)
        write_parquet_to_s3(songs_df, BUCKET, SONGS_OUT)
        
        # Load and validate users
        logger.info("📥 Loading users metadata...")
        users_df = read_csv_from_s3(BUCKET, USERS_KEY)
        validate_columns(users_df, [
            "user_id", "user_name", "user_age", "user_country", "created_at"
        ], "users.csv")
        users_df = validate_users(users_df)
        write_parquet_to_s3(users_df, BUCKET, USERS_OUT)
        
        # Create validation flag to prevent reprocessing
        import boto3
        s3 = boto3.client("s3")
        s3.put_object(
            Bucket=BUCKET, 
            Key=METADATA_VALIDATED_FLAG, 
            Body=f"Metadata validated on {pd.Timestamp.now()}"
        )
        
        logger.info("✅ Metadata validation complete and cached.")
        return True
        
    except Exception as e:
        logger.exception("❌ Failed during metadata validation.")
        raise  # Re-raise to fail the task

def process_new_streams():
    """
    Process only new streaming files that haven't been processed yet.
    This is the core efficiency improvement - only process incremental data.
    """
    try:
        logger.info("🔎 Scanning for new streaming files...")
        
        # Get all stream files and already processed files
        all_stream_keys = [
            key for key in list_s3_files(BUCKET, STREAMS_PREFIX) 
            if key.endswith(".csv") and "stream" in key.lower()
        ]
        processed_keys = read_manifest(BUCKET, MANIFEST_KEY)
        
        # Find new files to process
        new_files = [key for key in all_stream_keys if key not in processed_keys]
        
        if not new_files:
            logger.info("📭 No new streaming files to process.")
            return []
        
        logger.info(f"🆕 Found {len(new_files)} new stream files to process:")
        for file in new_files:
            logger.info(f"   - {file}")
        
        processed_files = []
        failed_files = []
        
        for stream_key in new_files:
            try:
                logger.info(f"📥 Processing stream file: {stream_key}")
                
                # Load and validate stream data
                df = read_csv_from_s3(BUCKET, stream_key)
                validate_columns(df, ["user_id", "track_id", "listen_time"], stream_key)
                df = validate_streams(df, stream_key)
                
                # Save validated stream data with organized naming
                stream_name = os.path.basename(stream_key).replace(".csv", "")
                timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
                validated_key = f"{STREAMS_VALIDATED_PREFIX}{stream_name}_validated_{timestamp}.parquet"
                
                write_parquet_to_s3(df, BUCKET, validated_key)
                processed_files.append(stream_key)
                
                logger.info(f"✅ Successfully processed: {stream_key} → {validated_key}")
                
            except Exception as stream_err:
                logger.exception(f"❌ Failed processing {stream_key}")
                failed_files.append(stream_key)
                
                if FAIL_ON_VALIDATION_ERROR:
                    # Fail immediately on first error
                    error_msg = f"Validation failed for stream file: {stream_key}. Error: {str(stream_err)}"
                    logger.error(error_msg)
                    raise ValueError(error_msg)
                else:
                    # Continue processing other files in resilient mode
                    logger.warning(f"⚠️ Continuing with other files due to resilient mode setting.")
                    continue
        
        # If we're in resilient mode and have failures, still report them
        if failed_files and not FAIL_ON_VALIDATION_ERROR:
            logger.warning(f"⚠️ {len(failed_files)} files failed validation but pipeline continued: {failed_files}")
        
        # Update manifest with successfully processed files only
        if processed_files:
            update_manifest(BUCKET, MANIFEST_KEY, processed_files)
            logger.info(f"📝 Updated manifest with {len(processed_files)} processed files.")
        
        # If in strict mode and we have failures, fail the task
        if failed_files and FAIL_ON_VALIDATION_ERROR:
            error_msg = f"Validation failed for {len(failed_files)} files: {failed_files}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        return processed_files
        
    except Exception as e:
        logger.exception("❌ Failed during stream processing.")
        raise  # Re-raise to fail the Airflow task

def validate():
    """
    Main validation function with optimized metadata handling and proper error propagation.
    Only processes metadata once, then focuses on incremental stream data.
    """
    try:
        logger.info("🚀 Starting optimized extraction and validation process...")
        
        # Step 1: Validate metadata only if not already done
        validate_metadata_once()
        
        # Step 2: Process only new streaming files
        processed_streams = process_new_streams()
        
        if processed_streams:
            logger.info(f"🏁 Pipeline complete. Processed {len(processed_streams)} new stream files.")
        else:
            logger.info("🏁 Pipeline complete. No new data to process.")
            
        return {
            "status": "success",
            "processed_files": len(processed_streams),
            "files": processed_streams
        }
            
    except Exception as e:
        logger.exception("❌ Failed during extract_and_validate execution.")
        # Re-raise the exception to fail the Airflow task
        raise

if __name__ == "__main__":
    validate()



# import pandas as pd
# import os
# import logging
# from utils import (
#     read_csv_from_s3,
#     list_s3_files,
#     read_manifest,
#     update_manifest,
#     write_parquet_to_s3,
#     check_s3_object_exists
# )

# # === Setup Logging ===
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s - %(levelname)s - %(message)s"
# )
# logger = logging.getLogger(__name__)

# # === S3 Buckets and Keys ===
# BUCKET = "streaming-analytics-buck1"
# SONGS_KEY = "metadata/songs.csv"
# USERS_KEY = "metadata/users.csv"
# SONGS_OUT = "validated-data/validated_songs.parquet"
# USERS_OUT = "validated-data/validated_users.parquet"
# STREAMS_PREFIX = "streams/"
# STREAMS_VALIDATED_PREFIX = "validated-data/streams/"
# MANIFEST_KEY = "processing-metadata/processed_stream_files.txt"

# # === Metadata validation flags ===
# METADATA_VALIDATED_FLAG = "processing-metadata/metadata_validated.flag"

# def validate_columns(df, required_cols, dataset_name):
#     """Validate that all required columns exist in the dataset"""
#     missing = [col for col in required_cols if col not in df.columns]
#     if missing:
#         msg = f"{dataset_name} missing required columns: {missing}"
#         logger.error(msg)
#         raise ValueError(msg)
#     logger.info(f"{dataset_name} passed column validation.")
#     return True

# def validate_users(df):
#     """Validate and clean users data"""
#     initial_count = len(df)
    
#     # Drop rows with missing critical fields
#     df.dropna(subset=["user_id", "user_name", "user_age", "created_at"], inplace=True)
#     df["user_id"] = df["user_id"].astype(str)
    
#     # Ensure age is numeric and within sane bounds
#     df = df[pd.to_numeric(df["user_age"], errors="coerce").between(13, 120)]
    
#     # Parse and validate created_at
#     df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
#     df["user_age"] = pd.to_numeric(df["user_age"], errors="coerce").fillna(0).astype("int32")
#     df.dropna(subset=["created_at"], inplace=True)
    
#     logger.info(f"✅ Users validation complete: {initial_count} → {len(df)} records")
#     return df

# def validate_songs(df):
#     """Validate and clean songs data"""
#     initial_count = len(df)
    
#     # Drop rows with missing critical fields
#     df.dropna(subset=["track_id", "track_name", "popularity", "duration_ms", "track_genre"], inplace=True)
#     df["track_id"] = df["track_id"].astype(str)
    
#     # Ensure popularity and duration_ms are numeric
#     df = df[pd.to_numeric(df["popularity"], errors="coerce").notna()]
#     df = df[pd.to_numeric(df["duration_ms"], errors="coerce").notna()]
    
#     # Cast types to optimized formats
#     df["id"] = df["id"].astype("int32")
#     df["popularity"] = df["popularity"].astype("int32")
#     df["duration_ms"] = df["duration_ms"].astype("int32")
#     df["explicit"] = df["explicit"].astype("int8")
#     df["danceability"] = df["danceability"].astype("float32")
#     df["energy"] = df["energy"].astype("float32")
#     df["key"] = pd.to_numeric(df["key"], errors="coerce").fillna(0).astype("int32")
#     df["loudness"] = df["loudness"].astype("float32")
#     df["mode"] = pd.to_numeric(df["mode"], errors="coerce").fillna(0).astype("int32")
#     df["speechiness"] = df["speechiness"].astype("float32")
#     df["acousticness"] = df["acousticness"].astype("float32")
#     df["instrumentalness"] = df["instrumentalness"].astype("float32")
#     df["liveness"] = df["liveness"].astype("float32")
#     df["valence"] = df["valence"].astype("float32")
#     df["tempo"] = df["tempo"].astype("float32")
#     df["time_signature"] = pd.to_numeric(df["time_signature"], errors="coerce").fillna(0).astype("int32")
    
#     # Clean string fields
#     df["track_genre"] = df["track_genre"].str.lower().str.strip()
#     df["track_name"] = df["track_name"].astype(str)
#     df["album_name"] = df["album_name"].astype(str)
#     df["artists"] = df["artists"].astype(str)
    
#     logger.info(f"✅ Songs validation complete: {initial_count} → {len(df)} records")
#     return df

# def validate_streams(df, stream_key):
#     """Validate and clean streaming data"""
#     initial_count = len(df)
    
#     # Drop rows with missing critical fields
#     df.dropna(subset=["user_id", "track_id", "listen_time"], inplace=True)
#     df["user_id"] = df["user_id"].astype(str)
#     df["track_id"] = df["track_id"].astype(str)
    
#     # Parse timestamps
#     df["listen_time"] = pd.to_datetime(df["listen_time"], errors="coerce")
#     dropped = df["listen_time"].isna().sum()
#     if dropped > 0:
#         logger.warning(f"🕒 Dropping {dropped} rows with invalid timestamps.")
#     df.dropna(subset=["listen_time"], inplace=True)
    
#     logger.info(f"✅ Streams validation complete: {stream_key} → {initial_count} → {len(df)} records")
#     return df

# def validate_metadata_once():
#     """
#     Validate songs and users metadata only if not already processed.
#     This ensures we don't reprocess static data on every pipeline run.
#     """
#     try:
#         # Check if metadata has already been validated
#         if check_s3_object_exists(BUCKET, METADATA_VALIDATED_FLAG):
#             logger.info("📋 Metadata already validated. Skipping songs and users processing.")
#             return True
        
#         logger.info("🔄 First-time metadata validation starting...")
        
#         # Load and validate songs
#         logger.info("📥 Loading songs metadata...")
#         songs_df = read_csv_from_s3(BUCKET, SONGS_KEY)
#         validate_columns(songs_df, [
#             "track_id", "track_name", "artists", "popularity", "duration_ms", "track_genre"
#         ], "songs.csv")
#         songs_df = validate_songs(songs_df)
#         write_parquet_to_s3(songs_df, BUCKET, SONGS_OUT)
        
#         # Load and validate users
#         logger.info("📥 Loading users metadata...")
#         users_df = read_csv_from_s3(BUCKET, USERS_KEY)
#         validate_columns(users_df, [
#             "user_id", "user_name", "user_age", "user_country", "created_at"
#         ], "users.csv")
#         users_df = validate_users(users_df)
#         write_parquet_to_s3(users_df, BUCKET, USERS_OUT)
        
#         # Create validation flag to prevent reprocessing
#         import boto3
#         s3 = boto3.client("s3")
#         s3.put_object(
#             Bucket=BUCKET, 
#             Key=METADATA_VALIDATED_FLAG, 
#             Body=f"Metadata validated on {pd.Timestamp.now()}"
#         )
        
#         logger.info("✅ Metadata validation complete and cached.")
#         return True
        
#     except Exception as e:
#         logger.exception("❌ Failed during metadata validation.")
#         raise

# def process_new_streams():
#     """
#     Process only new streaming files that haven't been processed yet.
#     This is the core efficiency improvement - only process incremental data.
#     """
#     try:
#         logger.info("🔎 Scanning for new streaming files...")
        
#         # Get all stream files and already processed files
#         all_stream_keys = [
#             key for key in list_s3_files(BUCKET, STREAMS_PREFIX) 
#             if key.endswith(".csv") and "stream" in key.lower()
#         ]
#         processed_keys = read_manifest(BUCKET, MANIFEST_KEY)
        
#         # Find new files to process
#         new_files = [key for key in all_stream_keys if key not in processed_keys]
        
#         if not new_files:
#             logger.info("📭 No new streaming files to process.")
#             return []
        
#         logger.info(f"🆕 Found {len(new_files)} new stream files to process:")
#         for file in new_files:
#             logger.info(f"   - {file}")
        
#         processed_files = []
        
#         for stream_key in new_files:
#             try:
#                 logger.info(f"📥 Processing stream file: {stream_key}")
                
#                 # Load and validate stream data
#                 df = read_csv_from_s3(BUCKET, stream_key)
#                 validate_columns(df, ["user_id", "track_id", "listen_time"], stream_key)
#                 df = validate_streams(df, stream_key)
                
#                 # Save validated stream data with organized naming
#                 stream_name = os.path.basename(stream_key).replace(".csv", "")
#                 timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
#                 validated_key = f"{STREAMS_VALIDATED_PREFIX}{stream_name}_validated_{timestamp}.parquet"
                
#                 write_parquet_to_s3(df, BUCKET, validated_key)
#                 processed_files.append(stream_key)
                
#                 logger.info(f"✅ Successfully processed: {stream_key} → {validated_key}")
                
#             except Exception as stream_err:
#                 logger.exception(f"❌ Failed processing {stream_key}. Skipping to next file.")
#                 continue
        
#         # Update manifest with successfully processed files
#         if processed_files:
#             update_manifest(BUCKET, MANIFEST_KEY, processed_files)
#             logger.info(f"📝 Updated manifest with {len(processed_files)} processed files.")
        
#         return processed_files
        
#     except Exception as e:
#         logger.exception("❌ Failed during stream processing.")
#         raise

# def validate():
#     """
#     Main validation function with optimized metadata handling.
#     Only processes metadata once, then focuses on incremental stream data.
#     """
#     try:
#         logger.info("🚀 Starting optimized extraction and validation process...")
        
#         # Step 1: Validate metadata only if not already done
#         validate_metadata_once()
        
#         # Step 2: Process only new streaming files
#         processed_streams = process_new_streams()
        
#         if processed_streams:
#             logger.info(f"🏁 Pipeline complete. Processed {len(processed_streams)} new stream files.")
#         else:
#             logger.info("🏁 Pipeline complete. No new data to process.")
            
#     except Exception as e:
#         logger.exception("❌ Failed during extract_and_validate execution.")
#         raise

# if __name__ == "__main__":
#     validate()
