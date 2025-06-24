import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
import logging

# === Setup Logging ===
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# === Get job parameters ===
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'S3_BUCKET',
    'TRANSFORMED_DATA_PATH',
    'DYNAMODB_TABLE_NAME'
])

# === Initialize Spark and Glue contexts ===
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def load_transformed_data():
    """Load all transformed KPI data from S3"""
    try:
        logger.info("Loading transformed KPI data from S3...")
        
        base_path = f"s3://{args['S3_BUCKET']}/{args['TRANSFORMED_DATA_PATH']}"
        
        # Load all KPI datasets
        genre_kpis_df = spark.read.parquet(f"{base_path}/genre_kpis/")
        top_songs_df = spark.read.parquet(f"{base_path}/top_songs/")
        top_genres_df = spark.read.parquet(f"{base_path}/top_genres/")
        
        logger.info(f"Loaded {genre_kpis_df.count()} genre KPI records")
        logger.info(f"Loaded {top_songs_df.count()} top song records")
        logger.info(f"Loaded {top_genres_df.count()} top genre records")
        
        return genre_kpis_df, top_songs_df, top_genres_df
        
    except Exception as e:
        logger.error(f"Failed to load transformed data: {str(e)}")
        raise

def reshape_genre_kpis_for_dynamodb(genre_kpis_df):
    """Reshape genre KPIs into DynamoDB format"""
    try:
        logger.info("Reshaping genre KPIs for DynamoDB...")
        
        # Create multiple rows for each metric type
        metrics_list = []
        
        # Listen Count
        listen_count_df = genre_kpis_df.select(
            F.concat(F.lit("GENRE#"), F.col("track_genre"), F.lit("#DATE#"), F.col("date")).alias("pk"),
            F.lit("METRIC#listen_count").alias("sk"),
            F.col("listen_count").cast("string").alias("value"),
            F.lit("listen_count").alias("metric_type"),
            F.col("date").alias("date"),
            F.col("track_genre").alias("genre")
        )
        
        # Unique Listeners
        unique_listeners_df = genre_kpis_df.select(
            F.concat(F.lit("GENRE#"), F.col("track_genre"), F.lit("#DATE#"), F.col("date")).alias("pk"),
            F.lit("METRIC#unique_listeners").alias("sk"),
            F.col("unique_listeners").cast("string").alias("value"),
            F.lit("unique_listeners").alias("metric_type"),
            F.col("date").alias("date"),
            F.col("track_genre").alias("genre")
        )
        
        # Total Listening Time
        total_time_df = genre_kpis_df.select(
            F.concat(F.lit("GENRE#"), F.col("track_genre"), F.lit("#DATE#"), F.col("date")).alias("pk"),
            F.lit("METRIC#total_listening_time_ms").alias("sk"),
            F.col("total_listening_time_ms").cast("string").alias("value"),
            F.lit("total_listening_time_ms").alias("metric_type"),
            F.col("date").alias("date"),
            F.col("track_genre").alias("genre")
        )
        
        # Average Listening Time
        avg_time_df = genre_kpis_df.select(
            F.concat(F.lit("GENRE#"), F.col("track_genre"), F.lit("#DATE#"), F.col("date")).alias("pk"),
            F.lit("METRIC#avg_listening_time_ms").alias("sk"),
            F.col("avg_listening_time_ms").cast("string").alias("value"),
            F.lit("avg_listening_time_ms").alias("metric_type"),
            F.col("date").alias("date"),
            F.col("track_genre").alias("genre")
        )
        
        # Union all metric DataFrames
        all_metrics_df = listen_count_df.union(unique_listeners_df).union(total_time_df).union(avg_time_df)
        
        logger.info(f"Reshaped {all_metrics_df.count()} genre KPI records")
        return all_metrics_df
        
    except Exception as e:
        logger.error(f"Failed to reshape genre KPIs: {str(e)}")
        raise

def reshape_top_songs_for_dynamodb(top_songs_df):
    """Reshape top songs into DynamoDB format"""
    try:
        logger.info("Reshaping top songs for DynamoDB...")
        
        reshaped_df = top_songs_df.select(
            F.concat(F.lit("GENRE#"), F.col("track_genre"), F.lit("#DATE#"), F.col("date")).alias("pk"),
            F.concat(F.lit("SONG#"), F.col("rank"), F.lit("#"), F.col("track_id")).alias("sk"),
            F.col("track_name").alias("song_name"),
            F.col("artists").alias("artists"),
            F.col("play_count").cast("string").alias("play_count"),
            F.col("rank").cast("string").alias("rank"),
            F.col("date").alias("date"),
            F.col("track_genre").alias("genre"),
            F.lit("top_song").alias("record_type")
        )
        
        logger.info(f"Reshaped {reshaped_df.count()} top song records")
        return reshaped_df
        
    except Exception as e:
        logger.error(f"Failed to reshape top songs: {str(e)}")
        raise

def reshape_top_genres_for_dynamodb(top_genres_df):
    """Reshape top genres into DynamoDB format"""
    try:
        logger.info("Reshaping top genres for DynamoDB...")
        
        reshaped_df = top_genres_df.select(
            F.concat(F.lit("DATE#"), F.col("date")).alias("pk"),
            F.concat(F.lit("GENRE_RANK#"), F.col("rank")).alias("sk"),
            F.col("track_genre").alias("genre"),
            F.col("total_plays").cast("string").alias("total_plays"),
            F.col("rank").cast("string").alias("rank"),
            F.col("date").alias("date"),
            F.lit("top_genre").alias("record_type")
        )
        
        logger.info(f"Reshaped {reshaped_df.count()} top genre records")
        return reshaped_df
        
    except Exception as e:
        logger.error(f"Failed to reshape top genres: {str(e)}")
        raise

def write_to_dynamodb(df, record_type):
    """Write DataFrame to DynamoDB"""
    try:
        logger.info(f"Writing {record_type} to DynamoDB...")
        
        # Convert DataFrame to DynamicFrame
        dynamic_frame = DynamicFrame.fromDF(df, glueContext, f"dynamic_frame_{record_type}")
        
        # Write to DynamoDB
        glueContext.write_dynamic_frame_from_options(
            frame=dynamic_frame,
            connection_type="dynamodb",
            connection_options={
                "dynamodb.output.tableName": args['DYNAMODB_TABLE_NAME'],
                "dynamodb.throughput.write.percent": "0.8"  # Use 80% of write capacity
            }
        )
        
        logger.info(f"Successfully wrote {record_type} to DynamoDB")
        
    except Exception as e:
        logger.error(f"Failed to write {record_type} to DynamoDB: {str(e)}")
        raise

def main():
    """Main DynamoDB ingestion logic"""
    try:
        logger.info("Starting DynamoDB ingestion job...")
        
        # Step 1: Load transformed data from S3
        genre_kpis_df, top_songs_df, top_genres_df = load_transformed_data()
        
        # Step 2: Reshape data for DynamoDB single-table design
        genre_metrics_df = reshape_genre_kpis_for_dynamodb(genre_kpis_df)
        top_songs_reshaped_df = reshape_top_songs_for_dynamodb(top_songs_df)
        top_genres_reshaped_df = reshape_top_genres_for_dynamodb(top_genres_df)
        
        # Step 3: Write each dataset to DynamoDB
        write_to_dynamodb(genre_metrics_df, "genre_metrics")
        write_to_dynamodb(top_songs_reshaped_df, "top_songs")
        write_to_dynamodb(top_genres_reshaped_df, "top_genres")
        
        logger.info("🎉 DynamoDB ingestion job completed successfully!")
        
    except Exception as e:
        logger.exception("DynamoDB ingestion job failed")
        raise

if __name__ == "__main__":
    main()
    job.commit()
