import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import boto3
import logging

# === Setup Logging ===
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# === Get job parameters ===
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'S3_BUCKET',
    'STREAMS_INPUT_PATH',
    'SONGS_INPUT_PATH', 
    'USERS_INPUT_PATH',
    'OUTPUT_PATH',
    'PROCESS_DATE'
])

# === Initialize Spark and Glue contexts ===
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def load_validated_data():
    """Load validated parquet data from S3"""
    try:
        logger.info("📥 Loading validated datasets...")
        
        # Load songs metadata (static)
        songs_df = spark.read.parquet(f"s3://{args['S3_BUCKET']}/{args['SONGS_INPUT_PATH']}")
        logger.info(f"✅ Loaded {songs_df.count()} songs records")
        
        # Load users metadata (static)
        users_df = spark.read.parquet(f"s3://{args['S3_BUCKET']}/{args['USERS_INPUT_PATH']}")
        logger.info(f"✅ Loaded {users_df.count()} users records")
        
        # Load streaming data (incremental)
        streams_df = spark.read.parquet(f"s3://{args['S3_BUCKET']}/{args['STREAMS_INPUT_PATH']}")
        logger.info(f"✅ Loaded {streams_df.count()} streaming records")
        
        return songs_df, users_df, streams_df
        
    except Exception as e:
        logger.error(f"❌ Failed to load data: {str(e)}")
        raise

def enrich_streaming_data(streams_df, songs_df, users_df):
    """Join streaming data with songs and users metadata"""
    try:
        logger.info("🔗 Enriching streaming data with metadata...")
        
        # Join streams with songs to get genre and other song attributes
        enriched_df = streams_df.join(
            songs_df.select("track_id", "track_name", "track_genre", "artists", "duration_ms"),
            on="track_id",
            how="inner"
        )
        
        # Join with users to get user demographics
        enriched_df = enriched_df.join(
            users_df.select("user_id", "user_age", "user_country"),
            on="user_id", 
            how="inner"
        )
        
        # Add date column for daily aggregations
        enriched_df = enriched_df.withColumn("date", F.to_date(F.col("listen_time")))
        
        # Filter for the specific process date if provided
        if args['PROCESS_DATE'] != 'NONE':
            process_date = datetime.strptime(args['PROCESS_DATE'], '%Y-%m-%d').date()
            enriched_df = enriched_df.filter(F.col("date") == F.lit(process_date))
            logger.info(f"🗓️ Filtering data for date: {process_date}")
        
        logger.info(f"✅ Enriched dataset contains {enriched_df.count()} records")
        return enriched_df
        
    except Exception as e:
        logger.error(f"❌ Failed to enrich data: {str(e)}")
        raise

def compute_daily_genre_kpis(enriched_df):
    """Compute daily genre-level KPIs as specified in requirements"""
    try:
        logger.info("📊 Computing daily genre-level KPIs...")
        
        # 1. Daily Genre-Level KPIs
        genre_kpis = enriched_df.groupBy("track_genre", "date").agg(
            F.count("*").alias("listen_count"),
            F.countDistinct("user_id").alias("unique_listeners"),
            F.sum("duration_ms").alias("total_listening_time_ms"),
            F.avg("duration_ms").alias("avg_listening_time_ms")
        ).withColumn("avg_listening_time_per_user", 
                    F.col("total_listening_time_ms") / F.col("unique_listeners"))
        
        logger.info(f"✅ Computed genre KPIs for {genre_kpis.count()} genre-date combinations")
        return genre_kpis
        
    except Exception as e:
        logger.error(f"❌ Failed to compute genre KPIs: {str(e)}")
        raise

def compute_top_songs_per_genre(enriched_df):
    """Compute top 3 songs per genre per day"""
    try:
        logger.info("🎵 Computing top 3 songs per genre per day...")
        
        # Count plays per song per genre per day
        song_counts = enriched_df.groupBy("track_genre", "date", "track_id", "track_name", "artists").agg(
            F.count("*").alias("play_count")
        )
        
        # Rank songs within each genre-date combination
        window_spec = Window.partitionBy("track_genre", "date").orderBy(F.desc("play_count"))
        top_songs = song_counts.withColumn("rank", F.row_number().over(window_spec)) \
                              .filter(F.col("rank") <= 3) \
                              .select("track_genre", "date", "rank", "track_id", "track_name", 
                                     "artists", "play_count")
        
        logger.info(f"✅ Computed top songs for {top_songs.count()} entries")
        return top_songs
        
    except Exception as e:
        logger.error(f"❌ Failed to compute top songs: {str(e)}")
        raise

def compute_top_genres_per_day(enriched_df):
    """Compute top 5 genres per day"""
    try:
        logger.info("🎭 Computing top 5 genres per day...")
        
        # Count total plays per genre per day
        genre_totals = enriched_df.groupBy("track_genre", "date").agg(
            F.count("*").alias("total_plays")
        )
        
        # Rank genres within each date
        window_spec = Window.partitionBy("date").orderBy(F.desc("total_plays"))
        top_genres = genre_totals.withColumn("rank", F.row_number().over(window_spec)) \
                                .filter(F.col("rank") <= 5) \
                                .select("date", "rank", "track_genre", "total_plays")
        
        logger.info(f"✅ Computed top genres for {top_genres.count()} entries")
        return top_genres
        
    except Exception as e:
        logger.error(f"❌ Failed to compute top genres: {str(e)}")
        raise

def save_transformed_data(genre_kpis, top_songs, top_genres):
    """Save all computed KPIs to S3 in parquet format"""
    try:
        logger.info("💾 Saving transformed data to S3...")
        
        output_base = f"s3://{args['S3_BUCKET']}/{args['OUTPUT_PATH']}"
        
        # Save genre KPIs
        genre_kpis.coalesce(1).write.mode("overwrite").parquet(f"{output_base}/genre_kpis/")
        logger.info("✅ Saved genre KPIs")
        
        # Save top songs
        top_songs.coalesce(1).write.mode("overwrite").parquet(f"{output_base}/top_songs/")
        logger.info("✅ Saved top songs")
        
        # Save top genres
        top_genres.coalesce(1).write.mode("overwrite").parquet(f"{output_base}/top_genres/")
        logger.info("✅ Saved top genres")
        
        logger.info("🎉 All transformed data saved successfully!")
        
    except Exception as e:
        logger.error(f"❌ Failed to save data: {str(e)}")
        raise

def main():
    """Main transformation logic"""
    try:
        logger.info("🚀 Starting music streaming data transformation...")
        
        # Step 1: Load validated data
        songs_df, users_df, streams_df = load_validated_data()
        
        # Step 2: Enrich streaming data with metadata
        enriched_df = enrich_streaming_data(streams_df, songs_df, users_df)
        
        # Step 3: Compute all required KPIs
        genre_kpis = compute_daily_genre_kpis(enriched_df)
        top_songs = compute_top_songs_per_genre(enriched_df)
        top_genres = compute_top_genres_per_day(enriched_df)
        
        # Step 4: Save results to S3
        save_transformed_data(genre_kpis, top_songs, top_genres)
        
        logger.info("🏁 Transformation job completed successfully!")
        
    except Exception as e:
        logger.exception("❌ Transformation job failed")
        raise

if __name__ == "__main__":
    main()
    job.commit()
