import sys
import boto3
import pandas as pd
from decimal import Decimal
import logging
from datetime import datetime

# === Setup Logging ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === Get job parameters ===
args = {}
for i, arg in enumerate(sys.argv):
    if arg.startswith('--'):
        key = arg[2:]
        if i + 1 < len(sys.argv) and not sys.argv[i + 1].startswith('--'):
            args[key] = sys.argv[i + 1]

S3_BUCKET = args.get('S3_BUCKET', 'streaming-analytics-buck1')
TRANSFORMED_DATA_PATH = args.get('TRANSFORMED_DATA_PATH', 'transformed-data')
DYNAMODB_TABLE_NAME = args.get('DYNAMODB_TABLE_NAME', 'music-streaming-kpis')

def load_transformed_data():
    """Load all transformed KPI data from S3 using pandas"""
    try:
        logger.info("📥 Loading transformed KPI data from S3...")
        
        base_path = f"s3://{S3_BUCKET}/{TRANSFORMED_DATA_PATH}"
        
        # Load all KPI datasets using pandas (much lighter than Spark)
        genre_kpis_df = pd.read_parquet(f"{base_path}/genre_kpis/")
        top_songs_df = pd.read_parquet(f"{base_path}/top_songs/")
        top_genres_df = pd.read_parquet(f"{base_path}/top_genres/")
        
        logger.info(f"✅ Loaded {len(genre_kpis_df)} genre KPI records")
        logger.info(f"✅ Loaded {len(top_songs_df)} top song records")
        logger.info(f"✅ Loaded {len(top_genres_df)} top genre records")
        
        return genre_kpis_df, top_songs_df, top_genres_df
        
    except Exception as e:
        logger.error(f"❌ Failed to load transformed data: {str(e)}")
        raise

def reshape_genre_kpis_for_dynamodb(genre_kpis_df):
    """Reshape genre KPIs into DynamoDB format using pandas operations"""
    try:
        logger.info("🔄 Reshaping genre KPIs for DynamoDB...")
        
        records = []
        
        for _, row in genre_kpis_df.iterrows():
            base_pk = f"GENRE#{row['track_genre']}#DATE#{row['date']}"
            
            # Create records for each metric type
            metrics = [
                {'metric_type': 'listen_count', 'value': row['listen_count']},
                {'metric_type': 'unique_listeners', 'value': row['unique_listeners']},
                {'metric_type': 'total_listening_time_ms', 'value': row['total_listening_time_ms']},
                {'metric_type': 'avg_listening_time_ms', 'value': row['avg_listening_time_ms']}
            ]
            
            for metric in metrics:
                records.append({
                    'pk': base_pk,
                    'sk': f"METRIC#{metric['metric_type']}",
                    'value': str(metric['value']),
                    'metric_type': metric['metric_type'],
                    'date': str(row['date']),
                    'genre': row['track_genre']
                })
        
        logger.info(f"✅ Reshaped {len(records)} genre KPI records")
        return records
        
    except Exception as e:
        logger.error(f"❌ Failed to reshape genre KPIs: {str(e)}")
        raise

def reshape_top_songs_for_dynamodb(top_songs_df):
    """Reshape top songs into DynamoDB format using pandas operations"""
    try:
        logger.info("🔄 Reshaping top songs for DynamoDB...")
        
        records = []
        
        for _, row in top_songs_df.iterrows():
            records.append({
                'pk': f"GENRE#{row['track_genre']}#DATE#{row['date']}",
                'sk': f"SONG#{row['rank']}#{row['track_id']}",
                'song_name': row['track_name'],
                'artists': row['artists'],
                'play_count': str(row['play_count']),
                'rank': str(row['rank']),
                'date': str(row['date']),
                'genre': row['track_genre'],
                'record_type': 'top_song'
            })
        
        logger.info(f"✅ Reshaped {len(records)} top song records")
        return records
        
    except Exception as e:
        logger.error(f"❌ Failed to reshape top songs: {str(e)}")
        raise

def reshape_top_genres_for_dynamodb(top_genres_df):
    """Reshape top genres into DynamoDB format using pandas operations"""
    try:
        logger.info("🔄 Reshaping top genres for DynamoDB...")
        
        records = []
        
        for _, row in top_genres_df.iterrows():
            records.append({
                'pk': f"DATE#{row['date']}",
                'sk': f"GENRE_RANK#{row['rank']}",
                'genre': row['track_genre'],
                'total_plays': str(row['total_plays']),
                'rank': str(row['rank']),
                'date': str(row['date']),
                'record_type': 'top_genre'
            })
        
        logger.info(f"✅ Reshaped {len(records)} top genre records")
        return records
        
    except Exception as e:
        logger.error(f"❌ Failed to reshape top genres: {str(e)}")
        raise

def write_to_dynamodb_batch(records, record_type):
    """Write records to DynamoDB using efficient batch operations"""
    try:
        logger.info(f"💾 Writing {len(records)} {record_type} records to DynamoDB...")
        
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(DYNAMODB_TABLE_NAME)
        
        # Process in batches of 25 (DynamoDB batch_writer limit)
        batch_size = 25
        written_count = 0
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            
            with table.batch_writer() as batch_writer:
                for record in batch:
                    # Convert numeric values to Decimal for DynamoDB compatibility
                    processed_record = {}
                    for key, value in record.items():
                        if isinstance(value, (int, float)):
                            processed_record[key] = Decimal(str(value))
                        else:
                            processed_record[key] = value
                    
                    batch_writer.put_item(Item=processed_record)
                    written_count += 1
            
            logger.info(f"📝 Processed batch {i//batch_size + 1}, written {written_count} records so far")
        
        logger.info(f"✅ Successfully wrote {written_count} {record_type} records to DynamoDB")
        
    except Exception as e:
        logger.error(f"❌ Failed to write {record_type} to DynamoDB: {str(e)}")
        raise

def main():
    """Main DynamoDB ingestion logic using Python Shell"""
    try:
        logger.info("🚀 Starting Python Shell DynamoDB ingestion job...")
        
        # Step 1: Load transformed data from S3 using pandas
        genre_kpis_df, top_songs_df, top_genres_df = load_transformed_data()
        
        # Step 2: Reshape data for DynamoDB single-table design using pandas
        genre_metrics_records = reshape_genre_kpis_for_dynamodb(genre_kpis_df)
        top_songs_records = reshape_top_songs_for_dynamodb(top_songs_df)
        top_genres_records = reshape_top_genres_for_dynamodb(top_genres_df)
        
        # Step 3: Write each dataset to DynamoDB using batch operations
        write_to_dynamodb_batch(genre_metrics_records, "genre_metrics")
        write_to_dynamodb_batch(top_songs_records, "top_songs")
        write_to_dynamodb_batch(top_genres_records, "top_genres")
        
        logger.info("🎉 Python Shell DynamoDB ingestion job completed successfully!")
        
    except Exception as e:
        logger.exception("❌ DynamoDB ingestion job failed")
        raise

if __name__ == "__main__":
    main()
