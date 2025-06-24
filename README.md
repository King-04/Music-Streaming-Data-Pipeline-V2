# Music Streaming Data Pipeline

**A real-time, event-driven ETL pipeline for processing music streaming data using Apache Airflow, AWS Glue, and DynamoDB**

## 📋 Project Overview

This project implements a comprehensive data engineering solution for a music streaming service that processes user streaming behavior data arriving at unpredictable intervals. The pipeline automatically ingests streaming data from Amazon S3, validates and transforms it using AWS Glue, computes daily KPIs, and stores results in DynamoDB for fast access by downstream applications.

### Problems Solved
- **Real-time Processing**: Handles streaming data arriving at irregular intervals without manual intervention
- **Data Quality**: Automated validation ensures data integrity before processing
- **Scalable Analytics**: Computes complex daily KPIs efficiently using distributed processing
- **Fast Lookups**: Optimized DynamoDB storage design for sub-millisecond query performance
- **Event-Driven Architecture**: Lambda-triggered pipeline responds immediately to new data arrivals

## ✨ Features

- **Event-Driven Ingestion**: S3 events automatically trigger pipeline execution via Lambda
- **Data Validation**: Automated schema validation and data quality checks
- **Incremental Processing**: Only processes new data while maintaining historical accuracy
- **Daily KPI Computation**: 
  - Genre-level listen counts and unique listeners
  - Total and average listening times per user
  - Top 3 songs per genre per day
  - Top 5 genres per day
- **Optimized Storage**: Single-table DynamoDB design for fast queries
- **File Archival**: Automatic cleanup and archival of processed files
- **Comprehensive Monitoring**: CloudWatch logging and error handling
- **Cost Optimization**: Pay-per-use architecture with intelligent resource scaling

## 🏗️ Architecture
![]()


### Data Flow
1. **Ingestion**: Stream files uploaded to S3 trigger Lambda function
2. **Orchestration**: Lambda triggers MWAA Airflow DAG
3. **Validation**: Glue job validates data schema and quality
4. **Transformation**: PySpark jobs compute daily KPIs with metadata enrichment
5. **Storage**: Processed metrics stored in DynamoDB for fast access
6. **Archival**: Successfully processed files moved to archive directory

## 🛠️ Tech Stack

### **Infrastructure & Orchestration**
- **Apache Airflow (MWAA)** - Workflow orchestration
- **AWS Lambda** - Event-driven triggers
- **Amazon S3** - Data lake storage

### **Data Processing**
- **AWS Glue** - Serverless ETL jobs
- **PySpark** - Distributed data processing
- **Python** - Data validation and utilities

### **Data Storage**
- **Amazon DynamoDB** - NoSQL database for KPIs
- **Amazon S3** - Raw, validated, and transformed data storage

### **Monitoring & Logging**
- **Amazon CloudWatch** - Logging and monitoring
- **AWS X-Ray** - Distributed tracing

### **Development & Deployment**
- **Python 3.10+** - Primary programming language
- **Pandas** - Data manipulation
- **Boto3** - AWS SDK for Python

## 🚀 Setup Instructions

### Prerequisites
- AWS Account with appropriate permissions
- Python 3.10 or higher
- AWS CLI configured
- Access to MWAA environment

### 1. Clone Repository

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure Environment Variables
Create a `.env` file:
```bash
# AWS Configuration
AWS_REGION=us-east-1
AWS_ACCOUNT_ID=your-account-id

# S3 Configuration
S3_BUCKET=your-bucket-name
S3_RAW_DATA_PREFIX=streams/
S3_VALIDATED_PREFIX=validated-data/
S3_TRANSFORMED_PREFIX=transformed-data/
S3_ARCHIVE_PREFIX=archive/

# MWAA Configuration
MWAA_ENV_NAME=your-mwaa-environment
DAG_NAME=music_etl_pipeline

# DynamoDB Configuration
DYNAMODB_TABLE_NAME=music-streaming-kpis
```

### 4. Create AWS Resources

#### S3 Bucket Structure
```bash
aws s3 mb s3://your-bucket-name
aws s3api put-object --bucket your-bucket-name --key streams/
aws s3api put-object --bucket your-bucket-name --key validated-data/
aws s3api put-object --bucket your-bucket-name --key transformed-data/
aws s3api put-object --bucket your-bucket-name --key archive/
aws s3api put-object --bucket your-bucket-name --key processing-metadata/
```

#### DynamoDB Table
```bash
aws dynamodb create-table \
    --table-name music-streaming-kpis \
    --attribute-definitions \
        AttributeName=pk,AttributeType=S \
        AttributeName=sk,AttributeType=S \
    --key-schema \
        AttributeName=pk,KeyType=HASH \
        AttributeName=sk,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST
```

### 5. Deploy Glue Jobs
```bash
# Upload Glue scripts to S3
aws s3 cp glue-scripts/ s3://your-bucket-name/glue-scripts/ --recursive

# Create Glue jobs using AWS Console or CLI
# See deployment section for detailed instructions
```

## 📊 Data Pipeline Description

### Input Data Schema
- **Songs Metadata**: `track_id`, `track_name`, `artists`, `track_genre`, `popularity`, `duration_ms`
- **Users Metadata**: `user_id`, `user_name`, `user_age`, `user_country`, `created_at`
- **Streaming Data**: `user_id`, `track_id`, `listen_time` (timestamp)

### Processing Logic
1. **Validation**: Schema validation, data type checking, null value handling
2. **Enrichment**: Join streaming data with songs and users metadata
3. **Aggregation**: Compute daily KPIs using PySpark window functions
4. **Storage**: Reshape data for DynamoDB single-table design

### Output KPIs
- Daily genre-level metrics (listen count, unique listeners, total/avg listening time)
- Top 3 songs per genre per day
- Top 5 genres per day

## 💻 Usage Guide

### Running the Pipeline

#### Manual Trigger
```bash
# Trigger DAG via MWAA CLI
aws mwaa create-cli-token --name your-mwaa-environment
# Use token to trigger DAG in MWAA console
```

#### Automatic Trigger
1. Upload stream files to `s3://your-bucket-name/streams/`
2. Lambda function automatically triggers the pipeline
3. Monitor execution in MWAA console

### Querying Results

#### DynamoDB Console Queries
```bash
# Get all metrics for rock genre on specific date
pk = GENRE#rock#DATE#2025-06-24
sk begins_with METRIC#

# Get top songs for a genre
pk = GENRE#rock#DATE#2025-06-24  
sk begins_with SONG#

# Get top genres for a date
pk = DATE#2025-06-24
sk begins_with GENRE_RANK#
```

#### Programmatic Access
```python
import boto3

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('music-streaming-kpis')

# Query genre metrics
response = table.query(
    KeyConditionExpression=Key('pk').eq('GENRE#rock#DATE#2025-06-24')
)
```

## 🚀 Deployment Instructions

### 1. Deploy Lambda Function
```bash
# Create deployment package
zip -r lambda-dag-trigger.zip lambda_function.py

# Deploy function
aws lambda create-function \
    --function-name music-streaming-dag-trigger \
    --runtime python3.10 \
    --role arn:aws:iam::your-account:role/lambda-execution-role \
    --handler lambda_function.lambda_handler \
    --zip-file fileb://lambda-dag-trigger.zip \
    --environment Variables='{MWAA_ENV_NAME=your-mwaa-env,DAG_NAME=music_etl_pipeline}'
```

### 2. Configure S3 Event Notifications
```bash
# Add Lambda permission for S3
aws lambda add-permission \
    --function-name music-streaming-dag-trigger \
    --principal s3.amazonaws.com \
    --action lambda:InvokeFunction \
    --source-arn arn:aws:s3:::your-bucket-name

# Configure S3 event notification in AWS Console
# Trigger: ObjectCreated, Prefix: streams/, Suffix: .csv
```

### 3. Deploy Airflow DAG
```bash
# Upload DAG to MWAA S3 bucket
aws s3 cp dags/music_etl_pipeline.py s3://your-mwaa-bucket/dags/
```

### 4. Create Glue Jobs
Use AWS Glue Console to create jobs with provided scripts:
- `music-streaming-transform` (PySpark job)
- `music-streaming-dynamodb-ingestion` (Python Shell job)

## 🧪 Testing

### Unit Tests
```bash
# Run validation tests
python -m pytest tests/test_validation.py -v

# Run transformation tests  
python -m pytest tests/test_transformation.py -v
```

### Integration Tests
```bash
# Test complete pipeline with sample data
python tests/integration_test.py
```

### Sample Data
```bash
# Upload test data
aws s3 cp sample-data/test-stream.csv s3://your-bucket-name/streams/
```

## ⚠️ Known Issues & Limitations

- **Cold Start Latency**: Lambda cold starts may add 1-2 seconds to trigger time
- **DynamoDB Throttling**: High-volume writes may require provisioned capacity
- **Glue Job Startup**: Initial job startup takes 2-3 minutes

## 🔮 Future Improvements

### Short Term
- [ ] Create CloudFormation templates for infrastructure

### Long Term
- [ ] Real-time streaming with Kinesis Data Streams
- [ ] Machine learning models for music recommendation
- [ ] Advanced analytics with Amazon QuickSight dashboards

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

<!-- ## 📞 Contact & Maintainer Info

**Project Maintainer**: [
- **Email**: your.email@example.com
- **LinkedIn**: [Your LinkedIn Profile]
- **GitHub**: [@your-username](https://github.com/your-username)

**Project Repository**: [https://github.com/your-username/music-streaming-pipeline](https://github.com/your-username/music-streaming-pipeline)

--- -->