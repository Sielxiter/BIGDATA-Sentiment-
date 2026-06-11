# Project Context for AI Assistants

This document explains the current project state, the target AWS deployment, and the role of each active file in the repository.

## 1. Project Goal

Build a sentiment analysis platform for public YouTube comments with this production workflow:

`AWS Lambda -> Kafka on EC2 -> Spark on EC2 -> Fine-tuned BERT on EC2 -> Elasticsearch on EC2 -> Streamlit on EC2`

The active collector now uses the YouTube Data API. Streamlit can run the collector locally for testing or invoke Lambda in AWS mode depending on `COLLECTOR_MODE`. Lambda can reuse the latest saved collector settings from S3 when EventBridge triggers it on a schedule.

## 2. Important Constraints

- Deploy the runtime on AWS.
- Use AWS Lambda as the serverless collector when `COLLECTOR_MODE=lambda`.
- Use EC2 for Kafka, Spark, the fine-tuned BERT runtime, Elasticsearch, and Streamlit.
- Do not hardcode AWS credentials in code.
- Lambda must be able to reach Kafka over the network.
- Lambda also needs outbound internet access to reach the YouTube Data API.
- Lambda may optionally write a backup JSONL file to S3.
- The raw Kafka topic must remain `raw_social_posts`.
- Respectful API pacing must remain enabled.
- The collector depends on the YouTube Data API and a valid API key with quota.

## 3. Current Active Repository Tree

Only the files below are part of the current workflow.

### `cloud/`

- `cloud/youtube_api_collector.py`
  - Active YouTube collector that searches videos and collects public comments through the YouTube Data API.
  - Normalizes documents into Kafka-ready JSON.
  - Supports local mode and Lambda mode.

- `cloud/lambda_youtube_to_kafka.py`
  - AWS Lambda handler for the YouTube collector.
  - Collects comments, publishes them to Kafka, and optionally uploads a JSONL backup to S3.

- `cloud/kafka_client.py`
  - Reusable Kafka producer helper for Lambda and cloud-side code.
  - Reads Kafka connection settings from environment variables.
  - Handles bootstrap servers and optional SASL/SSL settings.

- `cloud/s3_backup.py`
  - Helper for writing collected records to S3 as JSONL.

- `cloud/collector_config.py`
  - Helper for saving and loading the active collector configuration from S3.

- `cloud/collector_state.py`
  - Helper for saving and loading the per-event dedup state for already-ingested YouTube comment IDs.

- `cloud/__init__.py`
  - Marks `cloud/` as a Python package.

### `dashboard/`

- `dashboard/app.py`
  - Streamlit dashboard for YouTube collection.
  - Supports `COLLECTOR_MODE=local` and `COLLECTOR_MODE=lambda`.
  - Shows the live Elasticsearch sentiment chart and collection results.

- `dashboard/aws_lambda.py`
  - Helper used by Streamlit to invoke Lambda synchronously with `boto3`.
  - Handles missing credentials, permission errors, and Lambda execution errors.

- `dashboard/__init__.py`
  - Package marker.

### `spark_app/`

- `spark_app/streaming_job.py`
  - Spark Structured Streaming job.
  - Reads raw messages from Kafka topic `raw_social_posts`.
  - Applies transformations and sends processed data to Elasticsearch.

- `spark_app/transformations.py`
  - Data-cleaning and feature transformation helpers used by the Spark job.
  - Accepts YouTube messages with `source="youtube"`.

- `spark_app/schemas.py`
  - Defines the expected schema for raw Kafka messages.
  - Includes YouTube fields such as `channel_id`, `channel_title`, `video_id`, `video_title`, and `collected_at`.

- `spark_app/__init__.py`
  - Package marker.

### `nlp/`

- `nlp/bert_sentiment_model.py`
  - Loads and runs the BERT-based sentiment model.

- `nlp/sentiment_service.py`
  - Service wrapper around the sentiment model.
  - Used by Spark processing.

- `nlp/trained_bert_model.py`
  - Wrapper for the fine-tuned BERT model artifacts stored locally or on EC2.

- `nlp/preprocessing.py`
  - Text cleaning helpers for sentiment inference.

- `nlp/base_model.py`
  - Base abstraction for model implementations.

- `nlp/__init__.py`
  - Package marker.

### `storage/`

- `storage/elastic_client.py`
  - Elasticsearch client helpers.
  - Normalizes and indexes processed documents into Elasticsearch.

- `storage/index_mapping.py`
  - Elasticsearch mapping for the processed sentiment index.
  - Includes YouTube metadata fields.

- `storage/init_elasticsearch.py`
  - Utility to create the Elasticsearch index.

- `storage/__init__.py`
  - Package marker.

### `deploy/`

- `deploy/nginx.conf`
  - Reverse proxy configuration for putting Streamlit behind nginx on EC2.

### `config/`

- `config/settings.py`
  - Central environment-based configuration.
  - Holds Kafka, Elasticsearch, collector, backup, API key, and model settings.

- `config/__init__.py`
  - Package marker.

### Root files

- `README.md`
  - Short summary of the project and the current YouTube workflow.

- `workflow.md`
  - Production workflow notes and local testing instructions.

- `requirements.txt`
  - Python dependencies for the active workflow.

- `.env.example`
  - Example environment variables for local development and deployment.

- `scripts/test_youtube_api_collector_local.py`
  - Local YouTube API collector test script.

## 4. What Was Removed From the Main Workflow

These were removed because they are not part of the current AWS EC2 + Lambda production flow:

- `collectors/`
- `kafka_app/`
- `notebooks/`
- `data/`
- `checkpoints/`
- `tools/`
- `context.md`
- `spark.md`
- `wsl.md`

Do not rely on those folders when explaining the current system.

## 5. AWS Deployment Interpretation

When helping with AWS setup, assume this target architecture:

1. `AWS Lambda`
   - Deployed as a serverless function.
   - Runs the YouTube collector in Lambda mode.
   - Publishes normalized comments directly to Kafka.
   - Optionally writes a backup JSONL file to S3.
   - Reads the latest saved collector parameters from S3 when invoked without an explicit payload, which allows EventBridge scheduled runs to reuse the settings chosen in Streamlit.
   - For production, Lambda should run in the same VPC as Kafka or reach Kafka through a private network path.
   - If Lambda is placed in private subnets, it needs a NAT Gateway or equivalent outbound path so it can call the YouTube Data API.
   - The Kafka security group must allow inbound traffic from the Lambda security group on the broker port.
   - Kafka must advertise an address that Lambda can resolve and reach; `localhost` will not work from Lambda.

2. `Kafka on EC2`
   - Kafka broker runs on an EC2 instance or EC2-based cluster.
   - Lambda and local scripts must be able to connect to the broker network endpoint.
   - Security groups must allow the collector -> Kafka path.

3. `Spark on EC2`
   - Spark Structured Streaming reads from Kafka topic `raw_social_posts`.
   - Applies preprocessing and sentiment inference.
   - Writes results to Elasticsearch.

4. `Fine-tuned BERT on EC2`
   - The model artifacts are stored on EC2.
   - Spark or the sentiment service loads the fine-tuned model for inference.

5. `Elasticsearch on EC2`
   - Stores the processed sentiment documents.
   - The dashboard reads from Elasticsearch to build the live chart.

6. `Streamlit on EC2`
   - Serves the dashboard UI.
   - Can invoke Lambda or run the local collector directly depending on `COLLECTOR_MODE`.
   - Can save the active Lambda collector settings to S3 so scheduled Lambda runs use the selected event, search query, video IDs, channel IDs, and comment limits.

## 6. Important Environment Variables

When configuring AWS and EC2, these are the most important values:

- `COLLECTOR_SOURCE`
- `COLLECTOR_MODE`
- `COLLECTOR_LAMBDA_FUNCTION_NAME`
- `COLLECTOR_LAMBDA_REGION`
- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_RAW_TOPIC`
- `KAFKA_SECURITY_PROTOCOL`
- `KAFKA_SASL_MECHANISM`
- `KAFKA_SASL_USERNAME`
- `KAFKA_SASL_PASSWORD`
- `KAFKA_SSL_CA_LOCATION`
- `ELASTICSEARCH_URL`
- `ELASTICSEARCH_INDEX`
- `MODEL_PROVIDER`
- `MODEL_NAME`
- `MODEL_VERSION`
- `MODEL_DEVICE`
- `AWS_REGION`
- `S3_BUCKET_NAME`
- `S3_PREFIX`
- `ENABLE_S3_BACKUP`
- `YOUTUBE_API_KEY`
- `YOUTUBE_API_BASE_URL`
- `YOUTUBE_MAX_VIDEOS`
- `YOUTUBE_MAX_COMMENTS_PER_VIDEO`
- `YOUTUBE_REQUEST_DELAY_SECONDS`
- `YOUTUBE_API_TIMEOUT_SECONDS`
- `YOUTUBE_USER_AGENT`
- `YOUTUBE_PUBLISHED_AFTER`
- `YOUTUBE_PUBLISHED_BEFORE`
- `COLLECTOR_CONFIG_BUCKET_NAME`
- `COLLECTOR_CONFIG_S3_KEY`
- `COLLECTOR_STATE_BUCKET_NAME`
- `COLLECTOR_STATE_S3_KEY`
- `COLLECTOR_STATE_MAX_COMMENT_IDS`

## 7. How the Streamlit Dashboard Works

- The user enters:
  - event name
  - search queries
  - optional video IDs
  - optional channel IDs
  - max videos
  - max comments per video
  - order
  - region code
  - relevance language
  - optional published-after / published-before bounds
  - engine label
- The dashboard includes:
  - a top-level event filter for the live Elasticsearch chart
  - a Lambda settings save flow for query/video/channel limit changes
- If `COLLECTOR_MODE=local`, the dashboard runs the local YouTube collector directly.
- If `COLLECTOR_MODE=lambda`, the dashboard saves the chosen collector settings to S3 and can also trigger the Lambda collector with `boto3` for an immediate run.
- When `COLLECTOR_MODE=lambda`, the saved S3 config is also what EventBridge scheduled invocations reuse.
- The collector also persists a per-event dedup state to S3 so repeated runs skip YouTube comments that were already ingested in earlier successful Kafka publishes.
- The dashboard displays:
  - status
  - session id
  - videos scanned
  - comments collected
  - Kafka topic
  - optional S3 backup key
  - any warnings or errors
- The live chart reads sentiment counts from Elasticsearch.

## 8. Local Testing Flow

To test collection without sending data to Kafka, use:

```bash
python scripts/test_youtube_api_collector_local.py --event "World Cup" --search-query "morocco football" --max-videos 20 --max-comments 50 --send-to-kafka false
```

This prints:

- number of videos scanned
- number of comments collected
- 3 sample documents

If you want JSONL output, add `--output-jsonl path/to/file.jsonl`.

## 9. How to Answer AWS Setup Questions

If asked how to deploy this project on AWS, focus on:

- Lambda deployment package or container image
- EC2 networking for Kafka, Spark, Elasticsearch, and Streamlit
- IAM roles for Lambda and EC2 instances
- Security groups and VPC routing
- S3 backup bucket and `s3:PutObject` permissions
- S3 collector-config bucket and `s3:GetObject` / `s3:PutObject` permissions for Streamlit and Lambda
- Kafka broker endpoint configuration in `KAFKA_BOOTSTRAP_SERVERS`
- Lambda VPC/subnet configuration and NAT access for YouTube API calls
- Kafka security group rules for Lambda-to-EC2 connectivity
- Kafka advertised listener configuration for private EC2 access
- Spark reading from Kafka topic `raw_social_posts`
- Nginx reverse proxy in front of Streamlit for production deployment on EC2

## 10. Short One-Line Summary

This is an AWS-first YouTube sentiment platform where Streamlit can trigger a Lambda collector or run locally, the collector publishes raw YouTube comments to Kafka and optionally S3, Spark processes the stream, BERT classifies sentiment, Elasticsearch stores results, and Streamlit displays them.
