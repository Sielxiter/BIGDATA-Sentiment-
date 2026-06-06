# Project Context for AI Assistants

This document explains the current project state, the target AWS deployment, and the role of each active file in the repository.

## 1. Project Goal

Build a sentiment analysis platform for public Reddit data with this production workflow:

`AWS Lambda -> Kafka on EC2 -> Spark on EC2 -> Fine-tuned BERT on EC2 -> Elasticsearch on EC2 -> Streamlit on EC2`

The project no longer depends on the YouTube API. The collector now scrapes public Reddit posts and comments, normalizes them, publishes them to Kafka, and can optionally save a JSONL backup to S3.

The Streamlit dashboard can run the collector locally for testing or invoke Lambda in AWS mode depending on `COLLECTOR_MODE`.

## 2. Important Constraints

- Deploy the runtime on AWS.
- Use AWS Lambda as the serverless collector when `COLLECTOR_MODE=lambda`.
- Use EC2 for Kafka, Spark, the fine-tuned BERT runtime, Elasticsearch, and Streamlit.
- Do not hardcode AWS credentials in code.
- Lambda must be able to reach Kafka over the network.
- Lambda may optionally write a backup JSONL file to S3.
- The raw Kafka topic must remain `raw_social_posts`.
- Respectful scraping delays must remain enabled.
- Selenium or Playwright are not required for the default build because the current collector uses public Reddit JSON endpoints and `requests`.

## 3. Current Active Repository Tree

Only the files below are part of the current workflow.

### `cloud/`

- `cloud/reddit_scraper_collector.py`
  - Reddit collector that scrapes public posts and comments.
  - Normalizes documents into Kafka-ready JSON.
  - Supports local mode and Lambda mode.
  - Uses `requests` and Reddit public JSON endpoints with delays.

- `cloud/lambda_reddit_to_kafka.py`
  - AWS Lambda handler for the Reddit collector.
  - Collects Reddit comments, publishes them to Kafka, and optionally uploads a JSONL backup to S3.

- `cloud/kafka_client.py`
  - Reusable Kafka producer helper for Lambda and cloud-side code.
  - Reads Kafka connection settings from environment variables.
  - Handles bootstrap servers and optional SASL/SSL settings.

- `cloud/s3_backup.py`
  - Helper for writing collected records to S3 as JSONL.

- `cloud/collector_config.py`
  - Helper for saving and loading the active collector configuration from S3.

- `cloud/__init__.py`
  - Marks `cloud/` as a Python package.

### `dashboard/`

- `dashboard/app.py`
  - Streamlit dashboard for Reddit collection.
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
  - Accepts Reddit messages with `source="reddit"`.

- `spark_app/schemas.py`
  - Defines the expected schema for raw Kafka messages.
  - Includes Reddit fields such as `subreddit`, `post_id`, `comment_id`, and `collected_at`.

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
  - Includes Reddit metadata fields.

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
  - Holds Kafka, Elasticsearch, collector, backup, and model settings.

- `config/__init__.py`
  - Package marker.

### Root files

- `README.md`
  - Short summary of the project and the current Reddit workflow.

- `workflow.md`
  - Production workflow notes and local testing instructions.

- `requirements.txt`
  - Python dependencies for the active workflow.

- `.env.example`
  - Example environment variables for local development and deployment.

- `scripts/test_reddit_scraper_local.py`
  - Local test script for scraping Reddit without sending to Kafka.

## 4. What Was Removed From the Main Workflow

These were removed because they are not part of the current AWS EC2 + Lambda production flow:

- `collectors/`
- `kafka_app/`
- `notebooks/`
- `data/`
- `checkpoints/`
- `tools/`
- `docker-compose.yml`
- `context.md`
- `spark.md`
- `wsl.md`

Do not rely on those folders when explaining the current system.

## 5. AWS Deployment Interpretation

When helping with AWS setup, assume this target architecture:

1. `AWS Lambda`
   - Deployed as a serverless function.
   - Runs the Reddit collector in Lambda mode.
   - Publishes normalized comments directly to Kafka.
   - Optionally writes a backup JSONL file to S3.
   - Reads the latest saved collector parameters from S3 when invoked without an explicit payload, which allows EventBridge scheduled runs to reuse the settings chosen in Streamlit.

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
   - Can save the active Lambda collector settings to S3 so scheduled Lambda runs use the selected event, keywords, subreddits, max posts, and max comments per post.

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
- `REDDIT_SCRAPER_ENGINE`
- `REDDIT_MAX_POSTS`
- `REDDIT_MAX_COMMENTS_PER_POST`
- `REDDIT_REQUEST_DELAY_SECONDS`
- `REDDIT_TIMEOUT_SECONDS`
- `REDDIT_HEADLESS`

## 7. How the Streamlit Dashboard Works

- The user enters:
  - event name
  - keywords
  - optional subreddits
  - max posts
  - max comments per post
  - sort mode
  - time filter
  - scraper engine label
- The dashboard now includes:
  - a top-level event filter for the live Elasticsearch chart
  - a Lambda settings save flow for event/keyword/subreddit/post-limit changes
- If `COLLECTOR_MODE=local`, the dashboard runs the local Reddit collector directly.
- If `COLLECTOR_MODE=lambda`, the dashboard saves the chosen collector settings to S3 and can also trigger the Lambda collector with `boto3` for an immediate run.
- The dashboard displays:
  - status
  - session id
  - posts scanned
  - comments collected
  - Kafka topic
  - optional S3 backup key
  - any warnings or errors
- The live chart reads sentiment counts from Elasticsearch.

## 8. Local Testing Flow

To test scraping without sending data to Kafka, use:

```bash
python scripts/test_reddit_scraper_local.py --event "World Cup" --keywords "morocco,football" --subreddits "soccer,worldcup" --max-posts 20 --max-comments 50 --send-to-kafka false
```

This prints:

- number of posts scanned
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
- Spark reading from Kafka topic `raw_social_posts`
- Nginx reverse proxy in front of Streamlit for production deployment on EC2

## 10. Short One-Line Summary

This is an AWS-first Reddit sentiment platform where Streamlit can trigger a Lambda collector or run locally, the collector publishes raw Reddit comments to Kafka and optionally S3, Spark processes the stream, BERT classifies sentiment, Elasticsearch stores results, and Streamlit displays them.
