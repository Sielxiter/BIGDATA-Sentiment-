# Lambda Deployment Context

This file is a deployment-only handoff for an AI assistant. It assumes the assistant already has the full project context from `PROJECT_CONTEXT.md`.

## 1. Goal

Deploy the YouTube collector as AWS Lambda so it can:

- read the latest saved collector config from S3
- call the YouTube Data API
- publish normalized comments to Kafka on EC2
- optionally write a JSONL backup to S3

Lambda is only the collector runtime. Spark, Elasticsearch, Streamlit, and the BERT inference stack stay on EC2.

## 2. Active Lambda Entry Point

Use this Lambda handler:

```text
cloud.lambda_youtube_to_kafka.lambda_handler
```

Main runtime modules:

- `cloud/lambda_youtube_to_kafka.py`
  - Lambda entrypoint
  - loads S3 config
  - builds runtime parameters
  - calls the collector
  - returns Lambda-shaped JSON

- `cloud/youtube_api_collector.py`
  - active collector implementation
  - searches YouTube videos
  - fetches comments
  - normalizes documents
  - optionally publishes to Kafka / S3

## 3. Files That Must Go Into the Lambda Package

Include these project files in the Lambda zip or container image:

- `cloud/lambda_youtube_to_kafka.py`
- `cloud/youtube_api_collector.py`
- `cloud/kafka_client.py`
- `cloud/s3_backup.py`
- `cloud/collector_config.py`
- `cloud/collector_state.py`
- `config/settings.py`
- `cloud/__init__.py`
- `config/__init__.py`

Include the Python packages required by the Lambda code:

- `requests`
- `confluent-kafka`
- `python-dotenv` if you use it in the runtime package
- `langdetect` is optional

Do **not** include the whole repository.

## 4. Files That Must Stay Out of Lambda

Do not package these into the Lambda artifact:

- `dashboard/`
- `spark_app/`
- `nlp/`
- `data/`
- `checkpoints/`
- docs (`README.md`, `run.md`, `workflow.md`, `PROJECT_CONTEXT.md`)
- local test outputs

## 5. What Lambda Needs at Runtime

Environment variables:

- `COLLECTOR_SOURCE=youtube`
- `COLLECTOR_MODE=lambda`
- `COLLECTOR_LAMBDA_FUNCTION_NAME=youtube-collector-to-kafka`
- `COLLECTOR_LAMBDA_REGION=...`
- `YOUTUBE_API_KEY`
- `YOUTUBE_API_BASE_URL=https://www.googleapis.com/youtube/v3`
- `YOUTUBE_REQUEST_DELAY_SECONDS`
- `YOUTUBE_API_TIMEOUT_SECONDS`
- `YOUTUBE_USER_AGENT`
- `YOUTUBE_PUBLISHED_AFTER` (optional)
- `YOUTUBE_PUBLISHED_BEFORE` (optional)
- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_RAW_TOPIC=raw_social_posts`
- `KAFKA_SECURITY_PROTOCOL` if needed
- `KAFKA_SASL_MECHANISM` if needed
- `KAFKA_SASL_USERNAME` if needed
- `KAFKA_SASL_PASSWORD` if needed
- `KAFKA_SSL_CA_LOCATION` if needed
- `AWS_REGION`
- `S3_BUCKET_NAME` if backup is enabled
- `S3_PREFIX`
- `ENABLE_S3_BACKUP`
- `COLLECTOR_CONFIG_BUCKET_NAME`
- `COLLECTOR_CONFIG_S3_KEY`
- `COLLECTOR_STATE_BUCKET_NAME`
- `COLLECTOR_STATE_S3_KEY`
- `COLLECTOR_STATE_MAX_COMMENT_IDS`

## 6. Lambda Networking Requirements

Lambda must be able to reach **two external targets**:

1. **Kafka on EC2**
   - Lambda should be in the same VPC as Kafka or have a route to it.
   - Kafka security group must allow inbound broker traffic from Lambda.
   - `KAFKA_BOOTSTRAP_SERVERS` must point to a real EC2 private address or internal DNS name.
   - Kafka must not advertise `localhost`.

2. **YouTube Data API**
   - Lambda needs outbound internet access.
   - If Lambda is in private subnets, add a NAT Gateway.

## 7. Collector Behavior

The Lambda handler does this:

1. loads saved config from S3
2. merges S3 config with any explicit payload
3. loads dedup state from S3 for the current event
4. runs the YouTube collector while skipping already-ingested comment IDs
5. sends new documents to Kafka
6. updates the dedup state in S3 only after a successful Kafka publish
7. optionally backs up JSONL records to S3
8. returns `statusCode` + JSON body

The collector uses the YouTube Data API, not Reddit.

## 8. Payload Fields Supported by Lambda

The Lambda payload can include:

- `event_name`
- `search_query`
- `video_ids`
- `channel_ids`
- `max_videos`
- `max_comments_per_video`
- `order`
- `region_code`
- `relevance_language`
- `published_after`
- `published_before`
- `engine`

If the payload is empty, Lambda should fall back to the config stored in S3.

## 9. What the Assistant Should Change When Packaging

If asked to prepare Lambda deployment:

- keep the handler set to `cloud.lambda_youtube_to_kafka.lambda_handler`
- make sure the Lambda package contains the `cloud/` and `config/` packages plus dependencies
- keep `KAFKA_BOOTSTRAP_SERVERS` pointed to the EC2 broker address
- verify `YOUTUBE_API_KEY` exists in Lambda environment variables or Secrets Manager injection
- verify S3 config permissions:
  - `s3:GetObject`
  - `s3:PutObject`
- verify Kafka publish permissions and broker reachability

## 10. Suggested Test Flow

1. Test locally without Kafka:

```bash
python scripts/test_youtube_api_collector_local.py --event "World Cup" --search-query "morocco football" --max-videos 5 --max-comments 1 --send-to-kafka false
```

2. Test locally with Kafka if the broker is reachable.

3. Deploy Lambda with the handler above.

4. Invoke Lambda with a small payload.

5. Confirm messages appear in Kafka topic `raw_social_posts`.

## 11. Short Summary for an AI Assistant

The Lambda part of this repo is a YouTube API collector that publishes normalized comments to Kafka on EC2. Package `cloud/lambda_youtube_to_kafka.py` as the handler, include the collector and support modules, set the Kafka bootstrap server to a reachable EC2 address, and give Lambda outbound internet access for YouTube API calls.
