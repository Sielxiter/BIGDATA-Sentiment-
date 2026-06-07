# sentiment-bigdata-platform

Big Data sentiment analysis pipeline for public YouTube comments.

## Workflow

`AWS Lambda -> Kafka on EC2 (+ optional S3 backup) -> Spark on EC2 -> Fine-tuned BERT on EC2 -> Elasticsearch on EC2 -> Streamlit on EC2`

## What changed

The active collector now uses the YouTube Data API. The dashboard can run the collector locally for testing or invoke Lambda in AWS mode, and Lambda can reuse the latest saved collector settings from S3.

## Main files

- `cloud/youtube_api_collector.py`
- `cloud/lambda_youtube_to_kafka.py`
- `cloud/kafka_client.py`
- `cloud/s3_backup.py`
- `cloud/collector_config.py`
- `spark_app/streaming_job.py`
- `nlp/trained_bert_model.py`
- `storage/elastic_client.py`
- `dashboard/app.py`
- `deploy/nginx.conf`
- `docker-compose.yml`

## Docker stack

Start the infrastructure stack with:

```bash
docker compose up -d
```

This brings up:

- Kafka
- Zookeeper
- Kafka UI
- Elasticsearch

Then run Spark and Streamlit separately from your Python environment if you want to keep them outside Docker.

## Local YouTube API test

1. Install dependencies from `requirements.txt`.
2. Create a `.env` file from `.env.example`.
3. Set `YOUTUBE_API_KEY` in `.env`.
4. Run the local test script:

```bash
python scripts/test_youtube_api_collector_local.py --event "World Cup" --search-query "morocco football" --channel-ids "" --video-ids "" --max-videos 20 --max-comments 50 --output-jsonl data/youtube_test.jsonl
```

Expected output:

- number of videos scanned
- number of comments collected
- 3 sample documents
- optional JSONL file on disk

To send the same data to Kafka, pass:

```bash
--send-to-kafka true
```

Example:

```bash
python scripts/test_youtube_api_collector_local.py --event "World Cup" --search-query "morocco football" --max-videos 20 --max-comments 50 --send-to-kafka true
```

## Lambda deployment

Deploy `cloud/lambda_youtube_to_kafka.py` as the Lambda handler.

The Lambda expects the same payload fields used by the dashboard:

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

The dashboard can save the active Lambda collector settings to S3 so scheduled EventBridge runs can reuse the latest query/video/channel limits.

## Limitations

- The collector depends on the YouTube Data API and a valid API key with remaining quota.
- Respectful delays are enabled to reduce burst traffic.
- If the API quota is exhausted, Lambda will fail until quota is restored or the request volume is reduced.

## Local Kafka note

When Kafka is running from Docker on the same machine, host-side clients like Spark and Streamlit should use:

```env
KAFKA_BOOTSTRAP_SERVERS=localhost:29092
```

If a client still receives `kafka:9092`, recreate the Docker stack so the broker advertises the external listener correctly.

## Spark and Elasticsearch

- Spark continues reading from `raw_social_posts`.
- Spark accepts YouTube messages with `source="youtube"`.
- Elasticsearch stores processed sentiment documents and powers the live Streamlit chart.
