# Production Workflow

Target flow:

`AWS Lambda -> Kafka on EC2 (+ optional S3 backup) -> Spark on EC2 -> Fine-tuned BERT on EC2 -> Elasticsearch on EC2 -> Streamlit on EC2`

## 1. Collector

Deploy `cloud/lambda_youtube_to_kafka.py` as the Lambda handler.

Environment variables:

- `COLLECTOR_SOURCE=youtube`
- `COLLECTOR_MODE=lambda`
- `COLLECTOR_LAMBDA_FUNCTION_NAME=youtube-collector-to-kafka`
- `COLLECTOR_LAMBDA_REGION=eu-north-1`
- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_RAW_TOPIC`
- `S3_BUCKET_NAME`
- `S3_PREFIX`
- `ENABLE_S3_BACKUP`
- `YOUTUBE_API_KEY`
- `YOUTUBE_REQUEST_DELAY_SECONDS`
- `YOUTUBE_API_TIMEOUT_SECONDS`
- `YOUTUBE_USER_AGENT`
- `YOUTUBE_PUBLISHED_AFTER`
- `YOUTUBE_PUBLISHED_BEFORE`

The collector uses the YouTube Data API and a valid API key with quota. Respectful pacing is enabled to reduce burst traffic.

## 2. Kafka on EC2

- Keep the raw topic name as `raw_social_posts`.
- Make sure the broker is reachable from Lambda and from local testing.
- If you use Docker on EC2, `docker-compose.yml` starts Kafka, Zookeeper, Kafka UI, and Elasticsearch together.
- Set `KAFKA_ADVERTISED_HOST_NAME` to the EC2 host name or public IP when Kafka must accept external clients.

## 3. Spark on EC2

Run:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 spark_app/streaming_job.py
```

Spark accepts YouTube messages where `source="youtube"`.

## 4. Fine-tuned BERT on EC2

- Use `nlp/trained_bert_model.py` and `nlp/sentiment_service.py`.
- Verify `MODEL_PROVIDER`, `MODEL_NAME`, `MODEL_VERSION`, and `MODEL_DEVICE`.

## 5. Elasticsearch on EC2

- Deploy Elasticsearch on EC2.
- Verify `ELASTICSEARCH_URL` and `ELASTICSEARCH_INDEX`.

## 6. Streamlit on EC2

Run:

```bash
streamlit run dashboard/app.py
```

Set `COLLECTOR_MODE=local` for local collection during testing, or `COLLECTOR_MODE=lambda` to invoke AWS Lambda.

## Local YouTube API test

Run:

```bash
python scripts/test_youtube_api_collector_local.py --event "World Cup" --search-query "morocco football" --max-videos 20 --max-comments 50 --output-jsonl data/youtube_test.jsonl
```

To send the same data to Kafka, add:

```bash
--send-to-kafka true
```

Expected output:

- videos scanned
- comments collected
- 3 sample documents
- optional JSONL file

## Lambda limitation

The current collector uses the YouTube Data API and does not require browser automation. If API quota becomes a problem, reduce request volume or widen the schedule interval instead of trying to scrape around quota.
