# Production Workflow

Target flow:

`AWS Lambda -> Kafka on EC2 (+ optional S3 backup) -> Spark on EC2 -> Fine-tuned BERT on EC2 -> Elasticsearch on EC2 -> Streamlit on EC2`

## 1. Collector

Deploy `cloud/lambda_reddit_to_kafka.py` as the Lambda handler.

Environment variables:

- `COLLECTOR_SOURCE=reddit`
- `COLLECTOR_MODE=lambda`
- `COLLECTOR_LAMBDA_FUNCTION_NAME=reddit-collector-to-kafka`
- `COLLECTOR_LAMBDA_REGION=eu-north-1`
- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_RAW_TOPIC`
- `S3_BUCKET_NAME`
- `S3_PREFIX`
- `ENABLE_S3_BACKUP`
- `REDDIT_SCRAPER_ENGINE`
- `REDDIT_REQUEST_DELAY_SECONDS`
- `REDDIT_TIMEOUT_SECONDS`
- `REDDIT_HEADLESS`

The collector scrapes public Reddit posts and comments with respectful delays.

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

Spark accepts Reddit messages where `source="reddit"`.

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

Set `COLLECTOR_MODE=local` for local scraping during testing, or `COLLECTOR_MODE=lambda` to invoke AWS Lambda.

## Local Reddit scraping test

Run:

```bash
python scripts/test_reddit_scraper_local.py --event "World Cup" --keywords "morocco,football" --subreddits "soccer,worldcup" --max-posts 20 --max-comments 50 --output-jsonl data/reddit_test.jsonl
```

To send the same data to Kafka, add:

```bash
--send-to-kafka true
```

Expected output:

- posts scanned
- comments collected
- 3 sample documents
- optional JSONL file

## Lambda limitation

Selenium and Playwright are intentionally not required in the default build because they add browser runtime overhead in Lambda. The current collector uses `requests` and Reddit's public JSON endpoints, which keeps the code path lighter and easier to deploy. If Reddit changes its structure, a browser-based fallback can be added later.
