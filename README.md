# sentiment-bigdata-platform

Big Data sentiment analysis pipeline for public Reddit data.

## Workflow

`AWS Lambda -> Kafka on EC2 (+ optional S3 backup) -> Spark on EC2 -> Fine-tuned BERT on EC2 -> Elasticsearch on EC2 -> Streamlit on EC2`

## What changed

The project no longer depends on the YouTube API. The collector now scrapes public Reddit posts and comments using `requests` plus Reddit's public JSON endpoints, which makes the demo usable without Reddit API credentials.

## Main files

- `cloud/reddit_scraper_collector.py`
- `cloud/lambda_reddit_to_kafka.py`
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

## Local Reddit scraping test

1. Install dependencies from `requirements.txt`.
2. Create a `.env` file from `.env.example`.
3. Run the local test script:

```bash
python scripts/test_reddit_scraper_local.py --event "World Cup" --keywords "morocco,football" --subreddits "soccer,worldcup" --max-posts 20 --max-comments 50 --output-jsonl data/reddit_test.jsonl
```

Expected output:

- number of posts scanned
- number of comments collected
- 3 sample documents
- optional JSONL file on disk

To send the same data to Kafka, pass:

```bash
--send-to-kafka true
```

Example:

```bash
python scripts/test_reddit_scraper_local.py --event "World Cup" --keywords "morocco,football" --subreddits "soccer,worldcup" --max-posts 20 --max-comments 50 --send-to-kafka true
```

## Lambda deployment

Deploy `cloud/lambda_reddit_to_kafka.py` as the Lambda handler.

The Lambda expects the same payload fields used by the dashboard:

- `event_name`
- `keywords`
- `subreddits`
- `max_posts`
- `max_comments_per_post`
- `sort_mode`
- `time_filter`
- `engine`

The dashboard can save the active Lambda collector settings to S3 so scheduled EventBridge runs can reuse the latest event/keyword/subreddit limits.

## Limitations

- The current scraper uses public Reddit pages and JSON endpoints, so page structure changes can affect it.
- Respectful delays are enabled to reduce the chance of blocking.
- Selenium or Playwright can be added later if Reddit changes its HTML/JSON behavior, but they are intentionally not required for the current Lambda-friendly build.

## Local Kafka note

When Kafka is running from Docker on the same machine, host-side clients like Spark and Streamlit should use:

```env
KAFKA_BOOTSTRAP_SERVERS=localhost:29092
```

If a client still receives `kafka:9092`, recreate the Docker stack so the broker advertises the external listener correctly.

## Spark and Elasticsearch

- Spark continues reading from `raw_social_posts`.
- Spark accepts Reddit messages with `source="reddit"`.
- Elasticsearch stores processed sentiment documents and powers the live Streamlit chart.
