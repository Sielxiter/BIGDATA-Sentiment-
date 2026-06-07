# Local Run Guide

This guide shows how to run the full project locally.

## 1. Environment Setup

Create a `.env` file from `.env.example` and make sure the local values are correct:

```env
KAFKA_BOOTSTRAP_SERVERS=localhost:29092
ELASTICSEARCH_URL=http://localhost:9200
COLLECTOR_MODE=local
YOUTUBE_API_KEY=your_api_key_here
ENABLE_S3_BACKUP=false
```

If you want S3 backup locally, keep `ENABLE_S3_BACKUP=true` only if your AWS credentials and bucket are configured.
If Kafka still resolves to `kafka:9092`, stop and recreate the containers after confirming `KAFKA_ADVERTISED_HOST_NAME=localhost`.

## 2. Start Infrastructure

Start Kafka, Zookeeper, Kafka UI, and Elasticsearch:

```bash
docker compose up -d
```

Services started by Compose:

- Kafka
- Zookeeper
- Kafka UI
- Elasticsearch

Important port mapping:

- `localhost:29092` from host processes
- `kafka:9092` only from inside Docker containers

If you change Kafka listener or advertised host settings, recreate the stack so the broker picks up the new values:

```bash
docker compose down -v
docker compose up -d
```

## 3. Make Sure Kafka Topics and Elasticsearch Are Ready

The Spark job now creates the Kafka topics it needs automatically before it subscribes. If you want to verify Kafka manually first, you can create the topics yourself:

```bash
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic raw_social_posts --partitions 1 --replication-factor 1
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic processed_sentiments --partitions 1 --replication-factor 1
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

If your WSL environment does not have the Python `confluent-kafka` package installed, Spark will skip the Python-based topic auto-creation and keep running as long as the topics already exist.

Then create the Elasticsearch index before starting Spark:

```bash
python -m storage.init_elasticsearch
```

## 4. Start Spark

Run the streaming consumer in a separate terminal:

```bash
cd /mnt/c/Users/My-PC/Desktop/BIGDATA\ Sentiment
```

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 spark_app/streaming_job.py
```

Spark reads from Kafka topic `raw_social_posts` and writes processed documents to Elasticsearch.
If you want the graph to move in very small increments, set `SPARK_MAX_OFFSETS_PER_TRIGGER=1` in `.env` before starting Spark.

## 5. Start Streamlit

Run the dashboard in another terminal:

```bash
streamlit run dashboard/app.py
```

With `COLLECTOR_MODE=local`, the dashboard uses the local YouTube API collector instead of AWS Lambda.

## 6. Test Collection Only

To verify collection without Kafka:

```bash
python scripts/test_youtube_api_collector_local.py --event "World Cup" --search-query "morocco football" --max-videos 20 --max-comments 50 --send-to-kafka false --output-jsonl data/youtube_test.jsonl
```

This checks:

- YouTube API collection
- normalization
- sample output

No Kafka and no Elasticsearch are needed for this test.

## 7. Test Full Local Pipeline

To test the end-to-end local flow:

```bash
python scripts/test_youtube_api_collector_local.py --event "World Cup" --search-query "morocco football" --max-videos 20 --max-comments 50 --send-to-kafka true
```

Expected flow:

1. YouTube API collector collects comments
2. Kafka receives messages on `raw_social_posts`
3. Spark ensures the required Kafka topics exist
4. Spark consumes Kafka messages
5. Spark runs the BERT sentiment layer
6. Elasticsearch stores the processed results

## 8. Common Problems

- Missing `YOUTUBE_API_KEY` will stop collection.
- Kafka bootstrap host mismatch will break producer/consumer connections.
- Elasticsearch index not created yet will keep the dashboard empty.
- Forgetting to set `COLLECTOR_MODE=local` when testing the dashboard locally.
