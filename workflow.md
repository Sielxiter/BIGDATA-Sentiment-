# Main Workflow

This project now uses one main workflow:

`AWS Lambda -> S3 -> Kafka -> Spark -> BERT -> Elasticsearch -> Streamlit`

## 1. One-time Setup

### Local

1. Start infrastructure:

```bash
docker compose up -d
```

2. Create Kafka topic (first run only):

```powershell
docker exec -it sentiment-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic raw_social_posts --partitions 1 --replication-factor 1
```
rm -rf checkpoints/spark

3. Install Python dependencies:

```bash
pip install -r requirements.txt
```

4. Copy env file and fill values:

```bash
cp .env.example .env
```

Required in `.env`:
- `YOUTUBE_API_KEY`
- `S3_BUCKET_NAME`
- `AWS_REGION`

### AWS

1. Create an S3 bucket for raw comments.
2. Create IAM role for Lambda with:
- `AWSLambdaBasicExecutionRole`
- `s3:PutObject` on your bucket path
3. Deploy `cloud/lambda_youtube_to_s3_example.py` as your Lambda code.
4. Set Lambda environment variables:
- `YOUTUBE_API_KEY`
- `S3_BUCKET_NAME`
- `S3_PREFIX` (example: `raw/youtube`)
- `ENABLE_S3_BACKUP=true`
- `AWS_REGION`
5. Add an EventBridge schedule to run Lambda periodically (for example every 1 minute).

## 2. Run Pipeline

Open separate terminals.

1. Start S3 -> Kafka bridge:

```bash
python cloud/s3_to_kafka_bridge.py
```

2. Start Spark streaming:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 spark_app/streaming_job.py
```

3. Start dashboard:

```bash
streamlit run dashboard/app.py
```

## 3. Verify

1. Kafka UI: [http://localhost:8080](http://localhost:8080)
2. Elasticsearch: [http://localhost:9200/sentiment_results/_search?pretty](http://localhost:9200/sentiment_results/_search?pretty)
3. Streamlit: [http://localhost:8501](http://localhost:8501)

## 4. Notes

- If Spark fails with `UnknownTopicOrPartitionException`, create `raw_social_posts` topic manually.
- If the bridge sends nothing, check S3 bucket/prefix and Lambda logs.
- The bridge keeps state in `cloud/.s3_bridge_state.json` to avoid re-sending already processed S3 files.
