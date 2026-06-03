# sentiment-bigdata-platform

Sentiment analysis pipeline for social media events.

## Main Workflow

`AWS Lambda -> S3 -> Kafka -> Spark Structured Streaming -> BERT -> Elasticsearch -> Streamlit`

## Quick Start

1. Start infra:
```bash
docker compose up -d
```

2. Create Kafka topic once:
```powershell
docker exec -it sentiment-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic raw_social_posts --partitions 1 --replication-factor 1
```

3. Install deps:
```bash
pip install -r requirements.txt
```

4. Configure env:
```bash
cp .env.example .env
```

5. Run bridge:
```bash
python cloud/s3_to_kafka_bridge.py
```

6. Run Spark:
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 spark_app/streaming_job.py
```

7. Run dashboard:
```bash
streamlit run dashboard/app.py
```

## Setup Details

Full local + AWS setup is documented in:

- [workflow.md](/C:/Users/My-PC/Desktop/BIGDATA%20Sentiment/workflow.md)

## Project Notes

- Spark reads from Kafka topic `raw_social_posts`.
- Elasticsearch index is `sentiment_results`.
- Bridge state file: `cloud/.s3_bridge_state.json`.
- Trained model path (if enabled): `models/fine_tuned_bert/`.
