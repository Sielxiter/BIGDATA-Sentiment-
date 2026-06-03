# Ubuntu WSL Setup

Use this setup if you want Spark to run more reliably on Windows.

Recommended split:

- Windows: Docker Desktop for Kafka, Kafka UI, Elasticsearch, Kibana
- Ubuntu WSL2: Python, Spark, collectors, and `spark-submit`

This avoids most Windows Hadoop / `winutils.exe` issues.

## 1. Install WSL2 and Ubuntu

Run this in **PowerShell as Administrator**:

```powershell
wsl --install -d Ubuntu
```

Restart if Windows asks for it.

## 2. Open Ubuntu and install dependencies

Run inside **Ubuntu WSL**:

```bash
sudo apt update
sudo apt install -y python3 python3-pip python3-venv openjdk-21-jdk
```

Check Java:

```bash
java -version
```

## 3. Go to the project folder

Inside **Ubuntu WSL**:

```bash
cd /mnt/c/Users/My-PC/Desktop/BIGDATA\ Sentiment
```

If your Windows user name or path is different, adjust it.

## 4. Create a Python virtual environment

Inside **Ubuntu WSL**:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

## 5. Install Python dependencies

Inside **Ubuntu WSL**:

```bash
pip install -r requirements.txt
```

## 6. Create the `.env` file

Inside **Ubuntu WSL**:

```bash
cp .env.example .env
nano .env
```

Fill in:

- `YOUTUBE_API_KEY`
- any other values you want to change

The important defaults already exist in `.env.example`.

## 7. Start Docker infrastructure

Run this in **Windows PowerShell** or in a terminal where Docker Desktop is available:

```powershell
docker compose up -d
docker compose ps
```

This starts:

- Kafka
- Kafka UI
- Elasticsearch
- Kibana

## 8. Test Elasticsearch

Inside **Ubuntu WSL**:

```bash
python storage/init_elasticsearch.py
python storage/test_elasticsearch.py
```

This checks:

- Elasticsearch connection
- index creation
- indexing and search

## 9. Test the sentiment model alone

Inside **Ubuntu WSL**:

```bash
python nlp/test_sentiment_model.py
```

This checks that the pretrained Hugging Face model loads and predicts correctly.

## 10. Run Spark streaming

Inside **Ubuntu WSL**:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 spark_app/streaming_job.py
```

What this does:

- reads from Kafka topic `raw_social_posts`
- parses and validates the JSON
- cleans the text
- predicts sentiment
- writes to Elasticsearch

## 11. Send sample messages

Open another **Ubuntu WSL** terminal, then run:

```bash
cd /mnt/c/Users/My-PC/Desktop/BIGDATA\ Sentiment
source .venv/bin/activate
python kafka_app/send_sample_posts.py
```

This sends sample JSON messages to Kafka.

## 12. Send real YouTube comments

Inside **Ubuntu WSL**:

```bash
python collectors/run_youtube_collector.py --event "Morocco football" --keywords "Morocco football,Morocco match,CAN Morocco" --max-videos 5 --max-comments 50
```

This collects YouTube comments and sends them to Kafka.

## 13. Verify output

Check Kafka UI:

http://localhost:8080

Check Elasticsearch:

http://localhost:9200/sentiment_results/_search?pretty

Check Kibana:

http://localhost:5601

## 14. Full workflow order

Use this order:

1. `docker compose up -d`
2. `python storage/init_elasticsearch.py`
3. `python storage/test_elasticsearch.py`
4. `python nlp/test_sentiment_model.py`
5. `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 spark_app/streaming_job.py`
6. `python kafka_app/send_sample_posts.py`
7. `python collectors/run_youtube_collector.py --event "Morocco football" --keywords "Morocco football,Morocco match,CAN Morocco" --max-videos 5 --max-comments 50`

## 15. Important notes

- Keep Docker on Windows.
- Run Spark in Ubuntu WSL.
- Do not use `spark-submit` from plain Windows PATH if it is missing.
- The Kafka connector version must match the Spark version.
- For this project on WSL, use Spark 4.1.1 with:

```bash
org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1
```

## 16. If something fails

- If Docker is down, run `docker compose ps`.
- If Elasticsearch is not ready, run `python storage/init_elasticsearch.py`.
- If Spark cannot connect to Kafka, make sure Docker is running and Kafka is healthy.
- If a YouTube run fails, check `YOUTUBE_API_KEY` in `.env`.
- If Spark fails on Windows, use WSL instead of native Windows Spark.
