# sentiment-bigdata-platform

Big Data / Cloud sentiment analysis platform for social media during major events.

## Architecture

YouTube API / Other APIs
        ↓
Kafka
        ↓
Spark Structured Streaming
        ↓
BERT Sentiment Analysis Layer
        ↓
Elasticsearch
        ↓
Streamlit Dashboard

## Step 1

This step creates the initial project structure and local Docker infrastructure only.

Included in Step 1:
- Kafka
- Kafka UI
- Elasticsearch
- Kibana
- Kafka runs in KRaft mode, so ZooKeeper is not needed
- environment template
- placeholder project folders
- sample data

Not included yet:
- YouTube collector
- Spark streaming job
- BERT model
- Streamlit dashboard logic

## Start Docker Services

```bash
docker compose up -d
docker compose ps
docker compose logs -f
```

## Stop Docker Services

```bash
docker compose down
```

## Check Kafka UI

Open:

http://localhost:8080

## Check Elasticsearch

Open:

http://localhost:9200

## Check Kibana

Open:

http://localhost:5601

## Environment

Copy `.env.example` to `.env` and adjust values if needed.

## Next Step

Step 2 will add the Kafka producer foundation.

## Step 2 - Kafka Producer Test

### Start infrastructure

```bash
docker compose up -d
```

### Install dependencies

```bash
pip install -r requirements.txt
```

### Copy environment file

Windows CMD:

```cmd
copy .env.example .env
```

PowerShell:

```powershell
Copy-Item .env.example .env
```

### Run producer

```bash
python kafka_app/send_sample_posts.py
```

### Run debug consumer in another terminal

```bash
python kafka_app/consumer_debug.py
```

### Kafka UI

Open:

http://localhost:8080

Then check:

Topics -> `raw_social_posts` -> Messages

### Verification options

Option 1:
- Open Kafka UI at http://localhost:8080
- Inspect `raw_social_posts`

Option 2:
- Run `python kafka_app/consumer_debug.py`
- Confirm printed messages match the sample CSV rows

## Step 3 - YouTube Collector

We switched Step 3 from Reddit to YouTube because Reddit API registration is blocked for now.

Current source flow:

YouTube Data API -> YouTube Collector -> normalized JSON comments -> Kafka topic `raw_social_posts`

### Create a YouTube API key

1. Open Google Cloud Console.
2. Create or select a project.
3. Enable the YouTube Data API v3.
4. Create an API key.
5. Put the key in `.env`.

Required `.env` values:

```env
YOUTUBE_API_KEY=your_key_here
DEFAULT_EVENT_NAME=Morocco football
DEFAULT_YOUTUBE_KEYWORDS=Morocco football,Morocco match,CAN Morocco
DEFAULT_YOUTUBE_MAX_VIDEOS=5
DEFAULT_YOUTUBE_MAX_COMMENTS=50
```

Reddit variables can stay in `.env.example` for later, but they are optional and not used in the current pipeline.

### Run the collector

```powershell
copy .env.example .env
```

Edit `.env` and add your YouTube API key.

```powershell
docker compose up -d
python kafka_app/consumer_debug.py
python collectors/run_youtube_collector.py --event "Morocco football" --keywords "Morocco football,Morocco match,CAN Morocco" --max-videos 5 --max-comments 50
```

You can also run it with defaults:

```powershell
python collectors/run_youtube_collector.py
```

### Verify in Kafka UI

Open:

http://localhost:8080

Then check:

Topics -> `raw_social_posts` -> Messages

### What the collector sends

- YouTube comments
- Normalized JSON with stable IDs and metadata
- One message per comment, ready for Spark later

If `YOUTUBE_API_KEY` is missing or still a placeholder, the script prints a clear message and exits without sending fake data.

## Step 4 - Spark Streaming Consumer

This step reads from Kafka topic `raw_social_posts`, parses the JSON from YouTube comments, validates required fields, cleans the text, and prints the processed rows to the console.

### Start infrastructure

```bash
docker compose up -d
```

### Terminal 1: run Spark streaming job

Windows CMD:

```cmd
spark-submit ^
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 ^
  spark_app/streaming_job.py
```

PowerShell:

```powershell
spark-submit `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 `
  spark_app/streaming_job.py
```

One-line command:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 spark_app/streaming_job.py
```

### Terminal 2: send test messages

```bash
python kafka_app/send_sample_posts.py
```

Or send real YouTube comments:

```powershell
python collectors/run_youtube_collector.py --event "Morocco football" --keywords "Morocco football,Morocco match,CAN Morocco" --max-videos 5 --max-comments 50
```

### Troubleshooting

- If Spark cannot find the Kafka source, check the `--packages` argument.
- If Kafka connection fails, check running containers with `docker compose ps`.
- If no data appears, send new messages after starting the Spark job.
- If the Windows command with `^` fails, use the one-line command.
- If your Spark version is different, adjust the Kafka connector package version to match your Spark/PySpark version.
- The Kafka connector version should match the installed PySpark/Spark version.
- On Windows, Spark may also require `HADOOP_HOME` and `winutils.exe`; without them, Spark can fail before the stream starts.

## Step 5 - BERT Sentiment Model Layer

The sentiment layer is replaceable.

For now, the pipeline uses a pretrained Hugging Face multilingual/social-media model:

- `cardiffnlp/twitter-xlm-roberta-base-sentiment`

Later, we will fine-tune our own BERT model and replace the implementation without changing the rest of the project.

Stable output format:

```json
{
  "label": "positive",
  "score": 0.94,
  "model_name": "cardiffnlp/twitter-xlm-roberta-base-sentiment",
  "model_version": "v1"
}
```

### Test the model alone

```bash
python nlp/test_sentiment_model.py
```

### Run Spark streaming with BERT sentiment

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 spark_app/streaming_job.py
```

### Send sample messages

```bash
python kafka_app/send_sample_posts.py
```

### Or send YouTube comments

```powershell
python collectors/run_youtube_collector.py --event "Morocco football" --keywords "Morocco football,Morocco match,CAN Morocco" --max-videos 5 --max-comments 50
```

The first run may take time because Hugging Face downloads the model.

### Troubleshooting

- If model download is slow, wait for Hugging Face to finish downloading.
- If `torch` is missing, run `pip install -r requirements.txt`.
- If memory is low, reduce the number of comments.
- If Spark is slow, that is expected for the BERT UDF in version 1.
- If labels are weird, check the label mapping in `nlp/bert_sentiment_model.py`.
- If Spark cannot find Kafka source, verify the `--packages` argument.

