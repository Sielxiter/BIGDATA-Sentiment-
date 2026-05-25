# Context — Big Data Sentiment Analysis Platform

## 1. Project Vision

We are building a real, medium-level Big Data / Cloud project, not a very basic academic demo.

The project is a **Sentiment Analysis Platform for Social Media During Major Events**.

During major events such as elections, sports competitions, crises, festivals, public debates, or football matches, people publish a large number of posts and comments on social media. The goal is to collect these posts, stream them in near real time, analyze their sentiment using a BERT-based NLP model, store the results, and display them in a professional Streamlit dashboard.

The project should be simple enough to finish progressively, but strong enough to look serious, well-architected, and worth presenting.

---

## 2. Main Objective

Build a complete streaming Big Data pipeline:

```text
Social Media API / Scraper
        ↓
Data Collector
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
```

The system should process posts related to a selected event and classify each post as:

```text
positive / negative / neutral
```

The dashboard should show sentiment statistics and live analysis.

---

## 3. Important Project Principle

The NLP model must be **replaceable**.

For now, we will use a pretrained multilingual BERT-like model from Hugging Face.

Later, after the UI and pipeline are finished, we will train or fine-tune our own BERT model and replace the pretrained model.

Therefore, the code must be designed so the model can be replaced without changing the whole project.

Bad approach:

```python
# Do not put model logic directly everywhere
model = pipeline("sentiment-analysis")
result = model(text)
```

Good approach:

```text
Input text
   ↓
SentimentService.predict(text)
   ↓
Standard output format
```

The rest of the application should not care whether the model is pretrained or fine-tuned.

---

## 4. Technology Choices

### Main Language

Use **Python**.

Reasons:

- Strongest ecosystem for NLP and Hugging Face.
- Easy integration with BERT models.
- Streamlit is Python-based.
- PySpark allows Big Data processing with Python.
- Kafka producers and collectors are easy to build in Python.
- Fine-tuning BERT later will be easier in Python.

Java is strong for backend systems, but for this project Python is better because the main components are NLP, Spark, streaming, and data visualization.

---

## 5. FastAPI Decision

For version 1, we will **not use FastAPI**.

Current architecture:

```text
Spark → Elasticsearch → Streamlit
```

Streamlit will read from Elasticsearch directly.

FastAPI can be added later if we need:

- A REST API.
- A React or mobile frontend.
- Authentication.
- Endpoints such as `/stats`, `/events`, `/sentiments`, `/predict`.
- A separate model inference service.

For now, avoiding FastAPI keeps the first version cleaner and lets us focus on the main Big Data pipeline.

---

## 6. Data Source Strategy

We want real data, not only a fake CSV.

Preferred sources:

1. **Reddit API** — recommended first source.
2. **YouTube Data API** — second possible source.
3. X/Twitter API — possible but harder because access and pricing can be restrictive.
4. CSV fallback — allowed only for testing or simulation.

### Recommended First Source

Use **Reddit API** first because:

- It has public discussions.
- It is easier than X/Twitter.
- It works well for event-based analysis.
- It gives posts and comments that can be analyzed.

Example event keywords:

```text
Morocco football
Morocco vs Spain
World Cup
CAN Morocco
Elections
AI news
Olympics
Earthquake
```

---

## 7. Target Architecture

Final version 1 architecture:

```text
Reddit API
   ↓
Python Collector
   ↓
Kafka topic: raw_social_posts
   ↓
PySpark Structured Streaming
   ↓
Text cleaning + BERT sentiment layer
   ↓
Elasticsearch index: sentiment_results
   ↓
Streamlit Dashboard
```

Optional future architecture:

```text
Reddit / YouTube / Other APIs
   ↓
Kafka
   ↓
Spark
   ↓
Model Inference Service / FastAPI
   ↓
Elasticsearch
   ↓
Streamlit or React Dashboard
```

---

## 8. Kafka Topics

Use clear Kafka topic names:

```text
raw_social_posts
processed_sentiments
```

### raw_social_posts

Contains raw collected posts before processing.

### processed_sentiments

Optional topic for processed results. In version 1, Spark can write directly to Elasticsearch. Later, processed messages can also be published to this topic.

---

## 9. Raw Data Message Format

Each collected post should follow this format:

```json
{
  "id": "unique-post-id",
  "event": "Morocco vs Spain",
  "text": "Morocco played very well today!",
  "source": "reddit",
  "language": "en",
  "created_at": "2026-05-24T15:00:00Z",
  "metadata": {
    "author": "anonymous",
    "url": "optional",
    "likes": 12,
    "comments_count": 4
  }
}
```

Required fields:

```text
id
event
text
source
created_at
```

Optional fields:

```text
language
metadata
```

---

## 10. Processed Data Format

After Spark cleaning and sentiment prediction, the result should be stored in Elasticsearch as:

```json
{
  "id": "unique-post-id",
  "event": "Morocco vs Spain",
  "source": "reddit",
  "original_text": "Morocco played very well today!",
  "cleaned_text": "morocco played very well today",
  "language": "en",
  "sentiment": "positive",
  "confidence": 0.94,
  "model_name": "pretrained-bert",
  "model_version": "v1",
  "created_at": "2026-05-24T15:00:00Z",
  "processed_at": "2026-05-24T15:00:05Z",
  "metadata": {
    "author": "anonymous",
    "url": "optional",
    "likes": 12,
    "comments_count": 4
  }
}
```

The dashboard and Elasticsearch queries must rely on this stable format.

This is important because later, when we replace the pretrained model with our own trained BERT model, the output format must stay the same.

---

## 11. Sentiment Model Design

The NLP part must be modular.

Recommended files:

```text
nlp/
├── base_model.py
├── bert_sentiment_model.py
├── trained_bert_model.py
└── sentiment_service.py
```

### base_model.py

Contains the common interface:

```python
class BaseSentimentModel:
    def predict(self, text: str) -> dict:
        raise NotImplementedError

    def predict_batch(self, texts: list[str]) -> list[dict]:
        return [self.predict(text) for text in texts]
```

### bert_sentiment_model.py

Contains the current pretrained Hugging Face model implementation.

Example model options:

```text
cardiffnlp/twitter-xlm-roberta-base-sentiment
nlptown/bert-base-multilingual-uncased-sentiment
```

Preferred first model:

```text
cardiffnlp/twitter-xlm-roberta-base-sentiment
```

because it is more adapted to social media / Twitter-like text and multilingual sentiment.

### trained_bert_model.py

Reserved for the future fine-tuned model.

This file should later load:

```text
models/fine_tuned_bert/
```

### sentiment_service.py

Central access point used by the rest of the application.

Expected output:

```json
{
  "label": "positive",
  "score": 0.94,
  "model_name": "pretrained-bert",
  "model_version": "v1"
}
```

---

## 12. Spark Responsibilities

Spark Structured Streaming should:

1. Read messages from Kafka topic `raw_social_posts`.
2. Parse JSON.
3. Validate required fields.
4. Clean text.
5. Apply sentiment prediction through the sentiment service/model layer.
6. Add processing timestamp.
7. Write results to Elasticsearch.

Spark should not contain messy model-specific code. It should call a clean prediction layer.

Important performance note:

BERT inference inside Spark can be heavy. For version 1, this is acceptable if the volume is controlled. Later, we can improve performance by:

- batching predictions,
- using a separate inference service,
- using GPU,
- using model quantization,
- using Spark Pandas UDFs,
- or sending processed text to a model API.

---

## 13. Text Cleaning Rules

Create reusable cleaning logic.

Cleaning should include:

- Lowercasing when appropriate.
- Removing URLs.
- Removing extra spaces.
- Removing unnecessary special characters.
- Keeping meaningful words.
- Keeping emojis optional, because emojis can contain sentiment.
- Avoiding over-cleaning Arabic, French, or multilingual text.

Text cleaning should be implemented in a separate file:

```text
spark_app/transformations.py
```

or

```text
nlp/preprocessing.py
```

---

## 14. Elasticsearch

Use Elasticsearch as the main storage for analyzed results.

Index name:

```text
sentiment_results
```

Important fields:

```text
id
event
source
original_text
cleaned_text
language
sentiment
confidence
model_name
model_version
created_at
processed_at
metadata
```

The index should support dashboard queries by:

- event,
- sentiment,
- source,
- language,
- created_at,
- processed_at,
- confidence,
- model version.

---

## 15. Streamlit Dashboard

The dashboard should look professional and useful, not just basic charts.

Recommended pages:

```text
1. Overview
2. Live Feed
3. Event Analysis
4. Sentiment Timeline
5. Source Comparison
6. Model Monitoring
```

### Overview Page

Show:

- Total posts processed.
- Number of positive posts.
- Number of negative posts.
- Number of neutral posts.
- Positive percentage.
- Negative percentage.
- Neutral percentage.
- Average confidence.

### Live Feed Page

Show recent analyzed posts:

- text,
- event,
- sentiment,
- confidence,
- source,
- created time.

### Event Analysis Page

Allow filtering by event.

Show:

- sentiment distribution for one event,
- number of posts per event,
- top recent posts for that event.

### Sentiment Timeline Page

Show sentiment evolution over time.

Examples:

- positive posts per minute/hour,
- negative posts per minute/hour,
- neutral posts per minute/hour.

### Source Comparison Page

Compare data sources:

- Reddit,
- YouTube later,
- CSV/test data.

### Model Monitoring Page

Show:

- current model name,
- model version,
- number of predictions,
- average confidence,
- low-confidence predictions,
- confidence distribution.

This page is important because later we can compare:

```text
Pretrained BERT vs Fine-tuned BERT
```

---

## 16. Suggested Project Structure

Use this project structure:

```text
sentiment-bigdata-platform/
│
├── collectors/
│   ├── __init__.py
│   ├── base_collector.py
│   ├── reddit_collector.py
│   └── youtube_collector.py
│
├── kafka_app/
│   ├── __init__.py
│   ├── producer.py
│   └── topics.py
│
├── spark_app/
│   ├── __init__.py
│   ├── streaming_job.py
│   ├── schemas.py
│   └── transformations.py
│
├── nlp/
│   ├── __init__.py
│   ├── base_model.py
│   ├── bert_sentiment_model.py
│   ├── trained_bert_model.py
│   ├── sentiment_service.py
│   └── preprocessing.py
│
├── storage/
│   ├── __init__.py
│   ├── elastic_client.py
│   └── index_mapping.py
│
├── dashboard/
│   ├── __init__.py
│   ├── app.py
│   ├── charts.py
│   └── queries.py
│
├── config/
│   ├── __init__.py
│   └── settings.py
│
├── data/
│   └── sample_events.csv
│
├── models/
│   └── fine_tuned_bert/
│
├── notebooks/
│   └── training_experiments.ipynb
│
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── README.md
└── context.md
```

---

## 17. Version Roadmap

### Version 1 — Strong Working Pipeline

Goal: complete working system.

Tasks:

1. Create project structure.
2. Create Docker Compose for Kafka, Kafka UI, Elasticsearch, and Kibana if needed.
3. Create Kafka topic `raw_social_posts`.
4. Build Reddit collector.
5. Send Reddit data to Kafka.
6. Build Spark streaming job.
7. Add preprocessing and sentiment prediction.
8. Store results in Elasticsearch.
9. Build Streamlit dashboard.
10. Add filters and charts.

### Version 2 — Better Data Sources

Add:

- YouTube comments collector.
- Multiple events.
- Better language detection.
- Better deduplication.

### Version 3 — Model Training

Add:

- Dataset preparation.
- Label cleaning.
- Fine-tuning BERT.
- Model evaluation.
- Save trained model.
- Replace pretrained model with trained model.
- Compare model versions in dashboard.

### Version 4 — Advanced Architecture

Optional:

- Add FastAPI.
- Add model inference service.
- Add authentication.
- Add Dockerized Streamlit.
- Deploy on AWS or GCP.

---

## 18. Development Rules

Follow these rules when generating code:

1. Build progressively.
2. Do not overcomplicate the first version.
3. Keep code modular.
4. Make all paths and settings configurable.
5. Use `.env` for secrets and credentials.
6. Never hardcode API keys.
7. Add comments where logic is not obvious.
8. Use clean function names.
9. Use typed functions where possible.
10. Keep the model replaceable.
11. Keep Elasticsearch schema stable.
12. Keep Streamlit separate from model inference.
13. Make every component testable separately.
14. Prefer real API data, but keep CSV fallback for testing.
15. Write commands in README for each step.

---

## 19. Environment Variables

Expected `.env.example`:

```env
# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_RAW_TOPIC=raw_social_posts
KAFKA_PROCESSED_TOPIC=processed_sentiments

# Elasticsearch
ELASTICSEARCH_URL=http://localhost:9200
ELASTICSEARCH_INDEX=sentiment_results

# Reddit API
REDDIT_CLIENT_ID=your_client_id
REDDIT_CLIENT_SECRET=your_client_secret
REDDIT_USER_AGENT=sentiment-bigdata-platform/1.0

# Model
MODEL_PROVIDER=huggingface
MODEL_NAME=cardiffnlp/twitter-xlm-roberta-base-sentiment
MODEL_VERSION=v1

# Dashboard
STREAMLIT_REFRESH_SECONDS=5
```

---

## 20. Local Development Commands

Expected commands later:

```bash
# Start infrastructure
docker compose up -d

# Install dependencies
pip install -r requirements.txt

# Run collector
python collectors/reddit_collector.py

# Run Spark job
spark-submit spark_app/streaming_job.py

# Run dashboard
streamlit run dashboard/app.py
```

---

## 21. What We Should Build First

The first real implementation step should be:

```text
Step 1: Create project structure and Docker Compose.
```

Docker services should include:

```text
Kafka
Kafka UI
Elasticsearch
Kibana optional
```

Then test that:

```text
Kafka is running
Kafka UI opens
Elasticsearch responds
```

After that, continue with Kafka producer and Reddit collector.

---

## 22. Final Decisions

Use:

```text
Language: Python
Dashboard: Streamlit
Streaming: Kafka
Processing: PySpark
Storage: Elasticsearch
NLP: pretrained BERT now, fine-tuned BERT later
Data source: Reddit API first, YouTube API later
FastAPI: not in version 1
Deployment: local first, cloud later
```

The project must stay simple enough to implement progressively, but structured enough to be serious and extensible.
