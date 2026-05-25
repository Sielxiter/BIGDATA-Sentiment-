"""Spark Structured Streaming job for raw social posts."""

from __future__ import annotations

import sys
from pathlib import Path

from pyspark.sql import SparkSession


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import settings  # noqa: E402
from spark_app.transformations import (  # noqa: E402
    add_sentiment_predictions,
    clean_posts,
    parse_kafka_messages,
    validate_posts,
)


def build_spark_session() -> SparkSession:
    """Create and configure the Spark session."""

    return (
        SparkSession.builder.appName(settings.spark_app_name)
        .master(settings.spark_master)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def main() -> None:
    checkpoint_dir = (PROJECT_ROOT / settings.spark_checkpoint_dir).resolve()
    checkpoint_dir.mkdir(parents=True, exist_ok=True)

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    kafka_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", settings.kafka_raw_topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    cleaned_df = clean_posts(validate_posts(parse_kafka_messages(kafka_stream)))
    processed_df = add_sentiment_predictions(cleaned_df)

    query = (
        processed_df.writeStream.format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("numRows", "50")
        .option("checkpointLocation", str(checkpoint_dir))
        .start()
    )

    print(
        f"[streaming_job] Reading topic={settings.kafka_raw_topic} from "
        f"{settings.kafka_bootstrap_servers}"
    )
    print(f"[streaming_job] Checkpoint location: {checkpoint_dir}")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("[streaming_job] Stopping streaming query")
    finally:
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
