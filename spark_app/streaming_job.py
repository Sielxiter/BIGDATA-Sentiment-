"""Spark Structured Streaming job for raw social posts."""

from __future__ import annotations

import sys
from pathlib import Path
from pprint import pformat

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
from storage.elastic_client import bulk_index_documents, create_index_if_not_exists  # noqa: E402


VALID_OUTPUT_MODES = {"console", "elasticsearch", "both"}


def build_spark_session() -> SparkSession:
    """Create and configure the Spark session."""

    return (
        SparkSession.builder.appName(settings.spark_app_name)
        .master(settings.spark_master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.permissions.enabled", "false")
        .config("spark.hadoop.fs.permissions.umask-mode", "000")
        .getOrCreate()
    )


def _resolve_output_mode() -> str:
    mode = (settings.spark_output_mode or "both").strip().lower()
    if mode not in VALID_OUTPUT_MODES:
        print(
            f"[streaming_job] Invalid SPARK_OUTPUT_MODE={mode!r}. "
            "Falling back to 'both'."
        )
        return "both"
    return mode


def main() -> None:
    checkpoint_dir = (PROJECT_ROOT / settings.spark_checkpoint_dir).resolve()
    checkpoint_dir.mkdir(parents=True, exist_ok=True)

    output_mode = _resolve_output_mode()
    use_console = output_mode in {"console", "both"}
    use_elasticsearch = output_mode in {"elasticsearch", "both"}

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    if use_elasticsearch:
        create_index_if_not_exists()

    kafka_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", settings.kafka_raw_topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    processed_df = add_sentiment_predictions(clean_posts(validate_posts(parse_kafka_messages(kafka_stream))))

    def write_batch(batch_df, batch_id: int) -> None:
        """Process a single micro-batch.

        Collecting a controlled micro-batch to the driver is acceptable for v1.
        For higher throughput, we can move to the Elasticsearch Spark connector
        or partition-based writes later.
        """

        rows = batch_df.collect()
        if not rows:
            print(f"[streaming_job] Batch {batch_id}: empty batch")
            return

        documents = [row.asDict(recursive=True) for row in rows]
        print(f"[streaming_job] Batch {batch_id}: received {len(documents)} records")

        if use_console:
            preview_count = min(len(documents), 5)
            print(f"[streaming_job] Batch {batch_id} preview ({preview_count} rows):")
            for document in documents[:preview_count]:
                print(pformat(document, sort_dicts=False))
            if len(documents) > preview_count:
                print(f"[streaming_job] Batch {batch_id}: preview truncated at {preview_count} rows")

        indexed_count = 0
        if use_elasticsearch:
            indexed_count = bulk_index_documents(documents)
            print(
                f"[streaming_job] Batch {batch_id}: indexed {indexed_count}/{len(documents)} records"
            )

        if use_console and not use_elasticsearch:
            print(f"[streaming_job] Batch {batch_id}: printed {len(documents)} records")

    query = (
        processed_df.writeStream.foreachBatch(write_batch)
        .outputMode("append")
        .option("checkpointLocation", str(checkpoint_dir))
        .start()
    )

    print(
        f"[streaming_job] Reading topic={settings.kafka_raw_topic} from "
        f"{settings.kafka_bootstrap_servers}"
    )
    print(f"[streaming_job] Checkpoint location: {checkpoint_dir}")
    print(f"[streaming_job] Output mode: {output_mode}")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("[streaming_job] Stopping streaming query")
    finally:
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
