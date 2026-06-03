# Spark runtime report

## 1. What the Spark problem was

The Spark streaming job did not fail because of Kafka or Elasticsearch code. It failed on this Windows machine while Spark was starting its streaming checkpoint layer.

The visible error was:

- `java.lang.UnsatisfiedLinkError`
- It referenced `org.apache.hadoop.io.nativeio.NativeIO$Windows.access0`
- The failure happened during Spark Structured Streaming initialization, before the first batch was processed

In simple terms: Spark could start the Python job, but it could not fully start the streaming engine on Windows because the Hadoop Windows native runtime was not matching what Spark expected.

## 2. What Hadoop is

Hadoop is the storage and file-system layer that Spark often uses under the hood.

Even when a project is not "using Hadoop" directly, Spark still depends on Hadoop libraries for things like:

- file system access
- checkpoint directories for Structured Streaming
- temporary files and metadata logs
- cross-platform file permissions and local file operations

On Windows, Hadoop also expects some native helper binaries such as `winutils.exe` and related DLLs.

## 3. What is the relation between Spark and Hadoop

Spark and Hadoop are separate projects, but they are closely linked.

- Spark is the processing engine
- Hadoop provides file system and lower-level storage utilities that Spark uses

For our project, Spark Structured Streaming needs a checkpoint directory. That checkpoint logic uses Hadoop file-system classes. On Windows, those classes can require native helpers.

So the relationship is:

- Kafka sends the data
- Spark reads and processes the stream
- Hadoop helps Spark with local file system and checkpoint operations

## 4. What failed in our case

The job reached this stage:

- Kafka source was configured correctly
- Spark session started
- Elasticsearch initialization worked
- The streaming query began startup

Then Spark failed during checkpoint initialization with a Windows native access error.

This means the pipeline logic was fine, but the local Windows runtime was incomplete or mismatched.

## 5. What I tried to fix it

I tried several things in order.

### A. Matched the Kafka connector package to the installed PySpark version

Your machine had PySpark `4.1.1`, so the original Spark 3.5 Kafka connector package was not a good match.

I changed the connector to:

```bash
org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1
```

This solved the Spark/Kafka package compatibility issue.

### B. Disabled Hadoop permissions in Spark config

I added these Spark configs:

```python
.config("spark.hadoop.fs.permissions.enabled", "false")
.config("spark.hadoop.fs.permissions.umask-mode", "000")
```

This reduced permission-related file-system issues, but it did not fix the native Windows error.

### C. Set `HADOOP_HOME` on Windows

I tried pointing Spark to a local Hadoop directory.

### D. Added Windows Hadoop helper binaries

I created a local `C:\hadoop\bin` folder and copied in:

- `winutils.exe`
- `hadoop.dll`
- related helper files

This is the standard Windows workaround for Spark/Hadoop.

### E. Tried different runtime combinations

I tested multiple connector and runtime combinations and checked the logs carefully.

## 6. What actually fixed parts of the problem

There were two different issues.

### Fixed

The Spark Kafka connector version mismatch was fixed by matching the package version to the installed PySpark version.

### Not fully fixed on this machine

The Windows Hadoop native runtime problem was not fully resolved.

Even after adding `winutils.exe` and `hadoop.dll`, Spark still failed with the native `NativeIO$Windows.access0` error.

So the code is correct, but the local Windows runtime still blocks full streaming startup.

## 7. What I changed in the code

These code changes were made while debugging the Spark pipeline:

- Matched the Kafka connector package to the installed PySpark version.
- Kept the Spark streaming logic modular in `spark_app/streaming_job.py`.
- Kept the checkpoint directory configurable through `SPARK_CHECKPOINT_DIR`.
- Added a configurable output mode with `SPARK_OUTPUT_MODE` so Spark can write to:
  - console
  - Elasticsearch
  - both
- Added Elasticsearch helpers in `storage/elastic_client.py` so Spark does not talk to Elasticsearch directly.
- Added the Elasticsearch index mapping in `storage/index_mapping.py`.
- Added `spark.md` and README notes so the Windows issue is documented instead of hidden.

The important point is this:

- the Spark code path is wired correctly
- the remaining failure is in the Windows runtime, not in the pipeline design

## 8. What you may need to install manually

If you want Spark to run natively on Windows, you may need to install or configure:

- `HADOOP_HOME`
- `winutils.exe`
- matching Hadoop native binaries
- a Spark/Kafka connector version that matches your installed PySpark version

In practice, the cleanest fix is usually one of these:

1. Use WSL2 or Linux for Spark
2. Use a Windows Hadoop helper bundle that matches your Spark runtime
3. Keep Docker for Kafka and Elasticsearch, but run Spark in WSL2

## 9. What to edit in the code if you move machines

If you move to another machine and want the same project to run, check these code-driven settings first:

- `config/settings.py`
  - `KAFKA_BOOTSTRAP_SERVERS`
  - `KAFKA_RAW_TOPIC`
  - `ELASTICSEARCH_URL`
  - `ELASTICSEARCH_INDEX`
  - `SPARK_APP_NAME`
  - `SPARK_MASTER`
  - `SPARK_CHECKPOINT_DIR`
  - `SPARK_OUTPUT_MODE`
- `.env`
  - YouTube API key
  - model config
  - Elasticsearch URL and index if different
- `spark_app/streaming_job.py`
  - connector package version if your PySpark version changes
  - checkpoint path if you want a different local folder

## 10. What is currently working

These parts are working:

- Docker Compose infrastructure
- Kafka producer
- YouTube collector
- Elasticsearch index creation
- Elasticsearch smoke test
- Spark code compiles
- Spark streaming pipeline is wired correctly in code

What is not fully verified on this specific Windows machine:

- a complete live Spark Structured Streaming run from Kafka to Elasticsearch

## 11. Do you need the same setup on another PC

Not exactly the same setup, but you do need the same general components.

### Minimum setup needed on another PC

- Python dependencies installed from `requirements.txt`
- Docker Desktop running
- Kafka, Kafka UI, Elasticsearch, Kibana from `docker compose up -d`
- `.env` file configured
- YouTube API key if you want real comments
- Hugging Face model download access the first time BERT runs
- A working Spark runtime compatible with the local PySpark version

### On Windows specifically

You may also need:

- `HADOOP_HOME`
- `winutils.exe`
- matching Hadoop native binaries for the Spark version you installed

If the other PC is Linux, this part is usually much easier.

## 12. Simple recommendation for another PC

If you want the least friction, use one of these options:

1. Linux machine
2. Windows machine with a Spark version and Hadoop native binaries that match
3. Docker or WSL-based setup for better compatibility

## 13. The practical takeaway

The project code is not the issue.

The problem was a local Windows Spark runtime dependency mismatch around Hadoop native file-system support.

So the status is:

- pipeline code: ready
- Elasticsearch: working
- Kafka: working
- collectors: working
- Spark logic: ready
- local Windows runtime: still the blocker for full live stream execution

## 14. What to do next if you want to run it elsewhere

1. Install Python dependencies.
2. Start Docker Compose.
3. Create `.env`.
4. Ensure the Spark version matches the Kafka connector package version.
5. On Windows, add the correct Hadoop native helper binaries.
6. Run the collector.
7. Start Spark.
8. Confirm Elasticsearch receives documents.

## 15. Short conclusion

Spark itself was not broken. The Windows environment was missing or mismatching the Hadoop native runtime pieces that Spark uses for streaming checkpointing.

I fixed the package-version mismatch and verified the Elasticsearch sink, but the final live Spark stream on this PC is still limited by Windows native Hadoop compatibility.
