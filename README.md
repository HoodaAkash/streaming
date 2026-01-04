# Streaming ETL Pipeline

This repository contains a high-throughput PySpark ETL pipeline template designed to handle **1M+ records per second**. It ingests JSON events from Kafka, applies schema-aware transformations, and writes windowed aggregates to a Parquet data lake while simultaneously streaming the same aggregates back to Kafka for **independent, real-time analytics**.

## Pipeline capabilities
- Structured Streaming from Kafka with backpressure to stabilize throughput.
- Kryo serialization, adaptive query execution, and tuned shuffle partitions for efficient CPU and network usage.
- Watermark-based window aggregations to manage state growth at high event volumes.
- Parquet sink with partitioning, checkpointing, and per-batch metrics logging.
- Dedicated Kafka analytics sink that runs independently of the data lake writer so reads and writes do not block each other.

## Running the pipeline
```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  src/etl_pipeline.py \
  --kafka-bootstrap localhost:9092 \
  --kafka-topic events \
  --output-path s3a://data-lake/events_agg \
  --checkpoint-path s3a://data-lake/checkpoints/events_agg \
  --analytics-checkpoint-path s3a://data-lake/checkpoints/events_analytics \
  --analytics-kafka-topic events_agg_rt \
  --shuffle-partitions 800 \
  --num-cores 8 \
  --trigger-seconds 2
```

## Real-time dashboard
The analytics Kafka sink emits JSON aggregates that can be visualized without touching Spark. Install the lightweight Dash/Kafka client dependencies:

```bash
pip install dash plotly kafka-python
```

Then launch the dashboard to consume the analytics topic:

```bash
python src/analytics_dashboard.py \
  --kafka-bootstrap localhost:9092 \
  --analytics-topic events_agg_rt \
  --group-id analytics-dashboard \
  --port 8050
```

The app displays stacked bar charts of event counts per window and summary cards for the most recent metrics per event type. It consumes in a background thread so the UI remains responsive even while ingesting a high-volume stream.

### Throughput tuning tips
- **Scale executors**: allocate enough executors to keep the cluster CPU-bound rather than I/O-bound; start with 8+ cores per executor for 1M+/sec workloads.
- **Manage partitions**: tune `--shuffle-partitions` based on cluster size (e.g., 50–100 per executor) to balance skew and overhead.
- **Kafka ingestion**: ensure sufficient topic partitions to feed all executors; enable compression (lz4 or snappy) to reduce network pressure.
- **Storage layout**: keep Parquet block sizes large (e.g., 256–512 MB) and partition by high-cardinality dimensions such as event type and time windows to optimize downstream reads.
- **Monitoring**: watch structured streaming metrics and adjust `--trigger-seconds` to keep micro-batch durations below the trigger interval.

## Development
- The pipeline code lives in [`src/etl_pipeline.py`](src/etl_pipeline.py) and can be imported into notebooks for experimentation.
- Run a quick syntax check locally with `python -m compileall src`.
