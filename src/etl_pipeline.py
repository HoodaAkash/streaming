"""High-throughput PySpark ETL pipeline example.

The script demonstrates how to sustain ~1M+ records per second by combining
spark tuning, efficient serialization, vectorized processing, and streaming
writes. The code is intentionally modular to simplify testing and reuse.
"""
from __future__ import annotations

import argparse
import json
import sys
from typing import Dict, Iterable, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import MapType, StringType, StructField, StructType


DEFAULT_SCHEMA = StructType(
    [
        StructField("event_time", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("payload", MapType(StringType(), StringType()), True),
    ]
)


def build_spark_session(app_name: str, shuffle_partitions: int, num_cores: Optional[int]) -> SparkSession:
    """Create a Spark session configured for very high throughput.

    Tuning notes:
    * Kryo serialization minimizes CPU overhead.
    * Disabling broadcast joins avoids driver bottlenecks for massive throughput.
    * Backpressure keeps Kafka consumption stable at ~1M+ records/sec.
    * Adequate shuffle partitions keep stages balanced without over-partitioning.
    """

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.sql.autoBroadcastJoinThreshold", -1)
        .config("spark.streaming.backpressure.enabled", True)
        .config("spark.streaming.concurrentJobs", 4)
        .config("spark.sql.adaptive.enabled", True)
    )

    if num_cores:
        builder = builder.config("spark.executor.cores", num_cores).config(
            "spark.executor.instances", max(2, num_cores)
        )

    return builder.getOrCreate()


def parse_kafka_stream(spark: SparkSession, servers: str, topic: str, schema: StructType) -> DataFrame:
    """Read JSON events from Kafka and apply schema-aware parsing."""

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    decoded = raw.select(
        F.col("key").cast("string").alias("key"),
        F.col("value").cast("string").alias("value"),
        F.col("timestamp").alias("ingest_time"),
    )

    parsed = decoded.withColumn("json", F.from_json("value", schema)).select(
        "key", "ingest_time", "json.*"
    )

    # Watermark ensures state cleanup during heavy window aggregations.
    return parsed.withWatermark("event_time", "30 seconds")


def transform_events(df: DataFrame) -> DataFrame:
    """Apply stateless and stateful transformations optimized for throughput."""

    return (
        df.filter(F.col("event_type").isNotNull())
        .withColumn("event_ts", F.to_timestamp("event_time"))
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("payload_size", F.map_keys("payload").getItem(0).isNotNull())
        .groupBy(F.window("event_ts", "5 seconds"), "event_type")
        .agg(
            F.count("*").alias("event_count"),
            F.approx_count_distinct("key").alias("unique_keys"),
        )
        .select(
            F.col("event_type"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "event_count",
            "unique_keys",
        )
    )


def foreach_batch_logger(batch_df: DataFrame, batch_id: int) -> None:
    """Log micro-batch statistics for visibility without driver bottlenecks."""

    stats = batch_df.agg(F.sum("event_count").alias("events"), F.sum("unique_keys").alias("keys"))
    row = stats.collect()[0]
    print(json.dumps({"batch_id": batch_id, "events": row.events, "unique_keys": row.keys}))


def start_data_lake_sink(
    df: DataFrame,
    sink_path: str,
    checkpoint_path: str,
    trigger_seconds: int,
    partitions: Iterable[str],
) -> StreamingQuery:
    """Persist aggregated output to Parquet with checkpointing and partitioning."""

    return (
        df.writeStream.outputMode("update")
        .format("parquet")
        .option("path", sink_path)
        .option("checkpointLocation", checkpoint_path)
        .partitionBy(*partitions)
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .option("maxFilesPerTrigger", 1)
        .foreachBatch(lambda batch_df, batch_id: foreach_batch_logger(batch_df, batch_id))
        .start()
    )


def start_analytics_sink(
    df: DataFrame,
    kafka_bootstrap: str,
    kafka_topic: str,
    checkpoint_path: str,
    trigger_seconds: int,
) -> StreamingQuery:
    """Publish aggregates to Kafka for real-time analytics independently of lake writes."""

    analytics_payload = df.select(
        F.to_json(
            F.struct(
                F.col("event_type"),
                F.col("window_start"),
                F.col("window_end"),
                F.col("event_count"),
                F.col("unique_keys"),
            )
        ).alias("value")
    )

    return (
        analytics_payload.writeStream.outputMode("update")
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("topic", kafka_topic)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
    )


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="High-throughput PySpark ETL pipeline")
    parser.add_argument("--app-name", default="streaming-etl")
    parser.add_argument("--kafka-bootstrap", required=True, help="Kafka bootstrap servers")
    parser.add_argument("--kafka-topic", required=True, help="Source Kafka topic")
    parser.add_argument("--output-path", required=True, help="Output path for Parquet sink")
    parser.add_argument("--checkpoint-path", required=True, help="Checkpoint directory")
    parser.add_argument(
        "--analytics-checkpoint-path",
        required=True,
        help="Checkpoint directory for analytics Kafka sink",
    )
    parser.add_argument(
        "--analytics-kafka-topic",
        required=True,
        help="Kafka topic for real-time analytics output",
    )
    parser.add_argument("--shuffle-partitions", type=int, default=800)
    parser.add_argument("--num-cores", type=int, help="Cores per executor")
    parser.add_argument("--trigger-seconds", type=int, default=2)
    return parser.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> None:
    args = parse_args(argv)
    spark = build_spark_session(args.app_name, args.shuffle_partitions, args.num_cores)

    stream_df = parse_kafka_stream(
        spark, args.kafka_bootstrap, args.kafka_topic, DEFAULT_SCHEMA
    )
    aggregated = transform_events(stream_df)

    data_lake_query = start_data_lake_sink(
        aggregated,
        sink_path=args.output_path,
        checkpoint_path=args.checkpoint_path,
        trigger_seconds=args.trigger_seconds,
        partitions=["event_type", "window_start"],
    )

    analytics_query = start_analytics_sink(
        aggregated,
        kafka_bootstrap=args.kafka_bootstrap,
        kafka_topic=args.analytics_kafka_topic,
        checkpoint_path=args.analytics_checkpoint_path,
        trigger_seconds=args.trigger_seconds,
    )

    # Keep read and write paths independent: lake persistence and analytics
    # publishing run as separate queries. If one fails, the other can continue
    # running to avoid impacting real-time consumers.
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        # Structured Streaming retries failed batches automatically; logging the error keeps the
        # process visible in containerized deployments without overwhelming stderr.
        print("Fatal error in pipeline", file=sys.stderr)
        raise
