"""Structured Streaming job: raw JSON events -> quarantine + fact + 1-minute aggregates.

  python -m src.stream_job --source files --input data/incoming --root warehouse
  python -m src.stream_job --source kafka --bootstrap localhost:9092 --topic live_events
"""
from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession, functions as F

from src import dq, warehouse
from src.schema import EVENT_SCHEMA

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"


def get_spark(source: str, master: str = "local[2]") -> SparkSession:
    b = (SparkSession.builder.master(master).appName("live-events-warehouse")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.enabled", "false"))
    if source == "kafka":
        b = b.config("spark.jars.packages", KAFKA_PACKAGE)
    return b.getOrCreate()


def read_raw(spark: SparkSession, a: argparse.Namespace) -> DataFrame:
    if a.source == "files":
        return spark.readStream.schema(EVENT_SCHEMA).json(a.input)
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", a.bootstrap)
           .option("subscribe", a.topic).option("startingOffsets", "earliest").load())
    return (raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
            .select("e.*"))


def run(a: argparse.Namespace):
    spark = get_spark(a.source)
    spark.sparkContext.setLogLevel("WARN")
    warehouse.build_dimensions(spark, a.root)

    clean, bad = dq.split(read_raw(spark, a))
    fact = warehouse.to_fact(clean, a.watermark)
    agg = warehouse.to_minute_agg(fact)

    trigger = {"availableNow": True} if a.trigger == "availableNow" else {"processingTime": "10 seconds"}

    q_bad = (bad.select("event_id", "event_type", "event_ts", "user_id", "streamer_id",
                        "gift_value_coins", "dq_reason")
             .writeStream.format("parquet").outputMode("append")
             .option("path", f"{a.root}/quarantine")
             .option("checkpointLocation", f"{a.checkpoints}/quarantine")
             .trigger(**trigger).start())
    q_fact = (fact.writeStream.format("parquet").outputMode("append")
              .partitionBy("event_date")
              .option("path", f"{a.root}/fact_live_events")
              .option("checkpointLocation", f"{a.checkpoints}/fact")
              .trigger(**trigger).start())
    q_agg = (agg.writeStream.format("parquet").outputMode("append")
             .option("path", f"{a.root}/agg_streamer_minute")
             .option("checkpointLocation", f"{a.checkpoints}/agg")
             .trigger(**trigger).start())
    queries = [q_bad, q_fact, q_agg]
    for q in queries:
        q.awaitTermination()
    return spark


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source", choices=["files", "kafka"], default="files")
    p.add_argument("--input", default="data/incoming")
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="live_events")
    p.add_argument("--root", default="warehouse")
    p.add_argument("--checkpoints", default="checkpoints")
    p.add_argument("--watermark", default="10 minutes")
    p.add_argument("--trigger", choices=["availableNow", "continuous"], default="availableNow")
    return p.parse_args(argv)


if __name__ == "__main__":
    run(parse_args())
