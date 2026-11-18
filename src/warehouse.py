"""Dimension tables and the streaming fact/aggregate transformations."""
from pyspark.sql import DataFrame, SparkSession, functions as F

from src.generator import streamer_ids
from src.schema import COUNTRIES, DEVICES, GIFT_CATALOGUE


def build_dimensions(spark: SparkSession, root: str) -> None:
    """Write the small, static dimensions (batch). Facts reference them by key."""
    spark.createDataFrame(GIFT_CATALOGUE, ["gift_id", "gift_name", "coin_value"]) \
        .write.mode("overwrite").parquet(f"{root}/dim_gift")
    spark.createDataFrame([(s, f"Streamer {s[2:]}") for s in streamer_ids()],
                          ["streamer_id", "streamer_name"]) \
        .write.mode("overwrite").parquet(f"{root}/dim_streamer")
    spark.createDataFrame([(c,) for c in COUNTRIES], ["country"]) \
        .write.mode("overwrite").parquet(f"{root}/dim_country")
    spark.createDataFrame([(d,) for d in DEVICES], ["device"]) \
        .write.mode("overwrite").parquet(f"{root}/dim_device")


def to_fact(clean: DataFrame, watermark: str) -> DataFrame:
    """Deduplicate (at-least-once delivery) and shape the fact table."""
    return (
        clean.withWatermark("event_time", watermark)
        .dropDuplicates(["event_id", "event_time"])
        .select(
            "event_id", "event_type", "event_time",
            F.to_date("event_time").alias("event_date"),
            "user_id", "streamer_id", "room_id", "country", "device",
            "gift_id", "gift_value_coins", "watch_seconds",
        )
    )


def to_minute_agg(fact: DataFrame) -> DataFrame:
    """One row per streamer per closed 1-minute window.

    `fact` already carries the watermark set in to_fact(); redefining it here
    is disallowed by Spark, so the aggregate simply inherits it.
    """
    return (
        fact
        .groupBy(F.window("event_time", "1 minute").alias("w"), "streamer_id")
        .agg(
            F.count(F.when(F.col("event_type") == "view", 1)).alias("views"),
            F.count(F.when(F.col("event_type") == "like", 1)).alias("likes"),
            F.count(F.when(F.col("event_type") == "comment", 1)).alias("comments"),
            F.sum(F.coalesce("gift_value_coins", F.lit(0))).alias("gift_coins"),
            F.approx_count_distinct("user_id").alias("active_users"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "streamer_id", "views", "likes", "comments", "gift_coins", "active_users",
        )
    )
