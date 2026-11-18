"""Data-quality rules. Each record gets at most one reason (first match wins);
records with a reason go to the quarantine table instead of the warehouse."""
from pyspark.sql import DataFrame, functions as F

from src.schema import EVENT_TYPES

FUTURE_TOLERANCE_MINUTES = 5


def apply_rules(df: DataFrame) -> DataFrame:
    """Add `event_time` (parsed timestamp) and `dq_reason` (null = clean)."""
    parsed = df.withColumn("event_time", F.to_timestamp("event_ts"))
    reason = (
        F.when(F.col("event_time").isNull(), "unparsable_timestamp")
        .when(F.col("event_time") > F.expr(
            f"current_timestamp() + INTERVAL {FUTURE_TOLERANCE_MINUTES} MINUTES"),
            "future_timestamp")
        .when(F.col("user_id").isNull() | (F.col("user_id") == ""), "missing_user_id")
        .when(F.col("streamer_id").isNull(), "missing_streamer_id")
        .when(~F.col("event_type").isin(EVENT_TYPES), "bad_event_type")
        .when((F.col("event_type") == "gift_sent")
              & (F.col("gift_value_coins").isNull() | (F.col("gift_value_coins") <= 0)),
              "negative_gift_value")
    )
    return parsed.withColumn("dq_reason", reason)


def split(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return (clean, quarantined)."""
    flagged = apply_rules(df)
    return flagged.filter(F.col("dq_reason").isNull()), flagged.filter(F.col("dq_reason").isNotNull())
