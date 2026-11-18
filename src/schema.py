"""Shared event schema and reference data for the LIVE-events pipeline."""
from pyspark.sql.types import (
    IntegerType, LongType, StringType, StructField, StructType,
)

EVENT_TYPES = ["view", "like", "comment", "gift_sent", "follow"]

# Raw events arrive as JSON. event_ts stays a string at the edge so that
# malformed timestamps can be quarantined instead of silently becoming null.
EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_ts", StringType()),
    StructField("user_id", StringType()),
    StructField("streamer_id", StringType()),
    StructField("room_id", StringType()),
    StructField("country", StringType()),
    StructField("device", StringType()),
    StructField("gift_id", StringType()),
    StructField("gift_value_coins", LongType()),
    StructField("watch_seconds", IntegerType()),
])

# Static gift catalogue (becomes dim_gift).
GIFT_CATALOGUE = [
    ("g_rose", "Rose", 1),
    ("g_heart", "Heart", 5),
    ("g_panda", "Panda", 25),
    ("g_rocket", "Rocket", 250),
    ("g_lion", "Lion", 1000),
]

COUNTRIES = ["SG", "MY", "ID", "TH", "VN", "PH"]
DEVICES = ["ios", "android", "web"]
