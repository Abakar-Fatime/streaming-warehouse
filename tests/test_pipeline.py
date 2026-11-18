"""End-to-end: generate events -> run the streaming job -> reconcile with the manifest."""
import json

import pytest
from pyspark.sql import functions as F

from src import generator, stream_job


@pytest.fixture(scope="module")
def run_dir(tmp_path_factory):
    d = tmp_path_factory.mktemp("pipeline")
    events, manifest = generator.generate(6000, seed=5)
    generator.write_files(events, str(d / "incoming"))
    (d / "manifest.json").write_text(json.dumps(manifest))
    args = stream_job.parse_args([
        "--source", "files", "--input", str(d / "incoming"),
        "--root", str(d / "wh"), "--checkpoints", str(d / "ck")])
    stream_job.run(args)
    return d, manifest


def test_fact_is_deduplicated_and_complete(spark, run_dir):
    d, m = run_dir
    fact = spark.read.parquet(str(d / "wh" / "fact_live_events"))
    assert fact.count() == m["expected_valid_unique"]
    assert fact.select("event_id").distinct().count() == fact.count()


def test_quarantine_matches_injected_defects(spark, run_dir):
    d, m = run_dir
    q = spark.read.parquet(str(d / "wh" / "quarantine"))
    got = {r["dq_reason"]: r["count"] for r in q.groupBy("dq_reason").count().collect()}
    assert got == {k: v for k, v in m["quarantined_by_reason"].items() if v}


def test_closed_windows_reconcile_with_fact(spark, run_dir):
    """Aggregates only exist for windows the watermark has closed; for those
    windows the numbers must equal a batch recomputation from the fact table."""
    d, _ = run_dir
    fact = spark.read.parquet(str(d / "wh" / "fact_live_events"))
    agg = spark.read.parquet(str(d / "wh" / "agg_streamer_minute"))
    assert agg.count() > 0
    cutoff = agg.agg(F.max("window_end")).first()[0]
    batch = (fact.filter(F.col("event_time") < F.lit(cutoff))
             .groupBy(F.window("event_time", "1 minute").alias("w"), "streamer_id")
             .agg(F.count(F.when(F.col("event_type") == "view", 1)).alias("b_views"),
                  F.sum(F.coalesce("gift_value_coins", F.lit(0))).alias("b_gift_coins"))
             .select(F.col("w.start").alias("window_start"), "streamer_id", "b_views", "b_gift_coins"))
    joined = agg.join(batch, ["window_start", "streamer_id"], "full_outer")
    # same set of (window, streamer) keys on both sides ...
    assert joined.filter(F.col("views").isNull() | F.col("b_views").isNull()).count() == 0
    # ... and identical measures
    assert joined.filter((F.col("views") != F.col("b_views"))
                         | (F.col("gift_coins") != F.col("b_gift_coins"))).count() == 0
