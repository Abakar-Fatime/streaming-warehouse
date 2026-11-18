from src import dq
from src.schema import EVENT_SCHEMA


def _row(**kw):
    base = dict(event_id="e1", event_type="view", event_ts="2026-11-01T12:00:00.000Z",
                user_id="u_1", streamer_id="s_001", room_id="r_001", country="SG",
                device="ios", gift_id=None, gift_value_coins=None, watch_seconds=30)
    base.update(kw)
    return base


def _reasons(spark, rows):
    df = spark.createDataFrame([tuple(r[f.name] for f in EVENT_SCHEMA) for r in rows], EVENT_SCHEMA)
    return {r["event_id"]: r["dq_reason"] for r in dq.apply_rules(df).collect()}


def test_each_rule_fires(spark):
    rows = [
        _row(event_id="ok"),
        _row(event_id="no_user", user_id=None),
        _row(event_id="empty_user", user_id=""),
        _row(event_id="no_streamer", streamer_id=None),
        _row(event_id="bad_type", event_type="teleport"),
        _row(event_id="bad_ts", event_ts="not-a-timestamp"),
        _row(event_id="future", event_ts="2999-01-01T00:00:00.000Z"),
        _row(event_id="neg_gift", event_type="gift_sent", gift_id="g_rose", gift_value_coins=-5),
        _row(event_id="null_gift", event_type="gift_sent", gift_id="g_rose", gift_value_coins=None),
        _row(event_id="good_gift", event_type="gift_sent", gift_id="g_rose", gift_value_coins=1),
    ]
    got = _reasons(spark, rows)
    assert got["ok"] is None and got["good_gift"] is None
    assert got["no_user"] == got["empty_user"] == "missing_user_id"
    assert got["no_streamer"] == "missing_streamer_id"
    assert got["bad_type"] == "bad_event_type"
    assert got["bad_ts"] == "unparsable_timestamp"
    assert got["future"] == "future_timestamp"
    assert got["neg_gift"] == got["null_gift"] == "negative_gift_value"
