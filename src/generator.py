"""Simulated TikTok-LIVE-style event generator.

Produces JSON events with a controlled share of defects (missing ids, negative
gift values, future or unparsable timestamps, duplicate deliveries) so the
data-quality layer has something real to catch. A manifest of the exact
expected counts is written next to the files so tests can assert on them.

Sinks:
  files  - JSON-lines files in a directory (no Kafka needed, used for tests)
  kafka  - publishes to a Kafka topic (see docker-compose.yml)
"""
from __future__ import annotations

import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from src.schema import COUNTRIES, DEVICES, EVENT_TYPES, GIFT_CATALOGUE

N_STREAMERS = 20
N_USERS = 2000
TYPE_WEIGHTS = [0.55, 0.25, 0.12, 0.05, 0.03]  # view, like, comment, gift, follow


def streamer_ids() -> list[str]:
    return [f"s_{i:03d}" for i in range(N_STREAMERS)]


def _good_event(rng: random.Random, ts: datetime) -> dict:
    etype = rng.choices(EVENT_TYPES, weights=TYPE_WEIGHTS)[0]
    streamer = rng.choice(streamer_ids())
    ev = {
        "event_id": str(uuid.UUID(int=rng.getrandbits(128))),
        "event_type": etype,
        "event_ts": ts.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "user_id": f"u_{rng.randrange(N_USERS):05d}",
        "streamer_id": streamer,
        "room_id": f"r_{streamer[2:]}",
        "country": rng.choice(COUNTRIES),
        "device": rng.choice(DEVICES),
        "gift_id": None,
        "gift_value_coins": None,
        "watch_seconds": None,
    }
    if etype == "gift_sent":
        gid, _, coins = rng.choice(GIFT_CATALOGUE)
        ev["gift_id"], ev["gift_value_coins"] = gid, coins
    if etype == "view":
        ev["watch_seconds"] = rng.randrange(5, 600)
    return ev


DEFECTS = ["missing_user_id", "negative_gift_value", "future_timestamp",
           "unparsable_timestamp", "bad_event_type"]


def _break(rng: random.Random, ev: dict, defect: str) -> dict:
    ev = dict(ev)
    if defect == "missing_user_id":
        ev["user_id"] = None
    elif defect == "negative_gift_value":
        ev["event_type"] = "gift_sent"
        ev["gift_id"], ev["gift_value_coins"] = "g_rose", -abs(rng.randrange(1, 50))
    elif defect == "future_timestamp":
        ev["event_ts"] = (datetime.now(timezone.utc) + timedelta(days=1)).strftime(
            "%Y-%m-%dT%H:%M:%S.000Z")
    elif defect == "unparsable_timestamp":
        ev["event_ts"] = "not-a-timestamp"
    elif defect == "bad_event_type":
        ev["event_type"] = "teleport"
    return ev


def generate(n: int, seed: int = 7, bad_rate: float = 0.03,
             dup_rate: float = 0.02, start: datetime | None = None,
             span_minutes: int = 30) -> tuple[list[dict], dict]:
    """Return (events, manifest). Deterministic for a given seed and start."""
    rng = random.Random(seed)
    start = start or datetime(2026, 11, 1, 12, 0, 0, tzinfo=timezone.utc)
    events: list[dict] = []
    good_ids: set[str] = set()
    bad_by_reason = {d: 0 for d in DEFECTS}
    n_dups = 0

    for i in range(n):
        ts = start + timedelta(seconds=span_minutes * 60 * i / n)
        ev = _good_event(rng, ts)
        if rng.random() < bad_rate:
            defect = rng.choice(DEFECTS)
            events.append(_break(rng, ev, defect))
            bad_by_reason[defect] += 1
            continue
        events.append(ev)
        good_ids.add(ev["event_id"])
        if rng.random() < dup_rate:  # at-least-once delivery: same event twice
            events.append(dict(ev))
            n_dups += 1

    manifest = {
        "lines": len(events),
        "expected_valid_unique": len(good_ids),
        "expected_quarantined": sum(bad_by_reason.values()),
        "expected_duplicates_delivered": n_dups,
        "quarantined_by_reason": bad_by_reason,
    }
    return events, manifest


def write_files(events: list[dict], out_dir: str, files: int = 5) -> None:
    os.makedirs(out_dir, exist_ok=True)
    per = max(1, len(events) // files)
    for i in range(files):
        chunk = events[i * per:(i + 1) * per] if i < files - 1 else events[i * per:]
        with open(os.path.join(out_dir, f"events_{i:03d}.json"), "w") as f:
            for ev in chunk:
                f.write(json.dumps(ev) + "\n")


def publish_kafka(events: list[dict], bootstrap: str, topic: str, rate: float) -> None:
    from kafka import KafkaProducer  # imported lazily: only needed for this sink
    producer = KafkaProducer(bootstrap_servers=bootstrap,
                             value_serializer=lambda v: json.dumps(v).encode())
    for ev in events:
        producer.send(topic, ev)
        if rate > 0:
            time.sleep(1.0 / rate)
    producer.flush()


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--sink", choices=["files", "kafka"], default="files")
    p.add_argument("--events", type=int, default=20000)
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--out", default="data/incoming")
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="live_events")
    p.add_argument("--rate", type=float, default=0, help="events/sec for kafka (0 = as fast as possible)")
    a = p.parse_args()

    events, manifest = generate(a.events, seed=a.seed)
    if a.sink == "files":
        write_files(events, a.out)
        with open(os.path.join(a.out, "_manifest.json"), "w") as f:
            json.dump(manifest, f, indent=2)
    else:
        publish_kafka(events, a.bootstrap, a.topic, a.rate)
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
