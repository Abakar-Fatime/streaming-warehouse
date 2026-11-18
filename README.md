# Streaming Data Warehouse for Live-Event Analytics

A streaming pipeline that turns raw live-stream events (views, likes, comments, gifts, follows) into a
clean, deduplicated, partitioned warehouse with per-minute aggregates, and keeps bad records out of it
with explicit, testable data-quality rules.

Built with **Spark Structured Streaming** (PySpark), **Kafka** as the streaming source, and **Parquet**
as storage. The events are **synthetic**: the generator simulates a LIVE platform so the pipeline can be
run and tested without any private data.

## Architecture

```
 generator ──► Kafka topic / JSON files ──► Structured Streaming job
 (synthetic        (live_events)              │
  events with                                 ├─► data-quality rules ──► quarantine/   (rejected records + reason)
  injected                                    │
  defects)                                    └─► clean events
                                                    │  watermark 10 min + dropDuplicates(event_id)
                                                    ├─► fact_live_events/   (partitioned by event_date)
                                                    └─► agg_streamer_minute/ (1-minute tumbling windows)

 dim_streamer, dim_gift, dim_country, dim_device  (small static dimensions)
```

**Star schema.** `fact_live_events` is the fact table (one row per valid, unique event) and joins to
four dimensions by key. `agg_streamer_minute` is a pre-aggregated table for dashboards.

## Design decisions

| Decision | Why |
|---|---|
| Timestamps kept as strings at the edge, parsed in the quality step | A malformed timestamp is quarantined with a reason instead of silently becoming `null` |
| Quarantine table instead of dropping bad rows | Bad data stays inspectable and countable, which is how you notice an upstream bug |
| `dropDuplicates(event_id, event_time)` behind a 10-minute watermark | Kafka delivers at least once, so duplicates are expected. The watermark bounds the state Spark must keep |
| Aggregates emit only after the watermark closes a window | Output is final (append mode, no later revisions), at the cost of latency |
| Fact table partitioned by `event_date` | Typical queries filter by day, so they scan only the partitions they need |
| One rule per record, first match wins | Every rejected row has exactly one reason, so quarantine counts add up |

## Data-quality rules

| Reason | Rule |
|---|---|
| `unparsable_timestamp` | `event_ts` cannot be parsed |
| `future_timestamp` | Event is more than 5 minutes ahead of the current time |
| `missing_user_id` | `user_id` is null or empty |
| `missing_streamer_id` | `streamer_id` is null |
| `bad_event_type` | Not one of view, like, comment, gift_sent, follow |
| `negative_gift_value` | A gift event with a null, zero or negative coin value |

## Results (20,000-event run, seed 7)

The generator writes a manifest of exactly what it injected, so the pipeline's output can be checked
against known truth rather than eyeballed.

| Check | Expected (manifest) | Pipeline output |
|---|---|---|
| Valid unique events in the fact table | 19,403 | 19,403 |
| Quarantined records | 597 | 597 |
| Duplicates delivered, then removed | 419 | 0 duplicates remain |
| Quarantine counts per reason | 124 / 120 / 120 / 120 / 113 | identical for all five reasons |

Example queries are in [`sql/example_queries.sql`](sql/example_queries.sql) (top streamers by gift
revenue, engagement mix by country, busiest minute per streamer with a window function, quality summary).
Run them with `python -m src.report`.

## Run it

```bash
pip install -r requirements.txt        # needs Java 11+ for Spark

# 1. No Kafka needed: file source
python -m src.generator --events 20000 --out data/incoming
python -m src.stream_job --source files --input data/incoming --root warehouse
python -m src.report --root warehouse

# 2. With Kafka (requires Docker)
docker compose up -d
python -m src.generator --sink kafka --events 20000 --rate 500
python -m src.stream_job --source kafka --trigger continuous

# Tests
pytest
```

## Tests

Six tests (`pytest`), including an end-to-end test that generates events, runs the real streaming job and
reconciles the output with the generator's manifest:

- generator is deterministic and its manifest adds up
- each data-quality rule fires on the records it should and passes clean ones
- fact table is complete and contains no duplicate `event_id`
- quarantine counts match the injected defects, reason by reason
- every closed 1-minute window equals a batch recomputation from the fact table

## Limitations

- **The Kafka source has not been tested yet.** The tests and the results above use the file source.
  The Kafka path (`--source kafka`, `docker-compose.yml`) only swaps the reader, but it has not been run
  end to end, so treat it as unverified until it has been run against a real broker.
- **Synthetic data.** The distributions are random, so the example query results show that the queries
  work, not real audience behaviour.
- **Single machine.** It runs on local Spark with 2 cores. Partitioning and shuffle settings are chosen
  for correctness here, and would need tuning on a cluster.
- **Late data.** Events more than 10 minutes behind the watermark are dropped by design.
- **Approximate distinct counts.** `active_users` uses `approx_count_distinct`.
- **Open windows.** The newest windows are not in the aggregate table until the watermark passes them,
  so `agg_streamer_minute` always lags the fact table.

## Possible next steps

Flink version of the same job for comparison, a schema registry, an anomaly alert on per-minute gift
volume, Airflow orchestration of the batch dimensions, and a cloud deployment.

## Project layout

```
src/generator.py     synthetic events + manifest, file and Kafka sinks
src/schema.py        event schema and reference data
src/dq.py            data-quality rules
src/warehouse.py     dimensions, fact (dedup) and 1-minute aggregates
src/stream_job.py    Structured Streaming entry point
src/report.py        runs the example SQL
sql/                 example analytical queries
tests/               unit and end-to-end tests
docker-compose.yml   single-node Kafka (KRaft)
```
