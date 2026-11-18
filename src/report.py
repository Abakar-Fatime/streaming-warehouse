"""Register the warehouse parquet tables as Spark SQL views and run sql/example_queries.sql."""
from __future__ import annotations

import argparse
import pathlib

from pyspark.sql import SparkSession

TABLES = ["fact_live_events", "agg_streamer_minute", "quarantine",
          "dim_streamer", "dim_gift", "dim_country", "dim_device"]


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--root", default="warehouse")
    p.add_argument("--sql", default="sql/example_queries.sql")
    a = p.parse_args()

    spark = (SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    for t in TABLES:
        spark.read.parquet(f"{a.root}/{t}").createOrReplaceTempView(t)

    text = pathlib.Path(a.sql).read_text()
    for stmt in (s.strip() for s in text.split(";")):
        body = "\n".join(l for l in stmt.splitlines() if not l.strip().startswith("--")).strip()
        if not body:
            continue
        comments = [l.strip("- ").strip() for l in stmt.splitlines() if l.strip().startswith("--")]
        title = comments[-1] if comments else ""
        print(f"\n== {title}")
        spark.sql(body).show(truncate=False)


if __name__ == "__main__":
    main()
