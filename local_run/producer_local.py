"""Local stand-in for producer/producer.py.

Same job: read the parquet file, turn each row into a JSON message with the
same three fields, and publish it. Only the transport differs.
"""
import json
from pathlib import Path

import pandas as pd

DATA_PATH = Path(__file__).resolve().parents[1] / "data" / "synthetic_data.parquet"


def load_dataframe(limit=None):
    df = pd.read_parquet(DATA_PATH, engine="pyarrow")
    # Same cast the original does, so the JSON payload is string-typed.
    df["timestamp"] = df["timestamp"].astype("str")
    if limit is not None:
        df = df.head(limit)
    return df


def run_producer(bus, limit=None, progress_every=50_000):
    """Publish every row to the bus, then close the stream."""
    df = load_dataframe(limit)
    total = len(df)
    print(f"[producer] loaded {total:,} rows from {DATA_PATH.name}")
    print("[producer] producing messages...")

    # itertuples rather than iterrows: same row-by-row streaming, far less
    # per-row overhead at 527k rows.
    for i, row in enumerate(df.itertuples(index=False), start=1):
        message = {
            "timestamp": row.timestamp,
            "stock_price": row.stock_price,
            "sales_trend": row.sales_trend,
        }
        bus.produce(json.dumps(message).encode("utf-8"))

        if i % progress_every == 0:
            print(f"[producer] {i:,}/{total:,} sent")

    bus.close()
    print(f"[producer] finished. {total:,} messages produced.")
    return total
