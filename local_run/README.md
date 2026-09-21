# Local (no-Docker) pipeline runner

Runs the same producer → consumer → ETL flow as `docker-compose.yml`, without
Docker, Kafka, or PostgreSQL. Built because Docker Desktop's backend could not
be started on this machine.

## Run it

```bash
../.venv/Scripts/python.exe run_pipeline.py              # full 527,040 rows
../.venv/Scripts/python.exe run_pipeline.py --limit 5000 # quick smoke test
```

Output lands in `kafka_data.db` (SQLite, ~26 MB). Each run resets it.

Inspect the results:

```bash
../.venv/Scripts/python.exe -c "import sqlite3;c=sqlite3.connect('kafka_data.db');[print(r) for r in c.execute('SELECT * FROM stock_price_summary LIMIT 5')]"
```

## What is faithful to the original

- Same parquet source, same three-field JSON message shape.
- Producer and consumer run **concurrently** in threads — data genuinely
  streams through a bounded queue rather than being staged.
- Same schema as `init.sql`, translated to SQLite types.
- Same transforms: 10-minute `stock_price` mean, 20-minute `sales_trend` mean,
  via `dt.floor()` + `groupby().mean()`.
- Same `to_sql(if_exists='replace')` load, **including** the column-naming
  quirk this produces (see below).

## What differs, and why

| Original | Here | Why |
|---|---|---|
| Kafka topic | bounded in-process queue | no broker available |
| PostgreSQL | SQLite | no server available |
| commit per row | commit per 5,000 rows | 527k commits is needlessly slow |
| `flush()` per message | queue handoff | the original's per-message flush is a synchronous broker round-trip |
| `iterrows()` | `itertuples()` | same row-by-row streaming, far less overhead |
| ETL on a 300s timer, re-run forever | ETL once, after ingestion completes | the timer means early runs summarize a partial table |
| pandas 2.2.2 / py3.9 | pandas 3.0.6 / py3.13 | `requirements.txt` pins target Python 3.9 |

This is **not** a Kafka integration test. It validates the pipeline logic and
the summary calculations, not broker behaviour (partitioning, offsets,
consumer groups, redelivery).

## Known issue reproduced, not fixed

`etl.py` renames its output to `interval_start` / `avg_stock_price` and writes
with `if_exists='replace'`. That **drops** the `stock_price_summary` and
`sales_trend_summary` tables `init.sql` created — which declare
`timestamp` / `stock_price_avg` — and rebuilds them with different column
names. So those two `init.sql` definitions are dead code, and any query
written against them breaks.

Reproduced here deliberately so this run reflects real pipeline behaviour.
Fixing it means picking one naming convention and aligning `init.sql` and
`etl.py`.
