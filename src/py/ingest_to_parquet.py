# src/py/ingest_to_parquet.py
import duckdb
from pathlib import Path

RAW_GLOB = 'data/raw/*.jsonl'
OUT_DIR = 'data/parquet'

Path(OUT_DIR).mkdir(parents=True, exist_ok=True)

con = duckdb.connect(database=':memory:')

# Load ALL jsonl files at once
con.execute("""
CREATE OR REPLACE TABLE all_trades AS
SELECT
  pair,
  CAST(time AS TIMESTAMP) AS ts,
  CAST(trade_id AS BIGINT) AS trade_id,
  CAST(price AS DOUBLE) AS price,
  CAST(size AS DOUBLE) AS size,
  side,
  CAST(time AS DATE) AS date
FROM read_json_auto(?)
""", [RAW_GLOB])

n = con.execute("SELECT COUNT(*) FROM all_trades").fetchone()[0]
if n == 0:
    print("No rows found in data/raw/*.jsonl")
    raise SystemExit(0)

# De-dup across all files
con.execute("""
CREATE OR REPLACE TABLE dedup AS
SELECT * EXCLUDE rn FROM (
  SELECT *,
         row_number() OVER (PARTITION BY pair, date, trade_id ORDER BY ts DESC) rn
  FROM all_trades
) WHERE rn = 1
""")

# ONE copy with overwrite (refreshes the whole dataset once)
con.execute(f"""
COPY dedup TO '{OUT_DIR}'
(FORMAT PARQUET, PARTITION_BY (pair, date), OVERWRITE TRUE)
""")

print("Done → data/parquet/ (partitioned by pair, date)")
