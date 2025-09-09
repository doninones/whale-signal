# src/py/build_dataset.py
import argparse, duckdb, pandas as pd, numpy as np
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--pair", default="BTC-USD")
parser.add_argument("--bar", default="15min", help="pandas offset alias, e.g. 15min")
parser.add_argument("--whale_usd", type=float, default=2000.0,
                    help="trade notional >= this is a 'whale' (price*size)")
args = parser.parse_args()

PAIR = args.pair
BAR = args.bar
WHALE_USD = args.whale_usd

# Read all parquet partitions for this pair
pattern = f"data/parquet/pair={PAIR}/date=*/**/*.parquet"
con = duckdb.connect()
trades = con.execute(f"""
  SELECT ts, price, size, side
  FROM read_parquet('{pattern}')
  ORDER BY ts
""").df()

if trades.empty:
  raise SystemExit(f"No trades found for {PAIR}. Did you run ingest?")

# Prepare
trades["ts"] = pd.to_datetime(trades["ts"], utc=True)
trades["notional"] = trades["price"] * trades["size"]
trades["window"] = trades["ts"].dt.floor(BAR)

# We’ll keep groups sorted by time so open/close are meaningful
trades = trades.sort_values("ts")

def agg_window(g: pd.DataFrame) -> pd.Series:
  # basic OHLC
  open_ = g["price"].iloc[0]
  high_ = g["price"].max()
  low_  = g["price"].min()
  close_= g["price"].iloc[-1]

  # totals
  total_count = len(g)
  total_sz    = g["size"].sum()
  total_not   = g["notional"].sum()

  # side splits
  buys = g[g["side"] == "buy"]
  sells = g[g["side"] == "sell"]

  buy_count = len(buys)
  sell_count = len(sells)
  buy_not   = buys["notional"].sum()
  sell_not  = sells["notional"].sum()

  # whales (notional threshold)
  wb = buys[buys["notional"] >= WHALE_USD]
  ws = sells[sells["notional"] >= WHALE_USD]

  whale_buy_count = len(wb)
  whale_sell_count = len(ws)
  whale_buy_not = wb["notional"].sum()
  whale_sell_not = ws["notional"].sum()

  denom = whale_buy_not + whale_sell_not
  whale_imbalance = (whale_buy_not - whale_sell_not) / (denom + 1e-9)

  return pd.Series({
    "open": open_, "high": high_, "low": low_, "close": close_,
    "total_count": total_count, "total_size": total_sz, "total_notional": total_not,
    "buy_count": buy_count, "sell_count": sell_count,
    "buy_notional": buy_not, "sell_notional": sell_not,
    "whale_buy_count": whale_buy_count, "whale_sell_count": whale_sell_count,
    "whale_buy_notional": whale_buy_not, "whale_sell_notional": whale_sell_not,
    "whale_imbalance": whale_imbalance
  })

feat = trades.groupby("window", sort=True, group_keys=False).apply(agg_window).reset_index()
feat.insert(0, "pair", PAIR)

# Save
out = Path(f"data/parquet/{PAIR}_features_15m.parquet")
out.parent.mkdir(parents=True, exist_ok=True)
feat.to_parquet(out, index=False)
print(f"Wrote features → {out}")

