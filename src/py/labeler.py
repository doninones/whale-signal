# src/py/labeler.py
import argparse, duckdb, pandas as pd, numpy as np
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--pair", default="BTC-USD")
parser.add_argument("--tp_pct", type=float, default=0.05, help="take profit (fraction)")
parser.add_argument("--dd_guard", type=float, default=-0.015, help="forward drawdown guard (fraction, negative)")
parser.add_argument("--bar", default="15min")
parser.add_argument("--horizons", default="2h,4h,8h,24h")
args = parser.parse_args()

PAIR = args.pair
feat_path = Path(f"data/parquet/{PAIR}_features_15m.parquet")
if not feat_path.exists():
    raise SystemExit(f"Missing features parquet: {feat_path}. Run build_dataset.py first.")

df = pd.read_parquet(feat_path).sort_values("window").reset_index(drop=True)

# forward “close at +H” and max/min within horizon using 15m bars
def ahead_minutes(h):
    return {"2h":120,"4h":240,"8h":480,"24h":1440}[h]

HORIZONS = [h.strip() for h in args.horizons.split(",") if h.strip()]

# Build a rolling table with future close, high, low for each horizon
for H in HORIZONS:
    steps = ahead_minutes(H) // 15
    # future close at H (close-to-close return proxy)
    df[f"fut_close_{H}"] = df["close"].shift(-steps)
    df[f"fut_close_ret_{H}"] = (df[f"fut_close_{H}"] / df["close"]) - 1.0

    # future max/min over window (using expanding over next N rows with pandas trick)
    # compute rolling per-row forward max/min by applying on reversed series
    fwd_high = df["high"].iloc[::-1].rolling(window=steps, min_periods=1).max().iloc[::-1]
    fwd_low  = df["low"].iloc[::-1].rolling(window=steps, min_periods=1).min().iloc[::-1]

    df[f"fut_max_ret_{H}"] = (fwd_high / df["close"]) - 1.0
    df[f"fwd_dd_{H}"] = (fwd_low / df["close"]) - 1.0  # drawdown is negative when price goes down

# Build a clean, auto-named label for each horizon using tp/dd
def fmt_pct(x):
    # 0.02 -> '2pct', 0.05 -> '5pct'
    return f"{int(round(abs(x)*100))}pct"

tp_tag = fmt_pct(args.tp_pct)
dd_tag = f"dd{int(round(abs(args.dd_guard)*100))}pct"

for H in HORIZONS:
    label_col = f"label_bull_{H}_{tp_tag}_{dd_tag}"
    df[label_col] = ((df[f"fut_max_ret_{H}"] >= args.tp_pct) & (df[f"fwd_dd_{H}"] >= args.dd_guard)).astype(int)

out = Path(f"data/parquet/{PAIR}_features_labels_15m.parquet")
df.to_parquet(out, index=False)
print(f"Wrote labeled features → {out}")
print("Available label columns:")
print([c for c in df.columns if c.startswith("label_bull_")])
