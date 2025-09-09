# src/py/report_sweep.py
import argparse, duckdb, pandas as pd, numpy as np
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--pair", default="BTC-USD")
parser.add_argument("--horizon", choices=["2h","4h","8h","24h"], default="4h")
parser.add_argument("--imb_range", default="0.10,0.15,0.20,0.25,0.30,0.35")
parser.add_argument("--min_counts", default="1,2,3,4,5")
parser.add_argument("--fee", type=float, default=0.0005)  # per side
parser.add_argument("--tp_pct", type=float, default=0.05) # only used if label col missing
parser.add_argument("--dd_guard", type=float, default=-0.015)
args = parser.parse_args()

pair = args.pair
feat_path = Path(f"data/parquet/{pair}_features_labels_15m.parquet")
if not feat_path.exists():
    feat_path = Path(f"data/parquet/{pair}_features_15m.parquet")
if not feat_path.exists():
    raise SystemExit(f"Features parquet not found for {pair}")

con = duckdb.connect()
df = con.execute(f"SELECT * FROM read_parquet('{feat_path.as_posix()}')").df()

# Required columns
need = {"whale_buy_count","whale_sell_count","whale_imbalance"}
missing = need - set(df.columns)
if missing:
    raise SystemExit(f"Missing columns in features: {missing}")

H = args.horizon
fut_col = f"fut_max_ret_{H}"
dd_col  = f"fwd_dd_{H}"
label_guess = f"label_bull_{H}_5pct_dd1p5"

has_label = label_guess in df.columns
if not has_label and (fut_col not in df.columns or dd_col not in df.columns):
    raise SystemExit(f"Need {fut_col} and {dd_col} (run labeler.py), or add label column.")

imb_list = [float(x) for x in args.imb_range.split(",") if x]
minc_list = [int(x) for x in args.min_counts.split(",") if x]

rows = []
valid = df[fut_col].notna() if fut_col in df else pd.Series(True, index=df.index)

for imb in imb_list:
    for minc in minc_list:
        # Long signal: whale buy imbalance above threshold AND enough whale buys
        sig = (df["whale_imbalance"] >= imb) & (df["whale_buy_count"] >= minc) & valid

        signals = int(sig.sum())
        coverage = float(sig.mean())

        if has_label:
            precision = float(df.loc[sig, label_guess].mean()) if signals else 0.0
        else:
            # fallback: treat target as hitting tp_pct with DD guard
            precision = float(((df.loc[sig, fut_col] >= args.tp_pct) & (df.loc[sig, dd_col] >= args.dd_guard)).mean()) if signals else 0.0

        gross = df.loc[sig, fut_col].mean() if signals else np.nan
        net   = (df.loc[sig, fut_col] - 2*args.fee).mean() if signals else np.nan

        rows.append({
            "pair": pair, "horizon": H, "imb": imb, "min_count": minc,
            "signals": signals, "coverage": coverage, "precision": precision,
            "avg_gross": gross, "avg_net": net
        })

res = pd.DataFrame(rows).sort_values(
    ["precision","avg_net","signals"], ascending=[False, False, False]
)
outdir = Path("data/reports"); outdir.mkdir(parents=True, exist_ok=True)
outcsv = outdir / f"{pair}_sweep_{H}.csv"
res.to_csv(outcsv, index=False)

print(res.head(15).to_string(index=False))
print(f"\nSaved full grid → {outcsv}")
