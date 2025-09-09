import argparse, pandas as pd, numpy as np
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument('--pair', default='BTC-USD')
parser.add_argument('--tp_pct', type=float, default=0.05)     # +5% breakout
parser.add_argument('--dd_guard', type=float, default=-0.015) # -1.5% max drawdown
args = parser.parse_args()

PAIR = args.pair
df = pd.read_parquet(f"data/parquet/{PAIR}_features_15m.parquet").sort_values('window')

H_STEPS = {'2h':8,'4h':16,'8h':32,'24h':96}

def future_max_return(close, high, steps):
    mx = high.rolling(window=steps, min_periods=1).max().shift(-steps+1)
    return (mx / close) - 1.0

def future_min_dd(open_, low, steps):
    mn = low.rolling(window=steps, min_periods=1).min().shift(-steps+1)
    return (mn / open_) - 1.0

for name, H in H_STEPS.items():
    df[f'fut_max_ret_{name}'] = future_max_return(df['close'], df['high'], H)
    df[f'fwd_dd_{name}'] = future_min_dd(df['open'], df['low'], H)

# Example bull label: +5% within 4h, without >1.5% interim drawdown
df['label_bull_4h_5pct_dd1p5'] = ((df['fut_max_ret_4h'] >= args.tp_pct) & (df['fwd_dd_4h'] >= args.dd_guard)).astype(int)
df['label_bull_2h_5pct_dd1p5'] = ((df['fut_max_ret_2h'] >= args.tp_pct) & (df['fwd_dd_2h'] >= args.dd_guard)).astype(int)

out_path = Path(f"data/parquet/{PAIR}_features_labels_15m.parquet")
df.to_parquet(out_path, index=False)
print(f"Wrote labeled features → {out_path}")
