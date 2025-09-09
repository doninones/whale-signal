# src/py/backtest_baselines.py
import argparse, pandas as pd, numpy as np
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument('--pair', default='BTC-USD')
parser.add_argument('--imb', type=float, default=0.30, help='whale imbalance threshold')
parser.add_argument('--min_count', type=int, default=3, help='min whale buy count')
parser.add_argument('--horizon', choices=['2h','4h','8h','24h'], default='4h', help='future return horizon')
parser.add_argument('--label', default=None, help='label column to score precision against (auto if None)')
parser.add_argument('--fee', type=float, default=0.001, help='taker fee per side (fraction, e.g. 0.001 = 10bps)')
parser.add_argument('--imb_col', default=None, help='override imbalance column name')
parser.add_argument('--count_col', default=None, help='override whale buy count column name')
parser.add_argument('--quiet', action='store_true', help='suppress extra info')
args = parser.parse_args()

PAIR = args.pair
feat_path = Path(f"data/parquet/{PAIR}_features_labels_15m.parquet")
if not feat_path.exists():
    raise SystemExit(f"Features+labels parquet not found: {feat_path}. Run build_dataset.py then labeler.py first.")

df = pd.read_parquet(feat_path).sort_values('window')

def pick_col(candidates, required=True):
    if args.imb_col and candidates == 'imb':
        return args.imb_col
    if args.count_col and candidates == 'count':
        return args.count_col
    if candidates == 'imb':
        options = ['whale_imbalance', 'imbalance_usd_rel', 'imbalance_rel', 'imbalance']
    else:
        options = ['whale_buy_count', 'count_whale_buy_rel', 'buy_count_whale', 'buy_whale_count']
    for c in options:
        if c in df.columns:
            return c
    if required:
        raise SystemExit(f"Could not find a suitable column for {'imbalance' if candidates=='imb' else 'whale buy count'}. "
                         f"Available columns: {list(df.columns)}")
    return None

imb_col = args.imb_col or pick_col('imb')
cnt_col = args.count_col or pick_col('count')

H = args.horizon
fut_col = f"fut_max_ret_{H}"
if fut_col not in df.columns:
    raise SystemExit(f"Missing {fut_col} in dataset. Re-run labeler.py.")

label_col = args.label or f"label_bull_{H}_5pct_dd1p5"  # default naming from labeler.py
has_label = label_col in df.columns

# Build signal
df['signal_long'] = ((df[imb_col] >= args.imb) & (df[cnt_col] >= args.min_count)).astype(int)

# Only evaluate rows that actually have a valid horizon future
mask_valid = df[fut_col].notna()
mask_signal = (df['signal_long'] == 1) & mask_valid

signals = int(mask_signal.sum())
coverage = float(df['signal_long'].mean())

if has_label:
    precision = float(df.loc[mask_signal, label_col].mean()) if signals else 0.0
else:
    precision = 0.0  # label missing; you can compute a fallback precision if desired

gross = df.loc[mask_signal, fut_col]
net = gross - 2 * args.fee

# Output
print(f"PAIR: {PAIR} | HORIZON: {H}")
print(f"Using columns → imbalance: {imb_col}  | whale_count: {cnt_col}")
print(f"Thresholds → imb ≥ {args.imb:.2f}, min_count ≥ {args.min_count}")
print(f"Signals: {signals} | Coverage: {coverage:.3f}"
      + (f" | Precision[{label_col}]: {precision:.3f}" if has_label else " | Precision: N/A (label missing)"))

if signals > 0:
    print(f"Avg gross future-max: {gross.mean():.4f} | Avg net (fees only): {net.mean():.4f}")
else:
    # small nudge if no signals—helps debugging thresholds
    if not args.quiet:
        # show top 5 windows by imbalance to see where you’re close to firing
        if imb_col in df.columns:
            near = df.sort_values(imb_col, ascending=False)[['window', imb_col, cnt_col]].head(5)
            print("\nTop windows by imbalance (peek):")
            with pd.option_context('display.max_colwidth', None):
                print(near.to_string(index=False))
