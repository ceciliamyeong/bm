#!/usr/bin/env python3
import argparse, pandas as pd, numpy as np
EPS = 1e-8
def asof_le(s: pd.Series, when: pd.Timestamp):
    s = s.sort_index(); s_le = s.loc[s.index <= when]
    return s_le.iloc[-1] if not s_le.empty else np.nan
def rebase_to(series: pd.Series, base=100.0):
    head = series.head(5).astype(float)
    denom = float(np.median(head[head > EPS])) if (head > EPS).any() else float(series.iloc[0] or 1.0)
    if denom <= EPS: denom = 1.0
    return series / denom * base
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("backfill_csv"); ap.add_argument("daily_csv")
    ap.add_argument("--out", default="out/bm20_index_merged.csv")
    ap.add_argument("--base", type=float, default=100.0)
    args = ap.parse_args()
    bf = pd.read_csv(args.backfill_csv, parse_dates=["date"]).sort_values("date").reset_index(drop=True)
    dl = pd.read_csv(args.daily_csv,   parse_dates=["date"]).sort_values("date").reset_index(drop=True)
    if bf.empty:
        dl["index"] = rebase_to(dl["index"], args.base); dl.to_csv(args.out, index=False)
        print(f"[OK] no backfill; rebased daily → {args.out} rows={len(dl)}"); return
    first = dl["date"].min()
    bf_idx = bf.set_index("date")["index"].astype(float)
    bf_at_join = asof_le(bf_idx, first)
    dl_at_join = float(dl.loc[dl["date"] == first, "index"].iloc[0])
    if pd.isna(bf_at_join) or abs(bf_at_join) <= EPS:
        print(f"[WARN] bf_at_join invalid (value={bf_at_join}); skip scaling, concat then rebase")
        merged = pd.concat([bf, dl], ignore_index=True)
        merged["index"] = rebase_to(merged["index"].astype(float), args.base)
    else:
        scale = dl_at_join / float(bf_at_join)
        pre = bf[bf["date"] < first].copy(); pre["index"] = pre["index"].astype(float) * scale
        merged = pd.concat([pre, dl], ignore_index=True)
        merged["index"] = rebase_to(merged["index"].astype(float), args.base)
    merged.to_csv(args.out, index=False)
    print(f"[OK] merged+rebased → {args.out} rows={len(merged)} (pre {len(merged)-len(dl)} + daily {len(dl)})")
if __name__ == "__main__": main()
