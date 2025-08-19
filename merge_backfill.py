#!/usr/bin/env python3
import argparse, pandas as pd, numpy as np

EPS = 1e-9  # 거의 0 방지용

def asof_le(s: pd.Series, when: pd.Timestamp):
    s = s.sort_index()
    s_le = s.loc[s.index <= when]
    return s_le.iloc[-1] if not s_le.empty else np.nan

def rebase_first(series: pd.Series, base=100.0):
    """맨 앞에서부터 처음으로 EPS보다 큰 값을 찾아 그 값을 base로 맞춤(정확히 첫 값=100)."""
    s = series.astype(float).reset_index(drop=True)
    denom = None
    for v in s:
        if v is not None and np.isfinite(v) and abs(v) > EPS:
            denom = float(v); break
    if denom is None:  # 모두 0/NaN이면 건드리지 않음
        return series
    return series.astype(float) / denom * base

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("backfill_csv")
    ap.add_argument("daily_csv")   # out/bm20_index_from_csv.csv
    ap.add_argument("--out", default="out/bm20_index_merged.csv")
    ap.add_argument("--base", type=float, default=100.0)
    args = ap.parse_args()

    bf = pd.read_csv(args.backfill_csv, parse_dates=["date"]).sort_values("date").reset_index(drop=True)
    dl = pd.read_csv(args.daily_csv,   parse_dates=["date"]).sort_values("date").reset_index(drop=True)

    if bf.empty:
        dl["index"] = rebase_first(dl["index"], args.base)
        dl.to_csv(args.out, index=False)
        print(f"[OK] no backfill; rebased-first → {args.out} rows={len(dl)}")
        return

    first = dl["date"].min()
    bf_idx = bf.set_index("date")["index"].astype(float)
    bf_at_join = asof_le(bf_idx, first)
    dl_at_join = float(dl.loc[dl["date"] == first, "index"].iloc[0])

    if pd.isna(bf_at_join) or abs(bf_at_join) <= EPS:
        print(f"[WARN] bf_at_join invalid (value={bf_at_join}); skip scaling, concat then rebase-first")
        merged = pd.concat([bf, dl], ignore_index=True)
    else:
        scale = dl_at_join / float(bf_at_join)
        pre = bf[bf["date"] < first].copy()
        pre["index"] = pre["index"].astype(float) * scale
        merged = pd.concat([pre, dl], ignore_index=True)

    merged["index"] = rebase_first(merged["index"], args.base)
    merged.to_csv(args.out, index=False)
    print(f"[OK] merged+rebase-first → {args.out} rows={len(merged)} (pre {len(merged)-len(dl)} + daily {len(dl)})")

if __name__ == "__main__":
    main()
