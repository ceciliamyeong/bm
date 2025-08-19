#!/usr/bin/env python3
import argparse, pandas as pd

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("backfill_csv")
    ap.add_argument("daily_csv")   # out/bm20_index_from_csv.csv
    ap.add_argument("--out", default="out/bm20_index_merged.csv")
    args = ap.parse_args()

    bf = pd.read_csv(args.backfill_csv, parse_dates=["date"])
    dl = pd.read_csv(args.daily_csv, parse_dates=["date"])
    if bf.empty:
        dl.to_csv(args.out, index=False)
        print(f"[OK] no backfill, passthrough → {args.out} rows={len(dl)}")
        return
    first = dl["date"].min()
    pre = bf[bf["date"] < first].copy()
    if pre.empty:
        dl.to_csv(args.out, index=False)
        print(f"[OK] backfill exists but no earlier rows, passthrough → {args.out} rows={len(dl)}")
        return
    bf_at_join = bf.loc[bf["date"]==first, "index"]
    if not bf_at_join.empty:
        scale = float(dl.loc[dl["date"]==first, "index"].iloc[0]) / float(bf_at_join.iloc[0])
        pre["index"] = pre["index"] * scale
    merged = pd.concat([pre, dl], ignore_index=True)
    merged.to_csv(args.out, index=False)
    print(f"[OK] merged → {args.out} rows={len(merged)} (backfill {len(pre)} + daily {len(dl)})")

if __name__ == "__main__":
    main()
