#!/usr/bin/env python3
import argparse, pandas as pd

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("backfill_csv")
    ap.add_argument("daily_csv")   # out/bm20_index_from_csv.csv
    ap.add_argument("--out", default="out/bm20_index_merged.csv")
    ap.add_argument("--base", type=float, default=100.0)  # 시작 레벨
    args = ap.parse_args()

    bf = pd.read_csv(args.backfill_csv, parse_dates=["date"])
    dl = pd.read_csv(args.daily_csv,   parse_dates=["date"])
    if bf.empty:
        # 백필이 없으면 일별 지수만 리베이스해서 사용
        dl = dl.sort_values("date").reset_index(drop=True)
        first_val = float(dl.loc[0, "index"])
        dl["index"] = dl["index"] / first_val * args.base
        dl.to_csv(args.out, index=False)
        print(f"[OK] no backfill; rebased daily → {args.out} rows={len(dl)}")
        return

    bf = bf.sort_values("date").reset_index(drop=True)
    dl = dl.sort_values("date").reset_index(drop=True)

    first = dl["date"].min()
    # 백필에서 first와 같거나 그 이전 중 가장 최근 값 사용 (asof 매칭)
    bf_le = bf[bf["date"] <= first]
    if bf_le.empty:
        # 이 경우엔 스케일 불가능 → 그냥 연결 후 리베이스
        merged = pd.concat([bf, dl], ignore_index=True)
        first_val = float(merged.loc[0, "index"])
        merged["index"] = merged["index"] / first_val * args.base
        merged.to_csv(args.out, index=False)
        print(f"[WARN] no bf date <= first; concatenated+rebased → {args.out} rows={len(merged)}")
        return

    bf_at_join = float(bf_le.iloc[-1]["index"])
    dl_at_join = float(dl.loc[dl["date"] == first, "index"].iloc[0])

    scale = dl_at_join / bf_at_join
    pre = bf[bf["date"] < first].copy()
    pre["index"] = pre["index"] * scale

    merged = pd.concat([pre, dl], ignore_index=True)

    # 최종 안전장치: 시계열 시작값을 base로 리베이스
    first_val = float(merged.loc[0, "index"])
    merged["index"] = merged["index"] / first_val * args.base

    merged.to_csv(args.out, index=False)
    print(f"[OK] merged+rebased → {args.out} rows={len(merged)} (pre {len(pre)} + daily {len(dl)})")

if __name__ == "__main__":
    main()

