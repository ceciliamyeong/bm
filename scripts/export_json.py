#!/usr/bin/env python3
import json, sys, os, pandas as pd
from datetime import datetime

def compute_stats(df: pd.DataFrame):
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")
    today = df["date"].max()
    ytd_start = pd.Timestamp(year=today.year, month=1, day=1)
    mtd_start = pd.Timestamp(year=today.year, month=today.month, day=1)

    def total_return(s):
        if len(s) <= 1: return 0.0
        return float((s["index"].iloc[-1] / s["index"].iloc[0]) - 1.0)

    ytd = total_return(df[df["date"] >= ytd_start]) if (df["date"]>=ytd_start).any() else 0.0
    mtd = total_return(df[df["date"] >= mtd_start]) if (df["date"]>=mtd_start).any() else 0.0
    d1  = float(df["ret"].iloc[-1])
    return {"date": str(today.date()), "mtd": round(mtd,6), "ytd": round(ytd,6), "d1": round(d1,6)}

def main(index_csv, out_dir="site"):
    os.makedirs(out_dir, exist_ok=True)
    df = pd.read_csv(index_csv)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")

    latest = {"date": str(df['date'].iloc[-1].date()), "index": float(df["index"].iloc[-1]), "ret": float(df["ret"].iloc[-1])}
    stats = compute_stats(df)

    with open(os.path.join(out_dir, "latest.json"), "w", encoding="utf-8") as f:
        json.dump(latest, f, ensure_ascii=False, indent=2)
    with open(os.path.join(out_dir, "series.json"), "w", encoding="utf-8") as f:
        json.dump([{"date": str(d.date()), "index": float(v)} for d, v in zip(df["date"], df["index"])], f, ensure_ascii=False, indent=2)
    with open(os.path.join(out_dir, "stats.json"), "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)
    print("[OK] JSON exported →", out_dir)

if __name__ == "__main__":
    index_csv = sys.argv[1] if len(sys.argv) > 1 else "out/bm20_index_from_csv.csv"
    out_dir   = sys.argv[2] if len(sys.argv) > 2 else "site"
    main(index_csv, out_dir)
