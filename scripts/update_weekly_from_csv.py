#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, json, os
from datetime import datetime
from pathlib import Path
import pandas as pd

DATE_CANDS = ["date", "Date", "날짜"]
CLOSE_CANDS = ["종가", "Close", "close", "Adj Close", "adj_close", "종가(원)"]

def read_price_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    cols = {c: c.strip() for c in df.columns}
    df.columns = [cols[c] for c in df.columns]

    date_col = next((c for c in df.columns if c in DATE_CANDS), None)
    close_col = next((c for c in df.columns if c in CLOSE_CANDS), None)

    if date_col is None or close_col is None:
        raise ValueError(f"[{path}] needs date+close columns. got={list(df.columns)}")

    out = df[[date_col, close_col]].copy()
    out.columns = ["date", "close"]
    out["date"] = pd.to_datetime(out["date"]).dt.date
    out["close"] = pd.to_numeric(out["close"], errors="coerce")
    out = out.dropna(subset=["close"]).sort_values("date")
    return out

def read_bm20_json(path: str) -> pd.DataFrame:
    # 다양한 형태를 최대한 흡수: [{"date":"YYYY-MM-DD","bm20":...}] / {"data":[...]} / {"dates":[...],"bm20":[...]}
    raw = json.loads(Path(path).read_text(encoding="utf-8"))

    if isinstance(raw, dict) and "data" in raw and isinstance(raw["data"], list):
        arr = raw["data"]
        df = pd.DataFrame(arr)
    elif isinstance(raw, dict) and "dates" in raw and "bm20" in raw:
        df = pd.DataFrame({"date": raw["dates"], "bm20": raw["bm20"]})
    elif isinstance(raw, list):
        df = pd.DataFrame(raw)
    else:
        raise ValueError(f"Unsupported bm20 json structure: {path}")

    # 컬럼 자동 인식
    cols_lower = {c.lower(): c for c in df.columns}
    date_col = cols_lower.get("date") or cols_lower.get("날짜")
    val_col = None
    for cand in ["bm20", "level", "close", "index", "value"]:
        if cand in cols_lower:
            val_col = cols_lower[cand]
            break
    if not date_col or not val_col:
        raise ValueError(f"[{path}] need date + value columns. got={list(df.columns)}")

    out = df[[date_col, val_col]].copy()
    out.columns = ["date", "bm20"]
    out["date"] = pd.to_datetime(out["date"]).dt.date
    out["bm20"] = pd.to_numeric(out["bm20"], errors="coerce")
    out = out.dropna(subset=["bm20"]).sort_values("date")
    return out

def mmdd(d) -> str:
    return pd.Timestamp(d).strftime("%m/%d")

def find_bm20_json(user_path: str | None) -> str:
    cands = []
    if user_path:
        cands.append(user_path)
    cands += [
        "bm/bm20_series.json",
        "bm20_series.json",
        "bm/assets/bm20_series.json",
    ]
    for p in cands:
        if p and Path(p).exists():
            return p
    raise FileNotFoundError("bm20_series.json not found. Tried: " + ", ".join(cands))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--kospi_csv", required=True)
    ap.add_argument("--nasdaq_csv", required=True)
    ap.add_argument("--btc_csv", required=True)
    ap.add_argument("--bm20_json", default=None)
    ap.add_argument("--out_json", default="bm/assets/weekly/weekly_report.json")
    ap.add_argument("--title", default="BM20 Weekly Dashboard")
    args = ap.parse_args()

    kospi = read_price_csv(args.kospi_csv).rename(columns={"close":"kospi"})
    nasdaq = read_price_csv(args.nasdaq_csv).rename(columns={"close":"nasdaq"})
    btc = read_price_csv(args.btc_csv).rename(columns={"close":"btc"})

    bm20_path = find_bm20_json(args.bm20_json)
    bm20 = read_bm20_json(bm20_path)

    # 기준 날짜: KOSPI 최신 5거래일(=이번 주)
    kospi_week = kospi.tail(5).copy()
    week_dates = kospi_week["date"].tolist()

    # join: 기준 날짜에 맞춰 다른 시계열 매칭 (없으면 forward-fill로 보강)
    base = pd.DataFrame({"date": week_dates})
    base = base.merge(kospi, on="date", how="left")
    base = base.merge(nasdaq, on="date", how="left")
    base = base.merge(btc, on="date", how="left")
    base = base.merge(bm20, on="date", how="left")

    base = base.sort_values("date")
    for c in ["nasdaq","btc","bm20"]:
        base[c] = base[c].ffill()

    if base[["kospi","nasdaq","btc","bm20"]].isna().any().any():
        missing = base[base[["kospi","nasdaq","btc","bm20"]].isna().any(axis=1)][["date","kospi","nasdaq","btc","bm20"]]
        raise ValueError("Missing values after ffill:\n" + missing.to_string(index=False))

    payload = {
        "meta": {
            "title": args.title,
            "week_start": str(base["date"].min()),
            "week_end": str(base["date"].max()),
            "timezone_note": "Close 기준(레벨) 데이터. 차트에서 상대변화(%)로 비교.",
            "generated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        },
        "dates": [mmdd(d) for d in base["date"].tolist()],
        "bm20": base["bm20"].round(2).tolist(),
        "kospi": base["kospi"].round(2).tolist(),
        "nasdaq": base["nasdaq"].round(2).tolist(),
        "btc": base["btc"].round(2).tolist(),

        # 아래 3개는 일단 유지(혹은 기존 계산 로직 붙이면 자동화 가능)
        "contrib_best3": [["BTC", 0.24], ["XRP", 0.11], ["SOL", 0.09]],
        "contrib_worst3": [["ETH", -0.24], ["BNB", -0.08], ["DOGE", -0.06]],
        "sectors": [["Smart Contract/L1", 3.8], ["Payments", 2.4], ["DeFi", 1.1], ["Core", -0.6], ["Meme", -1.9], ["RWA", -3.2]]
    }

    out_path = Path(args.out_json)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[OK] wrote {out_path}")

if __name__ == "__main__":
    main()
