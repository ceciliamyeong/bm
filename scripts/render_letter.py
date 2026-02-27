#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
import json
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent

BTC_SERIES = ROOT / "out/history/btc_usd_series.json"
BM20_JSON  = ROOT / "bm20_latest.json"
DAILY_CSV  = ROOT / "bm20_daily_data_latest.csv"

def pick_date(bm20: dict) -> str:
    for k in ("asOf", "asof", "date", "timestamp"):
        v = bm20.get(k)
        if v:
            return str(v)[:10]  # "YYYY-MM-DD"만
    raise KeyError("bm20_latest.json missing date key (expected asOf/asof/date/timestamp)")

def pick_btc_price_usd() -> float:
    if not DAILY_CSV.exists():
        raise FileNotFoundError(f"Missing {DAILY_CSV}")
    df = pd.read_csv(DAILY_CSV)

    # 컬럼 표준화
    if "symbol" not in df.columns:
        for c in ("ticker", "asset"):
            if c in df.columns:
                df = df.rename(columns={c: "symbol"})
                break
    if "current_price" not in df.columns:
        for c in ("price", "price_usd", "close"):
            if c in df.columns:
                df = df.rename(columns={c: "current_price"})
                break

    row = df[df["symbol"].astype(str).str.upper() == "BTC"].head(1)
    if row.empty:
        raise ValueError("BTC row not found in bm20_daily_data_latest.csv")
    return float(row.iloc[0]["current_price"])

def load_series() -> list:
    if not BTC_SERIES.exists():
        BTC_SERIES.parent.mkdir(parents=True, exist_ok=True)
        return []
    s = json.loads(BTC_SERIES.read_text(encoding="utf-8") or "[]")
    return s if isinstance(s, list) else []

def update():
    bm20 = json.loads(BM20_JSON.read_text(encoding="utf-8"))
    asof = pick_date(bm20)
    btc_price = pick_btc_price_usd()

    series = load_series()

    # 중복 방지
    if series and str(series[-1].get("date")) == asof:
        print("BTC already updated.")
        return

    series.append({"date": asof, "price": btc_price})
    BTC_SERIES.write_text(json.dumps(series, indent=2), encoding="utf-8")
    print("BTC series updated:", asof, btc_price)

if __name__ == "__main__":
    update()
