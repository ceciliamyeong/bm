#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
import json
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent

BM20_JSON = ROOT / "bm20_latest.json"
DAILY_CSV = ROOT / "bm20_daily_data_latest.csv"
BTC_SERIES = ROOT / "out/history/btc_usd_series.json"

def pick_asof(bm20: dict) -> str:
    for k in ("asOf", "asof", "date", "timestamp"):
        v = bm20.get(k)
        if v:
            return str(v)[:10]
    raise KeyError("bm20_latest.json missing date key")

def load_series() -> list:
    if not BTC_SERIES.exists():
        BTC_SERIES.parent.mkdir(parents=True, exist_ok=True)
        return []
    obj = json.loads(BTC_SERIES.read_text(encoding="utf-8") or "[]")
    return obj if isinstance(obj, list) else []

def get_btc_from_csv() -> float:
    df = pd.read_csv(DAILY_CSV)
    row = df[df["symbol"].astype(str).str.upper() == "BTC"].head(1)
    if row.empty:
        raise ValueError("BTC row not found in CSV")
    return float(row.iloc[0]["current_price"])

def update():
    bm20 = json.loads(BM20_JSON.read_text(encoding="utf-8"))
    asof = pick_asof(bm20)
    btc_price = get_btc_from_csv()
    series = load_series()

    if series and str(series[-1].get("date")) == asof:
        series[-1]["price"] = btc_price
    else:
        series.append({"date": asof, "price": btc_price})

    BTC_SERIES.write_text(json.dumps(series, ensure_ascii=False, indent=2), encoding="utf-8")
    print("BTC series updated:", asof, btc_price)

if __name__ == "__main__":
    update()

