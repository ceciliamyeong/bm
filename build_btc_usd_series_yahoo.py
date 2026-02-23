import os
import json
from datetime import datetime
from pathlib import Path

import pandas as pd
import yfinance as yf

OUT = Path("btc_usd_series.json")

def main():
    start = os.getenv("START_DATE", "2018-01-01").strip() or "2018-01-01"
    # Download daily BTC-USD OHLCV from Yahoo Finance via yfinance
    df = yf.download("BTC-USD", start=start, interval="1d", auto_adjust=False, progress=False)
    if df is None or df.empty:
        raise SystemExit("No data returned for BTC-USD")

    df = df.dropna(subset=["Close"])
    series = [{"date": idx.strftime("%Y-%m-%d"), "price": float(row["Close"])} for idx, row in df.iterrows()]
    series.sort(key=lambda x: x["date"])

    OUT.write_text(json.dumps(series, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[btc] wrote {OUT} ({len(series)} points) from {series[0]['date']} to {series[-1]['date']}")

if __name__ == "__main__":
    main()
