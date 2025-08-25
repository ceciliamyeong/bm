#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, json, math
import requests
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import yfinance as yf

SERIES_URL = os.environ.get("BM20_SERIES_URL", "https://ceciliamyeong.github.io/bm/series.json")
BASE_DATE  = os.environ.get("BM20_BASE_DATE", "2018-01-01")
OUT_PNG    = os.environ.get("BM20_VS_BTC_OUT", "bm20_vs_btc.png")

VAL_KEYS   = ["index", "level", "value", "close", "bm20Level"]
DATE_KEYS  = ["date", "day", "asof", "asOf"]

def _pick(d, keys, fallback_idx=None):
    for k in keys:
        if isinstance(d, dict) and k in d:
            return d[k]
    if isinstance(d, (list, tuple)) and fallback_idx is not None and len(d) > fallback_idx:
        return d[fallback_idx]
    return None

def load_bm20_series(url: str) -> pd.DataFrame:
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()
    # data가 dict면 내부 배열을 찾아서 사용
    if isinstance(data, dict):
        for k, v in data.items():
            if isinstance(v, list) and v and isinstance(v[0], (dict, list)):
                data = v
                break
    if not isinstance(data, list):
        raise ValueError("series.json 형식이 리스트가 아님")

    rows = []
    for row in data:
        t = _pick(row, DATE_KEYS, 0)
        v = _pick(row, VAL_KEYS, 1)
        if t is None or v is None:
            continue
        try:
            v = float(v)
        except Exception:
            continue
        rows.append({"date": pd.to_datetime(t), "bm": v})

    if not rows:
        raise ValueError("series.json에서 사용할 값이 없음")

    df = pd.DataFrame(rows).sort_values("date").drop_duplicates("date")
    df = df.set_index("date")
    return df

def load_btc_series(start_date: str, index_like: pd.DatetimeIndex) -> pd.Series:
    btc = yf.download("BTC-USD", start=start_date, progress=False)["Close"]
    # normalize to 100 at BASE_DATE
    btc = btc / btc.iloc[0] * 100.0
    btc = btc.reindex(index_like).ffill()
    return btc

def main():
    bm20 = load_bm20_series(SERIES_URL)
    btc  = load_btc_series(BASE_DATE, bm20.index)

    plt.figure(figsize=(12, 6))
    plt.plot(bm20.index, bm20["bm"], label="BM20 Index", linewidth=2)
    plt.plot(btc.index,  btc,        label="BTC (normalized)", linewidth=2, linestyle="--")

    plt.yscale("log")
    plt.title("BM20 Index vs Bitcoin (2018-01-01 = 100)", fontsize=14)
    plt.xlabel("Date")
    plt.ylabel("Index (Log Scale)")
    plt.grid(True, which="both", linestyle="--", alpha=0.6)
    plt.legend()

    plt.tight_layout()
    os.makedirs(os.path.dirname(OUT_PNG) or ".", exist_ok=True)
    plt.savefig(OUT_PNG, dpi=150)
    print(f"[ok] wrote {OUT_PNG}")

if __name__ == "__main__":
    main()
