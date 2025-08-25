#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BM20 vs BTC 비교 차트 (최종)
- BM20: series.json (env BM20_SERIES_URL 로 변경 가능)
- BTC: yfinance(BTC-USD) 우선, 실패 시 Yahoo CSV 폴백
- 출력: bm20_vs_btc.png (env BM20_VS_BTC_OUT 로 변경 가능)
필요 패키지: pandas, matplotlib, requests, yfinance
"""

import os, io, json, time, datetime as dt
import requests
import pandas as pd
import matplotlib.pyplot as plt

# ===== 설정 =====
JSON_URL  = os.environ.get("BM20_SERIES_URL", "https://ceciliamyeong.github.io/bm/series.json")
OUT_PNG   = os.environ.get("BM20_VS_BTC_OUT", "bm20_vs_btc.png")
BASE_DATE = os.environ.get("BM20_BASE_DATE", "2018-01-01")  # 시작=100 기준

# ===== 유틸 =====
def pick_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    raise KeyError(f"none found in {candidates}; available={list(df.columns)}")

def to_ts(d: dt.date | dt.datetime) -> int:
    if isinstance(d, dt.date) and not isinstance(d, dt.datetime):
        d = dt.datetime(d.year, d.month, d.day)
    return int(d.replace(tzinfo=dt.timezone.utc).timestamp())

# ===== BM20 로딩 =====
def load_bm20_series(url: str) -> pd.DataFrame:
    r = requests.get(url, timeout=30, headers={"User-Agent": "BM20/1.0"})
    r.raise_for_status()
    try:
        data = r.json()
    except json.JSONDecodeError:
        data = json.loads(r.text)
    df = pd.DataFrame(data)
    date_col  = pick_col(df, ["date", "day", "asof", "asOf", 0])
    value_col = pick_col(df, ["index", "level", "value", "close", "bm20Level", 1])
    df = df.rename(columns={date_col: "date", value_col: "bm20"})[["date","bm20"]]
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["bm20"] = pd.to_numeric(df["bm20"], errors="coerce")
    df = df.dropna(subset=["date","bm20"]).sort_values("date").set_index("date")
    return df

# ===== BTC 로딩 (yfinance → 실패 시 CSV 폴백) =====
def load_btc_yfinance(start_date: dt.date) -> pd.Series:
    import yfinance as yf
    for i in range(3):
        df = yf.download("BTC-USD", start=str(start_date), progress=False)
        if isinstance(df, pd.DataFrame) and not df.empty and "Close" in df.columns:
            s = df["Close"].copy()
            s.index = pd.to_datetime(s.index)
            return s
        time.sleep(2 + i)
    raise RuntimeError("yfinance download failed or empty")

def load_btc_csv(start_date: dt.date, end_date: dt.date | None = None) -> pd.Series:
    if end_date is None:
        end_date = dt.date.today()
    p1 = to_ts(start_date)
    p2 = to_ts(end_date + dt.timedelta(days=1))
    url = ("https://query1.finance.yahoo.com/v7/finance/download/BTC-USD"
           f"?period1={p1}&period2={p2}&interval=1d&events=history&includeAdjustedClose=true")
    r = requests.get(url, timeout=30, headers={"User-Agent":"BM20/1.0"})
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text))
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date","Close"]).sort_values("Date")
    s = pd.Series(df["Close"].values, index=df["Date"].values, name="btc_close")
    return s

def load_btc_series(start_date: dt.date) -> pd.Series:
    try:
        print("[info] BTC via yfinance…")
        return load_btc_yfinance(start_date)
    except Exception as e:
        print(f"[warn] yfinance failed: {e!r} -> fallback to Yahoo CSV")
        return load_btc_csv(start_date)

# ===== 메인 =====
def main():
    print(f"[info] BM20 from {JSON_URL}")
    bm20 = load_bm20_series(JSON_URL)
    start = max(pd.to_datetime(BASE_DATE).date(), bm20.index.min().date())

    btc_close = load_btc_series(start)

    # BM20 정규화(시작=100)
    bm20_norm = bm20.copy()
    base_bm20 = bm20_norm["bm20"].iloc[0]
    bm20_norm["bm20"] = bm20_norm["bm20"] / base_bm20 * 100.0

    # BTC 정규화 + BM20 인덱스에 정렬/앞채움
    btc = btc_close.reindex(bm20_norm.index).ffill()
    base_btc = btc.iloc[0]
    btc_norm = btc / base_btc * 100.0
    btc_norm.name = "btc"

    m = pd.concat([bm20_norm["bm20"], btc_norm], axis=1).dropna()

    # 플롯
    plt.figure(figsize=(10.5, 5.5))
    plt.plot(m.index, m["bm20"], label="BM20 Index (=100 at start)", linewidth=2)
    plt.plot(m.index, m["btc"],  label="BTC-USD (=100 at start)", linewidth=1.6, linestyle="--")
    plt.yscale("log")
    plt.title(f"BM20 Index vs Bitcoin ({start} = 100, log scale)")
    plt.xlabel("Date"); plt.ylabel("Index (log)")
    plt.grid(True, which="both", linestyle="--", alpha=0.5)
    plt.legend()
    plt.tight_layout()
    plt.savefig(OUT_PNG, dpi=150)
    print(f"[ok] wrote {OUT_PNG}  (rows={len(m)})")

if __name__ == "__main__":
    import traceback
    try:
        main()
    except Exception:
        traceback.print_exc()
        raise
