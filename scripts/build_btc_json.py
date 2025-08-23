#!/usr/bin/env python3
import os, json
import pandas as pd
import yfinance as yf

BM20_JSON_CANDIDATES = ["out/series.json", "viewer/series.json", "series.json"]
OUT_JSON = "viewer/btc_series.json"

def load_bm20_series():
    for p in BM20_JSON_CANDIDATES:
        if os.path.exists(p):
            df = pd.read_json(p)
            df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)
            return df.sort_values("date").reset_index(drop=True)
    raise FileNotFoundError(f"BM20 series.json not found in {BM20_JSON_CANDIDATES}")

def main():
    bm = load_bm20_series()
    d0 = bm["date"].min().date()

    # BTC-USD 다운로드
    btc = yf.download("BTC-USD", start=str(d0), progress=False)["Close"].dropna()
    btc.index = pd.to_datetime(btc.index).tz_localize(None).normalize()

    # 2018-01-01 = 100 정규화
    base = btc.iloc[0]
    idx = (btc / base) * 100.0  # pandas Series (index: normalized date)

    # BM20 날짜에 맞춰 정렬/보간
    target_dates = bm["date"].dt.normalize()
    aligned = pd.Series(index=target_dates, dtype="float64")

    inter = idx.index.intersection(aligned.index)
    aligned.update(idx)  # 또는: aligned = btc_idx.reindex(bm.index).ffill()

    # JSON 저장: [ [date, index], ... ]
    arr = [[d.strftime("%Y-%m-%d"), float(v)] for d, v in zip(bm["date"], aligned)]
    os.makedirs(os.path.dirname(OUT_JSON), exist_ok=True)
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(arr, f, ensure_ascii=False)
    print(f"[OK] wrote {OUT_JSON} ({arr[0][0]} → {arr[-1][0]}, {len(arr)} pts)")

if __name__ == "__main__":
    main()
