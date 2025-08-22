#!/usr/bin/env python3
import os, json
import pandas as pd
import os, json, pandas as pd, yfinance as yf


BM20_SERIES_LOCAL = "out/series.json"          # 액션에서 bm20.py가 생성하는 경로
BM20_SERIES_FALLBACK = "viewer/series.json"    # (옵션) 이미 저장된 게 있으면 이걸 참고
BM20_JSON = "out/series.json" if os.path.exists("out/series.json") else "viewer/series.json"
OUT_JSON = "viewer/btc_series.json"

def load_bm20_dates():
    path = BM20_SERIES_LOCAL if os.path.exists(BM20_SERIES_LOCAL) else BM20_SERIES_FALLBACK
    if not os.path.exists(path):
        raise FileNotFoundError("BM20 series.json not found. Run bm20.py first or provide viewer/series.json")
    df = pd.read_json(path)
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)
    df = df.sort_values("date")
    return df["date"].iloc[0].date(), df["date"].iloc[-1].date(), df["date"]

df = pd.read_json(BM20_JSON)
df["date"] = pd.to_datetime(df["date"])
d0 = df["date"].min().date()

btc = yf.download("BTC-USD", start=str(d0))["Close"].dropna()
base = btc.iloc[0]
idx = (btc/base)*100.0
aligned = df["date"].dt.normalize().to_series().map(idx.reindex(btc.index.normalize()).ffill().to_dict())
arr = [[d.strftime("%Y-%m-%d"), float(v)] for d, v in zip(df["date"], aligned)]
os.makedirs("viewer", exist_ok=True)
json.dump(arr, open(OUT,"w",encoding="utf-8"), ensure_ascii=False)
print("[OK]", OUT, len(arr), "pts")


def main():
    d0, dN, bm_dates = load_bm20_dates()
    btc = yf.download("BTC-USD", start=str(d0), end=None)["Close"].rename("px")
    btc = btc.sort_index()
    # 2018-01-01 = 100 정규화
    base = btc.iloc[0]
    btc_idx = (btc / base) * 100.0
    # BM20 날짜에 맞춰 정렬/보간
    out = pd.Series(index=pd.to_datetime(bm_dates).dt.tz_localize(None), dtype="float64")
    out.loc[btc_idx.index] = btc_idx.values
    out = out.ffill()
    # JSON: [ [date, index], ... ]
    arr = [[d.strftime("%Y-%m-%d"), float(v)] for d, v in out.items() if pd.notna(v)]
    os.makedirs(os.path.dirname(OUT_JSON), exist_ok=True)
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(arr, f, ensure_ascii=False)
    print(f"[OK] wrote {OUT_JSON} ({arr[0][0]} → {arr[-1][0]}, {len(arr)} pts)")

if __name__ == "__main__":
    main()
