#!/usr/bin/env python3
import os, json
import pandas as pd
import numpy as np
import yfinance as yf

BM20_JSON_CANDIDATES = ["out/series.json", "viewer/series.json", "series.json"]
OUT_JSON = "viewer/btc_series.json"

def _to_scalar_float(x):
    """리스트/딕셔너리/문자열 등이 섞여도 스칼라 float로 강제 변환."""
    if isinstance(x, (list, tuple, np.ndarray)):
        if len(x) == 0:
            return np.nan
        x = x[0]
    if isinstance(x, dict):
        for k in ("value", "v", "index", "close"):
            if k in x:
                x = x[k]
                break
        else:
            return np.nan
    if isinstance(x, str):
        s = x.strip().replace(",", "")
        try:
            return float(s)
        except Exception:
            return np.nan
    try:
        return float(x)
    except Exception:
        return np.nan

def load_bm20_series() -> pd.Series:
    """
    BM20 series.json을 로드해 (DatetimeIndex, float) Series로 반환.
    지원 형식:
      - records: [{"date":"YYYY-MM-DD","index":100.0}, ...]
      - pairs  : [["YYYY-MM-DD", 100.0], ...]
    """
    path = None
    for p in BM20_JSON_CANDIDATES:
        if os.path.exists(p):
            path = p
            break
    if not path:
        raise FileNotFoundError(f"BM20 series.json not found in {BM20_JSON_CANDIDATES}")

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list) and data and isinstance(data[0], dict) and "date" in data[0]:
        df = pd.DataFrame(data)
        date_col = "date"
        # 값 컬럼 추론
        cand = [c for c in df.columns if c.lower() in ("index", "value", "v", "close") and c != date_col]
        val_col = cand[0] if cand else [c for c in df.columns if c != date_col][0]
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df[val_col]  = df[val_col].map(_to_scalar_float)
        df = df.dropna(subset=[date_col, val_col])
        s = pd.Series(df[val_col].astype(float).values, index=df[date_col])
    else:
        df = pd.DataFrame(data, columns=["date", "index"])
        df["date"]  = pd.to_datetime(df["date"], errors="coerce")
        df["index"] = df["index"].map(_to_scalar_float)
        df = df.dropna(subset=["date", "index"])
        s = pd.Series(df["index"].astype(float).values, index=df["date"])

    s.index = pd.to_datetime(s.index).tz_localize(None).normalize()
    s = s.sort_index()
    if s.empty:
        raise ValueError("Loaded BM20 series is empty.")
    return s

def main():
    # 1) BM20 로드
    bm = load_bm20_series()
    d0 = bm.index.min().date().isoformat()

    # 2) BTC 종가 Series (반드시 1D Series 유지)
    btc_close = yf.download("BTC-USD", start=d0, progress=False, auto_adjust=True)["Close"].dropna()
    if isinstance(btc_close, pd.DataFrame):
        btc_close = btc_close.iloc[:, 0]   # 혹시라도 DF가 들어오면 Series로 강제
    btc_close.index = pd.to_datetime(btc_close.index).tz_localize(None).normalize()
    btc_close = btc_close.sort_index()

    if btc_close.empty:
        raise ValueError("BTC close series is empty from yfinance.")

    # 3) 100 기준 정규화 → BM20 인덱스에 맞춰 채움 (shape 문제/브로드캐스트 위험 없음)
    base = float(btc_close.iloc[0])
    btc_idx = (btc_close / base) * 100.0                  # 1D Series
    aligned = btc_idx.reindex(bm.index).astype("float64").ffill()

    # 4) 저장: [["YYYY-MM-DD", 123.45], ...]
    arr = [[d.strftime("%Y-%m-%d"), float(v)] for d, v in zip(aligned.index, aligned.values)]
    os.makedirs(os.path.dirname(OUT_JSON), exist_ok=True)
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(arr, f, ensure_ascii=False)
    print(f"[OK] wrote {OUT_JSON} ({arr[0][0]} → {arr[-1][0]}, {len(arr)} pts)")

if __name__ == "__main__":
    main()
