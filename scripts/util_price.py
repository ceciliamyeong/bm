# scripts/util_price.py
import os, json, time, datetime as dt
from pathlib import Path
from typing import List, Tuple
import pandas as pd
import requests

CACHE_DIR = Path("cache"); CACHE_DIR.mkdir(exist_ok=True)

def _load_cache(path: Path, ttl_sec: int = 24*3600):
    if path.exists():
        if time.time() - path.stat().st_mtime <= ttl_sec:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    return None

def _save_cache(path: Path, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)

def coingecko_ohlc_daily(coin_id: str, vs: str="usd", days: str="max") -> pd.DataFrame:
    """
    CoinGecko market_chart API (daily close 근사) 사용.
    """
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
    params = {"vs_currency": vs, "days": days, "interval": "daily"}
    cache_path = CACHE_DIR / f"cg_{coin_id}_{vs}_{days}.json"
    j = _load_cache(cache_path, ttl_sec=12*3600)
    if j is None:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        j = r.json()
        _save_cache(cache_path, j)

    # prices: [[ms, price], ...]
    rows = [(dt.datetime.utcfromtimestamp(p[0]/1000).date(), float(p[1])) for p in j.get("prices", [])]
    df = pd.DataFrame(rows, columns=["date", "close"])
    df = df.drop_duplicates("date").sort_values("date").reset_index(drop=True)
    return df

def load_btc_close(start_date: str="2017-01-01", vs: str="usd") -> pd.DataFrame:
    return load_cg_close("bitcoin", start_date, vs)

def load_eth_close(start_date: str="2017-01-01", vs: str="usd") -> pd.DataFrame:
    return load_cg_close("ethereum", start_date, vs)

def load_bm20_index_history(csv_path: str="out/history/bm20_index_history.csv") -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    # 기대 스키마: date, index (혹은 level) 형태
    # 컬럼명 정규화
    cols = {c.lower(): c for c in df.columns}
    # 우선 date/ index 추론
    date_col = next(k for k in df.columns if k.lower() == "date")
    idx_col  = next(k for k in df.columns if k.lower() in ("index","level","bm20_index","bm20"))
    df = df[[date_col, idx_col]].rename(columns={date_col:"date", idx_col:"bm20"})
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df.sort_values("date").reset_index(drop=True)

def reindex_business_days(df: pd.DataFrame) -> pd.DataFrame:
    s = pd.Series(1, index=pd.to_datetime(df["date"]))
    cal = pd.date_range(start=s.index.min(), end=s.index.max(), freq="D")
    out = pd.DataFrame({"date": cal.date})
    return out
