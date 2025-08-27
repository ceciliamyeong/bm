# scripts/util_price.py
import os, json, time, datetime as dt
from pathlib import Path
from typing import List, Tuple
import pandas as pd
import requests

# --- Yahoo Finance only helpers ---
def _yf_close(symbol: str) -> pd.DataFrame:
    import yfinance as yf
    hist = yf.Ticker(symbol).history(period="max", interval="1d")["Close"].dropna()
    df = pd.DataFrame({"date": hist.index.date, "close": hist.values})
    return df.reset_index(drop=True)

def load_btc_close(start_date: str = "2017-01-01", vs: str = "usd") -> pd.DataFrame:
    sym = "BTC-USD" if vs.lower() == "usd" else "BTC-KRW"
    df = _yf_close(sym)
    return df[df["date"] >= pd.to_datetime(start_date).date()].reset_index(drop=True)

def load_eth_close(start_date: str = "2017-01-01", vs: str = "usd") -> pd.DataFrame:
    sym = "ETH-USD" if vs.lower() == "usd" else "ETH-KRW"
    df = _yf_close(sym)
    return df[df["date"] >= pd.to_datetime(start_date).date()].reset_index(drop=True)

def load_bm20_index_history(csv_path: str = "out/history/bm20_index_history.csv") -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    date_col = next(c for c in df.columns if c.lower() == "date")
    idx_col  = next(c for c in df.columns if c.lower() in ("index", "level", "bm20_index", "bm20", "index_level"))
    df = df[[date_col, idx_col]].rename(columns={date_col: "date", idx_col: "bm20"})
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df.sort_values("date").reset_index(drop=True)

def reindex_business_days(df: pd.DataFrame) -> pd.DataFrame:
    cal = pd.date_range(start=pd.to_datetime(df["date"]).min(), end=pd.to_datetime(df["date"]).max(), freq="D")
    return pd.DataFrame({"date": cal.date})
