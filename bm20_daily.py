#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BM20 Daily Automation — Production Version (A안)
2025-08-12

- 뉴스 → 퍼포먼스(Top10/Top10) → 거래량 Top3 → 김치프리미엄
- 김치프리미엄: Upbit × Coinbase × exchangerate.host
- 펀딩비: Binance + Bybit + 재시도 + 캐시 폴백
- 내부 BM20 데이터: get_today_snapshot()에 연결
"""
from __future__ import annotations
import os, math, json, time, textwrap, datetime as dt
from dataclasses import dataclass
from typing import Optional, List, Tuple
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import requests

try:
    from zoneinfo import ZoneInfo
except Exception:
    from pytz import timezone as ZoneInfo

KST = ZoneInfo("Asia/Seoul")
NOW = dt.datetime.now(KST)
TODAY = NOW.date()
DATE_STR = TODAY.strftime("%Y-%m-%d")
OUTPUT_ROOT = os.environ.get("BM20_OUTPUT_ROOT", "./out")
DRIVE_FOLDER_ID = os.environ.get("BM20_DRIVE_FOLDER_ID", "")
DAILY_DIR = os.path.join(OUTPUT_ROOT, DATE_STR)
os.makedirs(DAILY_DIR, exist_ok=True)

@dataclass
class Bm20Snapshot:
    date: dt.date
    index_level: float
    index_chg_pct: float
    mcap_total: float
    turnover_usd: float
    etf_flow_usd: Optional[float] = None
    cex_netflow_usd: Optional[float] = None
    top_movers: Optional[pd.DataFrame] = None
    contributions: Optional[pd.DataFrame] = None
    volume_growth: Optional[pd.DataFrame] = None

def safe_get(d: dict, *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur

# === 김치프리미엄 ===
# Upbit 5분봉 BTC/KRW
def fetch_upbit_minutes(market: str = "BTC-KRW", unit: int = 5, count: int = 288) -> pd.DataFrame:
    url = f"https://api.upbit.com/v1/candles/minutes/{unit}"
    r = requests.get(url, params={"market": market, "count": count}, timeout=12)
    r.raise_for_status()
    df = pd.DataFrame(r.json())
    df["timestamp"] = pd.to_datetime(df["candle_date_time_kst"]).dt.tz_localize("Asia/Seoul")
    return df.sort_values("timestamp").set_index("timestamp")["trade_price"].to_frame("close")

# Coinbase 5분봉 BTC/USD
def fetch_coinbase_candles(product: str = "BTC-USD", granularity: int = 300, hours: int = 24) -> pd.DataFrame:
    url = f"https://api.exchange.coinbase.com/products/{product}/candles"
    end = dt.datetime.utcnow()
    start = end - dt.timedelta(hours=hours)
    params = {"start": start.isoformat(), "end": end.isoformat(), "granularity": granularity}
    r = requests.get(url, params=params, timeout=12)
    r.raise_for_status()
    arr = r.json()
    df = pd.DataFrame(arr, columns=["time","low","high","open","close","volume"])
    df["timestamp"] = pd.to_datetime(df["time"], unit="s", utc=True).dt.tz_convert("Asia/Seoul")
    return df.sort_values("timestamp").set_index("timestamp")["close"].to_frame("close")

# USD/KRW
def fetch_usdkrw_latest() -> float:
    r = requests.get("https://api.exchangerate.host/latest", params={"base":"USD","symbols":"KRW"}, timeout=10)
    r.raise_for_status()
    return float(safe_get(r.json(), "rates", "KRW", default=1320.0))

def compute_kimchi_premium_series() -> Tuple[pd.DataFrame, float]:
    up = fetch_upbit_minutes()
    cb = fetch_coinbase_candles()
    usdkrw = fetch_usdkrw_latest()
    df = pd.concat({"btc_krw": up["close"], "btc_usd": cb["close"]}, axis=1).ffill().dropna()
    prem = (df["btc_krw"] / (df["btc_usd"] * usdkrw) - 1) * 100
    out = pd.DataFrame({"kimchi_premium_pct": prem})
    return out, float(out.iloc[-1,0])

def save_kimchi_premium_chart(prem: pd.DataFrame, out_dir: str) -> Tuple[str,str]:
    png_path = os.path.join(out_dir, f"kimchi_premium_{DATE_STR}.png")
    pdf_path = os.path.join(out_dir, f"kimchi_premium_{DATE_STR}.pdf")
    for path in [png_path, pdf_path]:
        plt.figure(figsize=(9,4.5))
        plt.plot(prem.index, prem["kimchi_premium_pct"])
        plt.axhline(0, lw=1)
        plt.title(f"Kimchi Premium — {DATE_STR}")
        plt.xlabel("Time (KST)"); plt.ylabel("% vs. offshore")
        plt.tight_layout()
        if path.endswith('.png'):
            plt.savefig(path, dpi=160)
        else:
            with PdfPages(path) as pdf: pdf.savefig()
        plt.close()
    return png_path, pdf_path

# === 펀딩비 ===

def _retry(fn, *, attempts:int=3, base_sleep:float=0.6):
    def wrapper(*a, **k):
        last=None
        for i in range(attempts):
            try: return fn(*a, **k)
            except Exception as e:
                last=e; time.sleep(base_sleep*(2**i))
        if last: raise last
    return wrapper

@_retry
def fetch_binance_funding(symbol: str = "BTCUSDT") -> Optional[float]:
    url = "https://fapi.binance.com/fapi/v1/fundingRate"
    r = requests.get(url, params={"symbol":symbol, "limit":1}, timeout=8)
    r.raise_for_status()
    js = r.json()
    return float(js[0]["fundingRate"]) * 100.0 if js else None

@_retry
def fetch_bybit_funding(symbol: str = "BTCUSDT") -> Optional[float]:
    url = "https://api.bybit.com/v5/market/funding/history"
    r = requests.get(url, params={"category":"linear","symbol":symbol,"limit":1}, timeout=8)
    r.raise_for_status()
    rate = safe_get(r.json(), "result", "list", 0, "fundingRate")
    return float(rate) * 100.0 if rate is not None else None

CACHE_DIR = os.path.join(OUTPUT_ROOT, "cache"); os.makedirs(CACHE_DIR, exist_ok=True)
CACHE_FUND = os.path.join(CACHE_DIR, "funding.json")

def load_cached_funding():
    try:
        with open(CACHE_FUND, 'r', encoding='utf-8') as f: return json.load(f)
    except: return {}

def save_cached_funding(d: dict):
    try:
        with open(CACHE_FUND, 'w', encoding='utf-8') as f: json.dump(d, f, ensure_ascii=False)
    except: pass

def get_funding_snapshot() -> dict:
    out = {"binance":{}, "bybit":{}}
    out["binance"]["BTC"] = fetch_binance_funding("BTCUSDT")
    out["binance"]["ETH"] = fetch_binance_funding("ETHUSDT")
    out["bybit"]["BTC"] = fetch_bybit_funding("BTCUSDT")
    out["bybit"]["ETH"] = fetch_bybit_funding("ETHUSDT")
    cached = load_cached_funding()
    for ex in ["binance","bybit"]:
        for sym in ["BTC","ETH"]:
            out[ex][sym] = out[ex][sym] or cached.get(ex,{}).get(sym)
    save_cached_funding(out)
    return out

# === 내부 BM20 데이터 연결 ===

def get_today_snapshot() -> Bm20Snapshot:
    # 여기에 내부 API/DB 호출 로직을 붙여서 데이터 반환
    data = fetch_bm20_data_from_internal_api()
    movers = fetch_top_movers_from_internal_api()
    contrib = fetch_contributions_from_internal_api()
    vol_top3 = fetch_volume_growth_top3_from_internal_api()
    return Bm20Snapshot(
        date=TODAY,
        index_level=data['index_level'],
        index_chg_pct=data['index_chg_pct'],
        mcap_total=data['mcap_total'],
        turnover_usd=data['turnover_usd'],
        etf_flow_usd=data.get('etf_flow_usd'),
        cex_netflow_usd=data.get('cex_netflow_usd'),
        top_movers=movers,
        contributions=contrib,
        volume_growth=vol_top3,
    )

# === 나머지: 뉴스 문장, 퍼포먼스, 거래량 Top3, PDF 생성 ===
# (이전 버전 동일 — 그대로 복사하여 사용)

