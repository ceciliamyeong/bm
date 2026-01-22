"""
make_coin_prices_usd.py
- Output: out/history/coin_prices_usd.csv
- Columns: date, BTC, ETH, ... (USD daily close)
- Source: CoinGecko (primary) with optional yfinance fallback
"""

from __future__ import annotations

import os
import time
import math
import json
from datetime import datetime, timezone
from typing import Dict, List, Optional

import pandas as pd
import requests

OUT_DIR = "out/history"
OUT_CSV = os.path.join(OUT_DIR, "coin_prices_usd.csv")

# === 1) BM20 유니버스(예시) ===
# 너희 BM20 최종 지침( BTC/ETH/XRP/USDT/BNB + 나머지 15개, APT 제외 SUI 포함 ) 기준으로,
# 실제 사용 코인에 맞게 여기만 정확히 맞춰주면 됨.
#
# 주의: 이 파일은 "가격 패널"이 목적이므로 스테이블 포함 여부는 자유.
# (ALT-RS 계산할 때는 자동으로 스테이블 제외 가능)
#
# 아래는 '예시'이므로, 너희 실제 BM20 컴포넌트와 1:1로 맞춰줘.
TICKERS = [
    "BTC","ETH","XRP","USDT","BNB",
    "SOL","DOGE","TON","SUI",
    "ADA","TRX","DOT","AVAX","LINK","MATIC","LTC","BCH","ATOM","UNI","XLM"
]

# CoinGecko ID 매핑 (필수)
# - 각 티커가 CoinGecko에서 어떤 coin id인지 매핑해줘야 함.
# - 아래도 예시 포함. (필요 시 수정/추가)
CG_ID: Dict[str, str] = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "XRP": "ripple",
    "USDT": "tether",
    "BNB": "binancecoin",
    "SOL": "solana",
    "DOGE": "dogecoin",
    "TON": "the-open-network",
    "SUI": "sui",
    "ADA": "cardano",
    "TRX": "tron",
    "DOT": "polkadot",
    "AVAX": "avalanche-2",
    "LINK": "chainlink",
    "MATIC": "matic-network",   # (Polygon 티커 변화 있을 수 있어 확인 권장)
    "LTC": "litecoin",
    "BCH": "bitcoin-cash",
    "ATOM": "cosmos",
    "UNI": "uniswap",
    "XLM": "stellar",
}

# (옵션) Yahoo Finance 티커 폴백용 매핑: yfinance가 설치되어 있으면 사용 가능
# 예: "BTC-USD", "ETH-USD" ...
YF_TICKER: Dict[str, str] = {t: f"{t}-USD" for t in TICKERS if t not in ["USDT"]}  # USDT-USD는 종종 애매

def to_unix(ts: datetime) -> int:
    return int(ts.replace(tzinfo=timezone.utc).timestamp())

def cg_market_chart_range(coin_id: str, start: datetime, end: datetime) -> pd.Series:
    """
    CoinGecko range endpoint returns [timestamp_ms, price] points.
    We resample to daily and take last (close).
    """
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart/range"
    params = {
        "vs_currency": "usd",
        "from": to_unix(start),
        "to": to_unix(end),
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    prices = data.get("prices", [])
    if not prices:
        raise ValueError(f"No price data for {coin_id}")

    s = pd.Series(
        data=[p[1] for p in prices],
        index=pd.to_datetime([p[0] for p in prices], unit="ms", utc=True),
        name=coin_id,
    )
    # 일봉 종가(UTC 기준): 하루 마지막 값
    daily = s.resample("1D").last()
    daily.index = daily.index.date
    return daily

def yf_daily_close(yf_symbol: str, start: str, end: str) -> pd.Series:
    import yfinance as yf
    df = yf.download(yf_symbol, start=start, end=end, interval="1d", auto_adjust=False, progress=False)
    if df.empty:
        raise ValueError(f"Empty yfinance data for {yf_symbol}")
    s = df["Close"].copy()
    s.index = pd.to_datetime(s.index).date
    return s

def main(
    start_date: str = "2018-01-01",
    end_date: Optional[str] = None,
    sleep_sec: float = 1.2,
    use_yfinance_fallback: bool = True,
):
    os.makedirs(OUT_DIR, exist_ok=True)

    start = datetime.fromisoformat(start_date)
    end = datetime.now(timezone.utc) if end_date is None else datetime.fromisoformat(end_date)

    # CoinGecko는 요청 제한이 있으니 코인당 sleep 권장
    series_map: Dict[str, pd.Series] = {}

    for t in TICKERS:
        if t not in CG_ID:
            raise KeyError(f"Missing CoinGecko id mapping for ticker: {t}")

        coin_id = CG_ID[t]
        try:
            s = cg_market_chart_range(coin_id, start, end)
            series_map[t] = s
            print(f"[CG OK] {t} ({coin_id}) rows={len(s)}")
        except Exception as e:
            print(f"[CG FAIL] {t} ({coin_id}) err={e}")

            if not use_yfinance_fallback:
                raise

            # yfinance fallback
            try:
                import importlib
                importlib.import_module("yfinance")
            except Exception:
                raise RuntimeError("yfinance not installed. pip install yfinance") from e

            yf_sym = YF_TICKER.get(t)
            if not yf_sym:
                raise RuntimeError(f"No yfinance mapping for {t}")

            s = yf_daily_close(yf_sym, start_date, end.date().isoformat())
            series_map[t] = s
            print(f"[YF OK] {t} ({yf_sym}) rows={len(s)}")

        time.sleep(sleep_sec)

    # wide table
    df = pd.DataFrame(series_map)
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()

    # CSV output
    out = df.copy()
    out.insert(0, "date", out.index.date.astype(str))
    out.to_csv(OUT_CSV, index=False)

    # metadata
    meta = {
        "start_date": start_date,
        "end_date": end.date().isoformat(),
        "tickers": TICKERS,
        "source": "CoinGecko(range)/yfinance(fallback)",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
    }
    with open(os.path.join(OUT_DIR, "coin_prices_usd.meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print(f"\nSaved: {OUT_CSV}")
    print(f"Saved: {os.path.join(OUT_DIR, 'coin_prices_usd.meta.json')}")

if __name__ == "__main__":
    main()
