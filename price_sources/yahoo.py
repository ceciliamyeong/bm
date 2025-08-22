"""
Yahoo Finance price source for BM20.

get_price_usd(coin_id: str, day: Optional[date]) -> float
 - coin_id uses CoinGecko-style ids (e.g., 'bitcoin', 'ethereum').
 - day=None returns a recent price (last non-null close from hourly candles).
 - day=<date> returns the daily close in USD for that UTC date (best-effort with fallbacks).

Includes lightweight on-disk caching to reduce API calls in CI.
"""
from __future__ import annotations

import json
import os
import time
from datetime import date as _date, datetime, timedelta, timezone
from typing import Dict, Optional

import requests

# -------------------- Config --------------------
UA = {"User-Agent": "Mozilla/5.0 (BM20 bot)"}
BASE = "https://query1.finance.yahoo.com/v8/finance/chart/{}"
TIMEOUT = 15
RETRY = 3
BACKOFF = 0.6

CACHE_PATH = os.getenv("YAHOO_CACHE_PATH", os.path.join("out", "yahoo_cache.json"))
os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)

# CoinGecko id -> Yahoo Finance symbol
YH_SYMBOL: Dict[str, str] = {
    "bitcoin": "BTC-USD",
    "ethereum": "ETH-USD",
    "ripple": "XRP-USD",
    "tether": "USDT-USD",
    "binancecoin": "BNB-USD",
    "dogecoin": "DOGE-USD",
    "toncoin": "TON-USD",
    "sui": "SUI-USD",
    "solana": "SOL-USD",
    "cardano": "ADA-USD",
    "avalanche-2": "AVAX-USD",
    "polkadot": "DOT-USD",
    "polygon": "MATIC-USD",
    "chainlink": "LINK-USD",
    "litecoin": "LTC-USD",
    "cosmos": "ATOM-USD",
    "near": "NEAR-USD",
    "aptos": "APT-USD",
    "filecoin": "FIL-USD",
    "internet-computer": "ICP-USD",
}

# -------------------- Cache helpers --------------------
_DEF_CACHE: Dict[str, float] = {}
if os.path.exists(CACHE_PATH):
    try:
        with open(CACHE_PATH, "r", encoding="utf-8") as f:
            _DEF_CACHE.update(json.load(f))
    except Exception:
        _DEF_CACHE = {}


def _cache_get(key: str) -> Optional[float]:
    v = _DEF_CACHE.get(key)
    try:
        return float(v) if v is not None else None
    except Exception:
        return None


def _cache_put(key: str, val: float) -> None:
    try:
        _DEF_CACHE[key] = float(val)
        with open(CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump(_DEF_CACHE, f, ensure_ascii=False)
    except Exception:
        pass

# -------------------- HTTP helpers --------------------

def _get(url: str, params: dict) -> dict:
    last = None
    for i in range(RETRY):
        try:
            r = requests.get(url, params=params, headers=UA, timeout=TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            time.sleep(BACKOFF * (i + 1) + 0.2)
    raise last


def _epoch(d: Optional[_date]) -> int:
    if d is None:
        return int(time.time())
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())


def _yyyy_mm_dd_utc(ts: int) -> _date:
    return datetime.utcfromtimestamp(ts).date()


# -------------------- Public API --------------------

def get_price_usd(coin_id: str, day: Optional[_date]) -> float:
    """Fetches USD price for the given coin_id from Yahoo Finance.

    - day=None: returns a recent price using hourly candles from last ~5 days (last non-null close).
    - day=date: returns the close of that UTC day using 1d interval (nearest candle on that date).

    Raises ValueError/RuntimeError on mapping or data errors.
    """
    sym = YH_SYMBOL.get(coin_id)
    if not sym:
        raise ValueError(f"Yahoo symbol not mapped for coin_id={coin_id}")

    cache_key = f"{sym}:{day.isoformat() if day else 'now'}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return float(cached)

    if day is None:
        # recent hourly window; pick last non-null close
        params = {
            "period1": _epoch(datetime.utcnow().date() - timedelta(days=5)),
            "period2": _epoch(None),
            "interval": "1h",
        }
        j = _get(BASE.format(sym), params)
        res = j.get("chart", {}).get("result", [])
        if not res:
            raise RuntimeError(f"Yahoo chart empty for {sym}")
        closes = res[0]["indicators"]["quote"][0].get("close", [])
        for v in reversed(closes):
            if v is not None:
                _cache_put(cache_key, float(v))
                return float(v)
        raise RuntimeError(f"No recent close for {sym}")

    # day is specified → 1d candles spanning [day-2, day+2] to be safe
    start = day - timedelta(days=2)
    end = day + timedelta(days=2)
    params = {"period1": _epoch(start), "period2": _epoch(end), "interval": "1d"}
    j = _get(BASE.format(sym), params)
    res = j.get("chart", {}).get("result", [])
    if not res:
        raise RuntimeError(f"Yahoo chart empty for {sym}")
    block = res[0]
    ts = block.get("timestamp", [])
    quote = block.get("indicators", {}).get("quote", [{}])[0]
    closes = quote.get("close", [])

    # find candle that matches the UTC date exactly; else fallback to nearest prior non-null
    target = day
    candidate = None
    for t, v in zip(ts, closes):
        if v is None:
            continue
        d_utc = _yyyy_mm_dd_utc(int(t))
        if d_utc == target:
            candidate = float(v)
            break
        if d_utc < target:
            candidate = float(v)  # keep latest prior as fallback
    if candidate is None:
        # as a last resort, use the last non-null close in window
        for v in reversed(closes):
            if v is not None:
                candidate = float(v); break
    if candidate is None:
        raise RuntimeError(f"No close data for {sym} around {day}")

    _cache_put(cache_key, candidate)
    return candidate
