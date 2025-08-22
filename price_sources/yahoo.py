from __future__ import annotations
from datetime import datetime, date, time, timedelta, timezone
from typing import Optional, Dict
import csv, pathlib
import yfinance as yf

_SYMBOLS: Dict[str, tuple[str, Optional[str]]] = {}
_CACHE: Dict[tuple[str, str], float] = {}  # (symbol, yyyymmdd) -> price

def _load_symbols(path: str | pathlib.Path = "data/yahoo_symbols.csv") -> None:
    global _SYMBOLS
    if _SYMBOLS:
        return
    p = pathlib.Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Yahoo symbol mapping not found: {p}")
    with p.open() as f:
        for row in csv.DictReader(f):
            _SYMBOLS[row["coin_id"]] = (row["symbol"], (row.get("alt_symbol") or "") or None)

def _cache_get(symbol: str, day: date) -> float | None:
    return _CACHE.get((symbol, day.strftime("%Y%m%d")))

def _cache_set(symbol: str, day: date, price: float) -> None:
    _CACHE[(symbol, day.strftime("%Y%m%d"))] = price

def _fetch_close(symbol: str, on_date: date) -> float:
    c = _cache_get(symbol, on_date)
    if c is not None:
        return c
    tk = yf.Ticker(symbol)
    start = datetime.combine(on_date, time.min).replace(tzinfo=timezone.utc)
    end = start + timedelta(days=2)  # 주말/휴장 완충
    hist = tk.history(start=start, end=end, interval="1d", auto_adjust=False)
    if hist.empty:
        raise RuntimeError(f"[Yahoo] empty history for {symbol} on {on_date.isoformat()}")
    price = float(hist["Close"].iloc[0])
    _cache_set(symbol, on_date, price)
    return price

def _fetch_spot(symbol: str) -> float:
    today = date.today()
    c = _cache_get(symbol, today)
    if c is not None:
        return c
    tk = yf.Ticker(symbol)
    try:
        price = float(tk.fast_info.last_price)
    except Exception:
        hist = tk.history(period="2d", interval="1d", auto_adjust=False)
        if hist.empty:
            raise RuntimeError(f"[Yahoo] empty spot for {symbol}")
        price = float(hist["Close"].iloc[-1])
    _cache_set(symbol, today, price)
    return price

def get_price_usd(coin_id: str, on_date: Optional[date] = None) -> float:
    _load_symbols()
    if coin_id not in _SYMBOLS:
        raise KeyError(f"Yahoo symbol not mapped: {coin_id}")
    symbol, alt = _SYMBOLS[coin_id]
    try:
        return _fetch_close(symbol, on_date) if on_date else _fetch_spot(symbol)
    except Exception:
        if alt:
            return _fetch_close(alt, on_date) if on_date else _fetch_spot(alt)
        raise
