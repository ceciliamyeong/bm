#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Kimchi Premium Top10 (Market-based)
- Definition (market kimchi):
  kimchi_market_pct = (Upbit_KRW - Yahoo_USD * Yahoo_USDKRW) / (Yahoo_USD * Yahoo_USDKRW) * 100

- Also stores:
  - USDKRW market (Yahoo: KRW=X)
  - USDTKRW domestic (Upbit: KRW-USDT)  # 참고용
  - FX premium pct (domestic vs market)

Outputs:
  out/history/
    - kimchi_top10_latest.json
    - kimchi_top10_snapshots.json
"""

import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

import requests

KST = timezone(timedelta(hours=9))

BASE_DIR = Path(__file__).resolve().parent.parent
HIST_DIR = BASE_DIR / "out" / "history"
HIST_DIR.mkdir(parents=True, exist_ok=True)

OUT_LATEST = HIST_DIR / "kimchi_top10_latest.json"
OUT_SNAPSHOTS = HIST_DIR / "kimchi_top10_snapshots.json"
MAX_SNAPSHOTS = 400  # 넉넉히

UPBIT_TICKER = "https://api.upbit.com/v1/ticker"
YAHOO_QUOTE = "https://query1.finance.yahoo.com/v7/finance/quote"

# 너희가 보고 싶은 Top10을 여기서 고정 (KRW 마켓 존재하는 것 위주로)
TOP10 = ["BTC", "ETH", "XRP", "SOL", "BNB", "ADA", "DOGE", "TRX", "TON", "SUI"]

# Yahoo 심볼 매핑 (필요시 여기만 수정)
# 기본은 "{SYM}-USD"로 가고, 예외만 따로 둠.
YAHOO_MAP = {
    "BTC": "BTC-USD",
    "ETH": "ETH-USD",
    "XRP": "XRP-USD",
    "SOL": "SOL-USD",
    "BNB": "BNB-USD",
    "ADA": "ADA-USD",
    "DOGE": "DOGE-USD",
    "TRX": "TRX-USD",
    "TON": "TON-USD",
    "SUI": "SUI-USD",
}

def now_kst() -> datetime:
    return datetime.now(tz=KST)

def safe_read_json(path: Path):
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None

def write_json(path: Path, obj):
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

def http_get_json(url: str, params=None, timeout: int = 20) -> Any:
    last_err = None
    for _ in range(3):
        try:
            r = requests.get(url, params=params, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
            if 200 <= r.status_code < 300:
                return r.json()
            last_err = RuntimeError(f"{r.status_code} {r.text[:200]}")
            time.sleep(1)
        except Exception as e:
            last_err = e
            time.sleep(1)
    raise RuntimeError(f"Failed request: {url} ({last_err})")

def upbit_prices(markets: List[str]) -> Dict[str, float]:
    # Upbit는 markets를 콤마로 한 번에 받을 수 있음
    j = http_get_json(UPBIT_TICKER, {"markets": ",".join(markets)})
    out: Dict[str, float] = {}
    if isinstance(j, list):
        for row in j:
            m = row.get("market")
            p = row.get("trade_price")
            if m and p is not None:
                out[m] = float(p)
    return out

def yahoo_quotes(symbols: List[str]) -> Dict[str, float]:
    # Yahoo는 여러 심볼을 한 번에 가능
    j = http_get_json(YAHOO_QUOTE, {"symbols": ",".join(symbols)})
    result = (((j or {}).get("quoteResponse") or {}).get("result") or [])
    out: Dict[str, float] = {}
    for row in result:
        sym = row.get("symbol")
        px = row.get("regularMarketPrice")
        if sym and px is not None:
            out[sym] = float(px)
    return out

def kimchi_market_pct(upbit_krw: float, yahoo_usd: float, usdkrw_market: float) -> Optional[float]:
    fair = yahoo_usd * usdkrw_market
    if fair <= 0:
        return None
    return (upbit_krw - fair) / fair * 100.0

def run():
    ts = now_kst()
    ts_iso = ts.strftime("%Y-%m-%dT%H:%M:%S%z")
    ts_label = ts.strftime("%m/%d %H:%M KST")

    # 1) FX: Yahoo USDKRW (KRW=X)
    # 2) Global coin USD: Yahoo (BTC-USD 등)
    yahoo_syms = ["KRW=X"] + [YAHOO_MAP.get(c, f"{c}-USD") for c in TOP10]
    yq = yahoo_quotes(yahoo_syms)

    usdkrw_market = yq.get("KRW=X")
    if not usdkrw_market:
        latest_err = {
            "schema": "kimchi_top10_v1",
            "timestamp_kst": ts_iso,
            "timestamp_label": ts_label,
            "status": "error",
            "error": "Yahoo FX (KRW=X) missing",
        }
        write_json(OUT_LATEST, latest_err)
        print("[WARN] Yahoo KRW=X missing -> wrote error latest")
        return

    # 3) Domestic prices: Upbit KRW-COIN + KRW-USDT(참고용)
    upbit_markets = [f"KRW-{c}" for c in TOP10] + ["KRW-USDT"]
    up = upbit_prices(upbit_markets)

    usdtkrw_domestic = up.get("KRW-USDT")
    fx_premium_pct = None
    if usdtkrw_domestic and usdkrw_market:
        fx_premium_pct = (usdtkrw_domestic - usdkrw_market) / usdkrw_market * 100.0

    # prev snapshot for delta
    history = safe_read_json(OUT_SNAPSHOTS)
    if not isinstance(history, list):
        history = []
    prev = history[-1] if history else {}
    prev_map = {}
    for row in (prev.get("coins") or []):
        prev_map[row.get("coin")] = row

    coins_out = []
    for coin in TOP10:
        mkt = f"KRW-{coin}"
        up_krw = up.get(mkt)
        ysym = YAHOO_MAP.get(coin, f"{coin}-USD")
        y_usd = yq.get(ysym)

        prem = None
        if up_krw and y_usd:
            prem = kimchi_market_pct(up_krw, y_usd, usdkrw_market)

        # delta in percentage points vs prev
        delta_pp = None
        prev_row = prev_map.get(coin) or {}
        prev_p = prev_row.get("premium_market_pct")
        if prem is not None and isinstance(prev_p, (int, float)):
            delta_pp = prem - float(prev_p)

        coins_out.append({
            "coin": coin,
            "upbit_krw": up_krw if up_krw is not None else None,
            "yahoo_usd": y_usd if y_usd is not None else None,
            "premium_market_pct": round(prem, 4) if prem is not None else None,
            "delta_pp": round(delta_pp, 4) if delta_pp is not None else None,
            "yahoo_symbol": ysym,
            "upbit_market": mkt,
        })

    # 김프 높은 순으로 정렬(없으면 아래로)
    coins_sorted = sorted(
        coins_out,
        key=lambda x: (-1e18 if x["premium_market_pct"] is None else -x["premium_market_pct"])
    )

    latest = {
        "schema": "kimchi_top10_v1",
        "status": "ok",
        "timestamp_kst": ts_iso,
        "timestamp_label": ts_label,
        "fx": {
            "usdkrw_market": round(float(usdkrw_market), 4),
            "usdtkrw_domestic": round(float(usdtkrw_domestic), 4) if usdtkrw_domestic else None,
            "fx_premium_pct": round(float(fx_premium_pct), 4) if fx_premium_pct is not None else None,
            "sources": {"market": "yahoo(KRW=X)", "domestic": "upbit(KRW-USDT)"},
        },
        "definition": {
            "premium_market_pct": "(Upbit_KRW - Yahoo_USD*Yahoo_USDKRW) / (Yahoo_USD*Yahoo_USDKRW) * 100"
        },
        "coins": coins_sorted,
    }

    # append history
    history = [x for x in history if x.get("timestamp_kst") != ts_iso]
    history.append(latest)
    history = history[-MAX_SNAPSHOTS:]

    write_json(OUT_LATEST, latest)
    write_json(OUT_SNAPSHOTS, history)

    print("[OK] Kimchi Top10 saved:", ts_label)
    print("     USDKRW(market):", usdkrw_market, "USDTKRW(domestic):", usdtkrw_domestic)

if __name__ == "__main__":
    run()
