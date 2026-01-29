#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Kimchi Premium Top10 (Yahoo-only, v8 chart)
- Fix: Avoid Yahoo v7 quote (401 Unauthorized). Use v8 finance/chart instead.
- Market FX: Yahoo KRW=X
- Global coin USD: Yahoo {COIN}-USD (via v8 chart)
- Domestic coin KRW: Upbit KRW-{COIN}
- Domestic USDTKRW (context): Upbit KRW-USDT
- Output:
  out/history/kimchi_top10_latest.json
  out/history/kimchi_top10_snapshots.json

Definition (market kimchi):
  premium_market_pct = (Upbit_KRW - Yahoo_USD * Yahoo_USDKRW) / (Yahoo_USD * Yahoo_USDKRW) * 100

Also:
  fx_premium_pct = (Upbit_KRWUSDT - Yahoo_USDKRW) / Yahoo_USDKRW * 100
"""

import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional

import requests

KST = timezone(timedelta(hours=9))

BASE_DIR = Path(__file__).resolve().parent.parent
HIST_DIR = BASE_DIR / "out" / "history"
HIST_DIR.mkdir(parents=True, exist_ok=True)

OUT_LATEST = HIST_DIR / "kimchi_top10_latest.json"
OUT_SNAPSHOTS = HIST_DIR / "kimchi_top10_snapshots.json"
MAX_SNAPSHOTS = 400

UPBIT_TICKER = "https://api.upbit.com/v1/ticker"
YAHOO_CHART = "https://query1.finance.yahoo.com/v8/finance/chart"

# Top10: 너희가 보고 싶은 코인(Upbit KRW 마켓 있는 것 위주)
TOP10 = ["BTC", "ETH", "XRP", "SOL", "LTC", "ADA", "DOGE", "TRX", "TON", "SUI"]

# Yahoo 심볼 매핑 (예외 있으면 여기서만 수정)
YAHOO_MAP = {c: f"{c}-USD" for c in TOP10}

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
    j = http_get_json(UPBIT_TICKER, {"markets": ",".join(markets)})
    out: Dict[str, float] = {}
    if isinstance(j, list):
        for row in j:
            m = row.get("market")
            p = row.get("trade_price")
            if m and p is not None:
                out[m] = float(p)
    return out

def yahoo_chart_last(symbol: str) -> Optional[float]:
    """
    Get last price using Yahoo v8 finance/chart endpoint.
    - For FX: "KRW=X"
    - For coins: "BTC-USD", "ETH-USD", ...
    """
    url = f"{YAHOO_CHART}/{symbol}"
    j = http_get_json(url, {"range": "1d", "interval": "1m"})

    try:
        res = (((j or {}).get("chart") or {}).get("result") or [])[0]
        meta = res.get("meta") or {}

        # 1) meta.regularMarketPrice (가장 깔끔)
        px = meta.get("regularMarketPrice")
        if px is not None:
            px = float(px)
            if px > 0:
                return px

        # 2) indicators.quote[0].close 마지막 유효값
        closes = (((res.get("indicators") or {}).get("quote") or [])[0].get("close") or [])
        closes = [c for c in closes if c is not None]
        if closes:
            px = float(closes[-1])
            if px > 0:
                return px
    except Exception:
        return None

    return None

def kimchi_market_pct(upbit_krw: float, yahoo_usd: float, usdkrw_market: float) -> Optional[float]:
    fair = yahoo_usd * usdkrw_market
    if fair <= 0:
        return None
    return (upbit_krw - fair) / fair * 100.0

def run():
    ts = now_kst()
    ts_iso = ts.strftime("%Y-%m-%dT%H:%M:%S%z")
    ts_label = ts.strftime("%m/%d %H:%M KST")

    # --- Yahoo FX (market) ---
    usdkrw_market = yahoo_chart_last("KRW=X")
    if not usdkrw_market:
        latest_err = {
            "schema": "kimchi_top10_yahoo_v1",
            "status": "error",
            "timestamp_kst": ts_iso,
            "timestamp_label": ts_label,
            "error": "Yahoo FX (KRW=X) missing via v8 chart",
        }
        # 에러여도 파일은 만들어서 pathspec 방지
        write_json(OUT_LATEST, latest_err)
        hist = safe_read_json(OUT_SNAPSHOTS)
        if not isinstance(hist, list):
            hist = []
        hist.append(latest_err)
        hist = hist[-MAX_SNAPSHOTS:]
        write_json(OUT_SNAPSHOTS, hist)
        print("[WARN] KRW=X missing -> wrote error latest + snapshots")
        return

    # --- Upbit domestic prices ---
    upbit_markets = [f"KRW-{c}" for c in TOP10] + ["KRW-USDT"]
    up = upbit_prices(upbit_markets)

    usdtkrw_domestic = up.get("KRW-USDT")
    fx_premium_pct = None
    if usdtkrw_domestic:
        fx_premium_pct = (usdtkrw_domestic - usdkrw_market) / usdkrw_market * 100.0

    # --- Yahoo global USD coin prices ---
    yahoo_usd_by_coin: Dict[str, Optional[float]] = {}
    for coin in TOP10:
        sym = YAHOO_MAP.get(coin, f"{coin}-USD")
        yahoo_usd_by_coin[coin] = yahoo_chart_last(sym)
        # 너무 빠른 연속 호출 방지(야후가 민감할 때가 있음)
        time.sleep(0.2)

    # prev snapshot for delta_pp
    history = safe_read_json(OUT_SNAPSHOTS)
    if not isinstance(history, list):
        history = []
    prev = history[-1] if history else {}
    prev_map = {r.get("coin"): r for r in (prev.get("coins") or [])}

    coins_out = []
    for coin in TOP10:
        up_krw = up.get(f"KRW-{coin}")
        y_usd = yahoo_usd_by_coin.get(coin)

        prem = None
        if up_krw is not None and y_usd is not None:
            prem = kimchi_market_pct(up_krw, y_usd, usdkrw_market)

        delta_pp = None
        prev_p = (prev_map.get(coin) or {}).get("premium_market_pct")
        if prem is not None and isinstance(prev_p, (int, float)):
            delta_pp = prem - float(prev_p)

        coins_out.append({
            "coin": coin,
            "upbit_market": f"KRW-{coin}",
            "yahoo_symbol": YAHOO_MAP.get(coin, f"{coin}-USD"),
            "upbit_krw": up_krw if up_krw is not None else None,
            "yahoo_usd": y_usd if y_usd is not None else None,
            "premium_market_pct": round(prem, 4) if prem is not None else None,
            "delta_pp": round(delta_pp, 4) if delta_pp is not None else None,
        })

    # 김프 높은 순 정렬 (None은 아래로)
    coins_sorted = sorted(
        coins_out,
        key=lambda x: (-1e18 if x["premium_market_pct"] is None else -x["premium_market_pct"])
    )

    latest = {
        "schema": "kimchi_top10_yahoo_v1",
        "status": "ok",
        "timestamp_kst": ts_iso,
        "timestamp_label": ts_label,
        "fx": {
            "usdkrw_market": round(float(usdkrw_market), 4),
            "usdtkrw_domestic": round(float(usdtkrw_domestic), 4) if usdtkrw_domestic else None,
            "fx_premium_pct": round(float(fx_premium_pct), 4) if fx_premium_pct is not None else None,
            "sources": {"market": "yahoo(KRW=X, v8 chart)", "domestic": "upbit(KRW-USDT)"},
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

    print("[OK] Kimchi Top10 (Yahoo v8) saved:", ts_label)
    print("     USDKRW(market):", usdkrw_market, "USDTKRW(domestic):", usdtkrw_domestic)

if __name__ == "__main__":
    run()
