#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""smart_kimchi_8h.py

Smart Kimchi Index v2 (8h snapshots)

What changed vs v1
- Add XRP
- Remove fixed-weight COMBO (no more 0.5/0.3/0.2)
- Use real-time-ish FX (USD/KRW) for the conversion instead of KRW-USDT
- Add ΔKimchi (%p) and Driver Share (%) to explain "who moved the Kimchi" (BTC vs ETH vs XRP)

Kimchi Premium definition (FX-neutral)
  premium = (KRW_domestic - (USDT_global * USDKRW)) / (USDT_global * USDKRW) * 100

Notes
- We still fetch Upbit KRW-USDT, but ONLY as a diagnostic (local stablecoin premium hint).

Outputs
  out/history/
    ├─ kimchi_latest.json
    └─ kimchi_snapshots.json
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

# input from your existing pipeline
KRW_LATEST_JSON = HIST_DIR / "krw_24h_latest.json"

# outputs
KIMCHI_LATEST_JSON = HIST_DIR / "kimchi_latest.json"
KIMCHI_SNAPSHOTS_JSON = HIST_DIR / "kimchi_snapshots.json"
MAX_SNAPSHOTS = 270

# --------- endpoints ----------
UPBIT_TICKER = "https://api.upbit.com/v1/ticker"  # markets=KRW-BTC

# Binance endpoints: api.binance.com may return 451 on GitHub Actions.
# Use binance.vision as fallback (same response format).
BINANCE_TICKER_URLS = [
    "https://api.binance.com/api/v3/ticker/price",
    "https://data-api.binance.vision/api/v3/ticker/price",
]

# Real-time-ish FX (no key)
FX_USDKRW_URL = "https://api.exchangerate.host/latest"  # base=USD&symbols=KRW

# Fallbacks (last-resort)
USDKRW_FALLBACK = 1450.0

# Local cache (so charts don't break when FX API is temporarily unavailable)
FX_CACHE_JSON = HIST_DIR / "fx_usdkrw_latest.json"


def now_kst() -> datetime:
    return datetime.now(tz=KST)


def http_get(url: str, params=None, timeout: int = 20):
    last_err = None
    for _ in range(3):
        try:
            r = requests.get(url, params=params, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
            # don’t r.raise_for_status() immediately; capture status for debugging/fallback
            if r.status_code >= 200 and r.status_code < 300:
                return r.json()
            last_err = RuntimeError(f"{r.status_code} {r.text[:200]}")
            time.sleep(1)
        except Exception as e:
            last_err = e
            time.sleep(1)
    raise RuntimeError(f"Failed request: {url} ({last_err})")


def safe_read_json(path: Path):
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def write_json(path: Path, obj):
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def usdkrw_rate() -> float:
    """Fetch USD/KRW.

    Priority:
      1) exchangerate.host
      2) last good cached value
      3) constant fallback
    """
    # 1) API
    try:
        j = http_get(FX_USDKRW_URL, {"base": "USD", "symbols": "KRW"})
        rate = float(((j.get("rates") or {}).get("KRW")) or 0)
        if rate > 0:
            write_json(
                FX_CACHE_JSON,
                {
                    "timestamp_kst": now_kst().strftime("%Y-%m-%dT%H:%M:%S%z"),
                    "USDKRW": rate,
                    "source": "exchangerate.host",
                },
            )
            return rate
    except Exception:
        pass

    # 2) cache
    cached = safe_read_json(FX_CACHE_JSON) or {}
    rate = float(cached.get("USDKRW") or 0)
    if rate > 0:
        return rate

    # 3) fallback
    return USDKRW_FALLBACK


def upbit_price(market: str) -> float:
    # market like "KRW-BTC"
    j = http_get(UPBIT_TICKER, {"markets": market})
    if isinstance(j, list) and j:
        return float(j[0].get("trade_price", 0) or 0)
    return 0.0


def binance_price(symbol: str) -> float:
    # symbol like "BTCUSDT"
    last_err = None
    for base in BINANCE_TICKER_URLS:
        try:
            j = http_get(base, {"symbol": symbol})
            return float(j.get("price", 0) or 0)
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(f"Binance price failed for {symbol}: {last_err}")


def kimchi_premium_pct(krw_price: float, usdt_price: float, usdkrw: float) -> float:
    """
    premium = (KRW_domestic - (USDT_global * USDTKRW)) / (USDT_global * USDTKRW) * 100
    """
    fair_krw = float(usdt_price) * float(usdkrw)
    if fair_krw <= 0:
        return 0.0
    return (float(krw_price) - fair_krw) / fair_krw * 100.0


def classify_kimchi_type(premium: float, ctx: Dict[str, Any]) -> Dict[str, Any]:
    """
    Very simple v1 heuristic using your existing KRW dashboard context.
    (v2: use snapshot deltas, onchain netflow, banking windows, etc.)
    """
    total = float(((ctx.get("totals") or {}).get("combined_24h")) or 0)
    top10_share = float(((ctx.get("top10") or {}).get("top10_share_pct")) or 0)
    stable_dom = float(((ctx.get("stablecoins") or {}).get("stable_dominance_pct")) or 0)

    prem_hi = premium >= 1.5
    vol_hi = total >= 2_000_000_000_000  # 2조 KRW (튜닝 포인트)
    top_hi = top10_share >= 65
    stable_hi = stable_dom >= 10  # 튜닝 포인트

    if prem_hi and vol_hi and top_hi:
        kind = "Retail-driven"
        rationale = "김프↑ + 거래대금↑ + Top10 쏠림↑ → 리테일 매수 주도 가능성"
    elif prem_hi and (not vol_hi or not stable_hi):
        kind = "Constraint-driven"
        rationale = "김프↑ + 거래대금/스테이블 비중 약함 → 구조/제약(차익거래 비활성) 가능성"
    elif premium <= -1.0:
        kind = "Reverse/Outflow"
        rationale = "역김프 구간 → 해외가 더 비싸거나 국내 매수 약함/해외 이동 가능성"
    else:
        kind = "Neutral"
        rationale = "뚜렷한 과열/제약 신호가 약함"

    score = 50.0
    score += min(20.0, max(-20.0, premium * 5.0))
    score += 10.0 if vol_hi else -5.0
    score += 10.0 if top_hi else -5.0
    score += 5.0 if stable_hi else -5.0
    score = max(0.0, min(100.0, score))

    return {
        "type": kind,
        "rationale": rationale,
        "score_v1": round(score, 1),
        "inputs": {
            "total_krw_24h": total,
            "top10_share_pct": top10_share,
            "stable_dominance_pct": stable_dom,
        },
    }


def run():
    ts = now_kst()
    ts_iso = ts.strftime("%Y-%m-%dT%H:%M:%S%z")
    ts_label = ts.strftime("%m/%d %H:%M KST")

    ctx = safe_read_json(KRW_LATEST_JSON) or {}

    # --- Prices ---
    krw_btc = upbit_price("KRW-BTC")
    krw_eth = upbit_price("KRW-ETH")
    krw_xrp = upbit_price("KRW-XRP")

    # FX
    usdkrw = usdkrw_rate()

    # Diagnostic only (local stablecoin premium proxy)
    krw_usdt = upbit_price("KRW-USDT")

    usdt_btc = binance_price("BTCUSDT")
    usdt_eth = binance_price("ETHUSDT")
    usdt_xrp = binance_price("XRPUSDT")

    prem_btc = kimchi_premium_pct(krw_btc, usdt_btc, usdkrw)
    prem_eth = kimchi_premium_pct(krw_eth, usdt_eth, usdkrw)
    prem_xrp = kimchi_premium_pct(krw_xrp, usdt_xrp, usdkrw)

    # --- ΔKimchi (%p) + Driver Share ---
    history = safe_read_json(KIMCHI_SNAPSHOTS_JSON)
    if not isinstance(history, list):
        history = []
    prev = history[-1] if history else {}
    prev_k = (prev.get("kimchi_premium_pct") or {})
    prev_btc = float(prev_k.get("BTC") or 0)
    prev_eth = float(prev_k.get("ETH") or 0)
    prev_xrp = float(prev_k.get("XRP") or 0)

    d_btc = prem_btc - prev_btc
    d_eth = prem_eth - prev_eth
    d_xrp = prem_xrp - prev_xrp

    total_abs = abs(d_btc) + abs(d_eth) + abs(d_xrp)
    if total_abs > 0:
        share_btc = abs(d_btc) / total_abs * 100.0
        share_eth = abs(d_eth) / total_abs * 100.0
        share_xrp = abs(d_xrp) / total_abs * 100.0
    else:
        share_btc = share_eth = share_xrp = 0.0

    # "Level" summary for v1 heuristic (simple mean; keep the logic stable)
    prem_level = (prem_btc + prem_eth + prem_xrp) / 3.0
    analysis = classify_kimchi_type(prem_level, ctx)

    latest = {
        "schema": "smart_kimchi_v2",
        "timestamp_kst": ts_iso,
        "timestamp_label": ts_label,
        "prices": {
            "upbit": {"KRW-BTC": krw_btc, "KRW-ETH": krw_eth, "KRW-XRP": krw_xrp, "KRW-USDT": krw_usdt},
            "binance": {"BTCUSDT": usdt_btc, "ETHUSDT": usdt_eth, "XRPUSDT": usdt_xrp},
            "fx": {"USDKRW": usdkrw, "source": "exchangerate.host"},
        },
        "kimchi_premium_pct": {
            "BTC": round(prem_btc, 3),
            "ETH": round(prem_eth, 3),
            "XRP": round(prem_xrp, 3),
        },
        "kimchi_delta_pp": {
            "BTC": round(d_btc, 3),
            "ETH": round(d_eth, 3),
            "XRP": round(d_xrp, 3),
        },
        "driver_share_pct": {
            "BTC": round(share_btc, 1),
            "ETH": round(share_eth, 1),
            "XRP": round(share_xrp, 1),
        },
        "diagnostics": {
            "local_usdt_premium_hint_pct": None
            if (krw_usdt <= 0 or usdkrw <= 0)
            else round((krw_usdt / usdkrw - 1.0) * 100.0, 3),
        },
        "smart_kimchi": analysis,
    }

    history = [x for x in history if x.get("timestamp_kst") != ts_iso]
    history.append(latest)
    history = history[-MAX_SNAPSHOTS:]

    write_json(KIMCHI_LATEST_JSON, latest)
    write_json(KIMCHI_SNAPSHOTS_JSON, history)

    # One-line summary for logs
    top_driver = max(
        [("BTC", share_btc), ("ETH", share_eth), ("XRP", share_xrp)],
        key=lambda x: x[1],
    )[0]

    print("[OK] Smart Kimchi v2 saved")
    print(
        f"     {ts_label} | BTC {prem_btc:.2f}% | ETH {prem_eth:.2f}% | XRP {prem_xrp:.2f}% "
        f"| Δp BTC {d_btc:+.2f} / ETH {d_eth:+.2f} / XRP {d_xrp:+.2f} | Driver={top_driver}"
    )
    print(f"     Type={analysis['type']} Score={analysis['score_v1']}")


if __name__ == "__main__":
    run()
