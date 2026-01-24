#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Smart Kimchi Index v1 (8h snapshots)
- Compute Kimchi Premium (BTC/ETH) using:
  - Upbit KRW price
  - Binance USDT price
  - USDT->KRW proxy via Upbit KRW-USDT price (if available)
- Enrich with "context" from your existing KRW rolling dashboard:
  - total KRW volume (combined_24h)
  - top10 share
  - stable dominance
- Outputs:
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
BINANCE_TICKER = "https://api.binance.com/api/v3/ticker/price"  # symbol=BTCUSDT

def now_kst() -> datetime:
    return datetime.now(tz=KST)

def http_get(url: str, params=None):
    last_err = None
    for _ in range(3):
        try:
            r = requests.get(url, params=params, timeout=20)
            r.raise_for_status()
            return r.json()
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

def upbit_price(market: str) -> float:
    # market like "KRW-BTC"
    j = http_get(UPBIT_TICKER, {"markets": market})
    if isinstance(j, list) and j:
        return float(j[0].get("trade_price", 0) or 0)
    return 0.0

def binance_price(symbol: str) -> float:
    # symbol like "BTCUSDT"
    j = http_get(BINANCE_TICKER, {"symbol": symbol})
    return float(j.get("price", 0) or 0)

def kimchi_premium_pct(krw_price: float, usd_price: float, usdt_krw: float) -> float:
    """
    premium = (KRW_domestic - (USD_global * USDTKRW)) / (USD_global * USDTKRW) * 100
    """
    fair_krw = float(usd_price) * float(usdt_krw)
    if fair_krw <= 0:
        return 0.0
    return (float(krw_price) - fair_krw) / fair_krw * 100.0

def classify_kimchi_type(premium: float, ctx: Dict[str, Any]) -> Dict[str, Any]:
    """
    Very simple v1 heuristic using your existing KRW dashboard context.
    - If premium high AND KRW volume strong AND top10 share rising -> Retail-driven
    - If premium high BUT volume weak OR stable dominance low -> Constraint/structural
    We don't have deltas here in v1; we use absolute levels + thresholds.
    (v2: use snapshot deltas, onchain netflow, banking windows, etc.)
    """
    total = float(((ctx.get("totals") or {}).get("combined_24h")) or 0)
    top10_share = float(((ctx.get("top10") or {}).get("top10_share_pct")) or 0)
    stable_dom = float(((ctx.get("stablecoins") or {}).get("stable_dominance_pct")) or 0)

    # thresholds (tune later)
    prem_hi = premium >= 1.5
    vol_hi = total >= 2_000_000_000_000  # 2조 KRW (대충; 너 데이터 보고 튜닝)
    top_hi = top10_share >= 65
    stable_hi = stable_dom >= 10  # KRW마켓 특성상 낮을 수 있어; 튜닝 포인트

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

    # v1 score: 0~100 (단순)
    score = 50.0
    score += min(20.0, max(-20.0, premium * 5.0))   # premium 영향
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
        }
    }

def run():
    ts = now_kst()
    ts_iso = ts.strftime("%Y-%m-%dT%H:%M:%S%z")
    ts_label = ts.strftime("%m/%d %H:%M KST")

    ctx = safe_read_json(KRW_LATEST_JSON) or {}

    # --- Prices ---
    krw_btc = upbit_price("KRW-BTC")
    krw_eth = upbit_price("KRW-ETH")

    # USDT KRW proxy (Upbit KRW-USDT market exists on Upbit)
    # If it doesn't exist or returns 0, fallback to 1350 as rough (better: actual FX API)
    usdt_krw = upbit_price("KRW-USDT")
    if usdt_krw <= 0:
        usdt_krw = 1350.0

    usdt_btc = binance_price("BTCUSDT")
    usdt_eth = binance_price("ETHUSDT")

    prem_btc = kimchi_premium_pct(krw_btc, usdt_btc, usdt_krw)
    prem_eth = kimchi_premium_pct(krw_eth, usdt_eth, usdt_krw)

    # Simple combined premium (weight BTC 70 / ETH 30)
    prem_combo = prem_btc * 0.7 + prem_eth * 0.3

    analysis = classify_kimchi_type(prem_combo, ctx)

    latest = {
        "schema": "smart_kimchi_v1",
        "timestamp_kst": ts_iso,
        "timestamp_label": ts_label,
        "prices": {
            "upbit": {"KRW-BTC": krw_btc, "KRW-ETH": krw_eth, "KRW-USDT": usdt_krw},
            "binance": {"BTCUSDT": usdt_btc, "ETHUSDT": usdt_eth},
        },
        "kimchi_premium_pct": {
            "BTC": round(prem_btc, 3),
            "ETH": round(prem_eth, 3),
            "COMBO": round(prem_combo, 3),
        },
        "smart_kimchi": analysis,
    }

    history = safe_read_json(KIMCHI_SNAPSHOTS_JSON)
    if not isinstance(history, list):
        history = []
    history = [x for x in history if x.get("timestamp_kst") != ts_iso]
    history.append(latest)
    history = history[-MAX_SNAPSHOTS:]

    write_json(KIMCHI_LATEST_JSON, latest)
    write_json(KIMCHI_SNAPSHOTS_JSON, history)

    print("[OK] Smart Kimchi v1 saved")
    print(f"     {ts_label} | BTC {prem_btc:.2f}% | ETH {prem_eth:.2f}% | COMBO {prem_combo:.2f}%")
    print(f"     Type={analysis['type']} Score={analysis['score_v1']}")

if __name__ == "__main__":
    run()
