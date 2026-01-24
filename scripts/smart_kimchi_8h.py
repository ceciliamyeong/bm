#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Smart Kimchi Index v1.1 (8h snapshots)
- Kimchi Premium (BTC/ETH) using:
  - Upbit KRW price
  - Binance USDT price (USDT≈USD proxy)
  - FX USDKRW (external)  ✅ use real FX, not KRW-USDT
- Keep KRW-USDT as a separate "USDT premium" (constraint) indicator.
- Enrich with context from your existing KRW rolling dashboard:
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
from typing import Dict, Any, List

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
UPBIT_TICKER = "https://api.upbit.com/v1/ticker"                 # markets=KRW-BTC
BINANCE_TICKER = "https://api.binance.com/api/v3/ticker/price"   # symbol=BTCUSDT
FX_URL = "https://api.exchangerate.host/latest"                  # base=USD&symbols=KRW


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
    j = http_get(UPBIT_TICKER, {"markets": market})
    if isinstance(j, list) and j:
        return float(j[0].get("trade_price", 0) or 0)
    return 0.0


def binance_price(symbol: str) -> float:
    j = http_get(BINANCE_TICKER, {"symbol": symbol})
    return float(j.get("price", 0) or 0)


def fx_usdkrw() -> float:
    """
    Real USDKRW FX. If this fails, fallback to a sane range using last saved value.
    """
    j = http_get(FX_URL, {"base": "USD", "symbols": "KRW"})
    rates = j.get("rates") or {}
    v = float(rates.get("KRW", 0) or 0)
    return v


def kimchi_premium_pct(krw_price: float, usd_price: float, usdkrw: float) -> float:
    fair_krw = float(usd_price) * float(usdkrw)
    if fair_krw <= 0:
        return 0.0
    return (float(krw_price) - fair_krw) / fair_krw * 100.0


def classify_kimchi_type(premium: float, delta: float, ctx: Dict[str, Any], usdt_prem: float) -> Dict[str, Any]:
    total = float(((ctx.get("totals") or {}).get("combined_24h")) or 0)
    top10_share = float(((ctx.get("top10") or {}).get("top10_share_pct")) or 0)
    stable_dom = float(((ctx.get("stablecoins") or {}).get("stable_dominance_pct")) or 0)

    prem_hi = premium >= 1.5
    prem_lo = premium <= -1.0
    vol_hi = total >= 2_000_000_000_000
    top_hi = top10_share >= 65
    stable_hi = stable_dom >= 10
    usdt_hi = usdt_prem >= 1.0  # KRW-USDT가 FX 대비 1% 이상 비싸면 "자본제약/수요" 신호

    if prem_hi and delta > 0 and vol_hi and top_hi:
        kind = "Retail-driven"
        rationale = "김프↑(확대) + 거래대금↑ + Top10 쏠림↑ → 리테일 매수 주도 가능성"
    elif prem_hi and usdt_hi:
        kind = "Constraint-driven"
        rationale = "김프↑ + USDT 프리미엄↑ → 환전/차익거래 제약·국내 달러수요 신호 가능성"
    elif prem_lo:
        kind = "Reverse/Outflow"
        rationale = "역김프 구간 → 국내 수요 약화 또는 해외 프리미엄"
    else:
        kind = "Neutral"
        rationale = "뚜렷한 과열/제약 신호가 약함"

    # score 0~100 (premium + momentum + constraints)
    score = 50.0
    score += min(25.0, max(-25.0, premium * 6.0))
    score += min(15.0, max(-15.0, delta * 8.0))
    score += 8.0 if vol_hi else -4.0
    score += 8.0 if top_hi else -4.0
    score += 4.0 if stable_hi else -4.0
    score += 6.0 if usdt_hi else 0.0
    score = max(0.0, min(100.0, score))

    return {
        "type": kind,
        "rationale": rationale,
        "score_v1": round(score, 1),
        "inputs": {
            "total_krw_24h": total,
            "top10_share_pct": top10_share,
            "stable_dominance_pct": stable_dom,
            "usdt_premium_pct": usdt_prem,
            "delta_combo": delta,
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

    # Upbit KRW-USDT (keep as separate indicator)
    krw_usdt = upbit_price("KRW-USDT")  # may be 0 if not listed; that's okay

    # Global (USDT ≈ USD proxy)
    usdt_btc = binance_price("BTCUSDT")
    usdt_eth = binance_price("ETHUSDT")

    # FX USDKRW (real)
    usdkrw = 0.0
    try:
        usdkrw = fx_usdkrw()
    except Exception:
        # fallback: try last saved value
        prev = safe_read_json(KIMCHI_LATEST_JSON) or {}
        usdkrw = float(prev.get("fx_usdkrw", 0) or 0)
    if usdkrw <= 0:
        usdkrw = 1350.0  # absolute last resort

    prem_btc = kimchi_premium_pct(krw_btc, usdt_btc, usdkrw)
    prem_eth = kimchi_premium_pct(krw_eth, usdt_eth, usdkrw)
    prem_combo = prem_btc * 0.7 + prem_eth * 0.3

    # deltas vs previous
    prev = safe_read_json(KIMCHI_LATEST_JSON) or {}
    prev_combo = float((prev.get("kimchi_premium_pct") or {}).get("COMBO", 0) or 0)
    delta_combo = prem_combo - prev_combo

    # USDT premium vs FX (only if KRW-USDT exists)
    usdt_prem = 0.0
    if krw_usdt > 0 and usdkrw > 0:
        usdt_prem = (krw_usdt / usdkrw - 1.0) * 100.0

    analysis = classify_kimchi_type(prem_combo, delta_combo, ctx, usdt_prem)

    latest = {
        "schema": "smart_kimchi_v1_1",
        "timestamp_kst": ts_iso,
        "timestamp_label": ts_label,
        "fx_usdkrw": round(usdkrw, 4),
        "prices": {
            "upbit": {"KRW-BTC": krw_btc, "KRW-ETH": krw_eth, "KRW-USDT": krw_usdt},
            "binance": {"BTCUSDT": usdt_btc, "ETHUSDT": usdt_eth},
        },
        "kimchi_premium_pct": {
            "BTC": round(prem_btc, 3),
            "ETH": round(prem_eth, 3),
            "COMBO": round(prem_combo, 3),
        },
        "delta": {
            "COMBO": round(delta_combo, 3),
        },
        "usdt_premium_pct": round(usdt_prem, 3),
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

    print("[OK] Smart Kimchi v1.1 saved")
    print(f"     {ts_label} | FX={usdkrw:,.2f} | USDTprem={usdt_prem:+.2f}%")
    print(f"     BTC {prem_btc:+.2f}% | ETH {prem_eth:+.2f}% | COMBO {prem_combo:+.2f}% (Δ {delta_combo:+.2f}p)")
    print(f"     Type={analysis['type']} Score={analysis['score_v1']}")

if __name__ == "__main__":
    run()
