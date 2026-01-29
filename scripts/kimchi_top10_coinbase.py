#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Kimchi Premium Top10 (Coinbase USD) — test build
- No Binance.
- Global reference: Coinbase spot USD (fallback: none; skip if pair unavailable)
- FX reference: fx_latest.json -> usdkrw.official (BOK ECOS)
- Domestic: Upbit KRW spot

Outputs (separate from v1 so you can sandbox safely):
  out/history/
    ├─ kimchi_top10_latest.json
    └─ kimchi_top10_snapshots.json

Notes:
- This script intentionally SKIPS coins Coinbase doesn't support (so you can see coverage gaps first).
- Add/adjust TOP10 list + SYMBOL_MAP as you want.
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

FX_LATEST_JSON = HIST_DIR / "fx_latest.json"

OUT_LATEST = HIST_DIR / "kimchi_top10_latest.json"
OUT_SNAPSHOTS = HIST_DIR / "kimchi_top10_snapshots.json"
MAX_SNAPSHOTS = 400

UPBIT_TICKER = "https://api.upbit.com/v1/ticker"
COINBASE_SPOT = "https://api.coinbase.com/v2/prices/{pair}/spot"  # pair like BTC-USD

# ✅ Start simple: "상위 10개"는 우선 고정 리스트로 (필요하면 교체)
TOP10 = ["BTC", "ETH", "XRP", "SOL", "ADA", "DOGE", "SUI", "TON", "TRX", "BNB"]

# ✅ 심볼 예외(코인베이스/업비트 표기 차이 있을 때만 추가)
# - 대부분은 그대로 통과하지만, 나중에 필요하면 여기에 추가.
SYMBOL_MAP = {
    # "COIN": {"upbit": "KRW-COIN", "coinbase": "COIN-USD"}
    # 예: "XBT": {"coinbase": "BTC-USD"} 같은 변환 케이스를 여기에 넣으면 됨
}

# ✅ 운영 정책: 공식 환율 없으면 계산하지 않기(축이 바뀌면 안 됨)
ALLOW_FX_FALLBACK = False  # True로 하면 official 없을 때 fx.usdkrw.market 같은 걸 쓰게 만들 수 있지만, 지금은 금지


def now_kst() -> datetime:
    return datetime.now(tz=KST)


def http_get(url: str, params=None, timeout: int = 20) -> Any:
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


def safe_read_json(path: Path) -> Optional[Any]:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def write_json(path: Path, obj: Any) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def upbit_price_krw(symbol: str) -> float:
    market = SYMBOL_MAP.get(symbol, {}).get("upbit") or f"KRW-{symbol}"
    j = http_get(UPBIT_TICKER, {"markets": market})
    if isinstance(j, list) and j:
        return float(j[0].get("trade_price", 0) or 0)
    return 0.0


def coinbase_price_usd(symbol: str) -> Tuple[float, str]:
    """Return (price_usd, pair_str). Raise on failure."""
    pair = SYMBOL_MAP.get(symbol, {}).get("coinbase") or f"{symbol}-USD"
    url = COINBASE_SPOT.format(pair=pair)
    j = http_get(url)
    # Coinbase: {"data":{"base":"BTC","currency":"USD","amount":"xxxxx.xx"}}
    amt = (((j or {}).get("data") or {}).get("amount")) if isinstance(j, dict) else None
    if not amt:
        return 0.0, pair
    return float(amt), pair


def kimchi_pct(dom_krw: float, glob_usd: float, usdkrw: float) -> float:
    fair_krw = float(glob_usd) * float(usdkrw)
    if fair_krw <= 0:
        return 0.0
    return (float(dom_krw) - fair_krw) / fair_krw * 100.0


def load_fx_official() -> Tuple[Optional[float], Dict[str, Any]]:
    fx = safe_read_json(FX_LATEST_JSON) or {}
    u = (fx.get("usdkrw") or {})
    official = u.get("official", None)
    basis = {
        "source_file": str(FX_LATEST_JSON),
        "official": official,
        "official_date": u.get("official_date"),
        "official_source": u.get("official_source"),
        "market_proxy": u.get("market", None),
    }
    if official is None:
        if not ALLOW_FX_FALLBACK:
            return None, basis
        # (not recommended) fallback path:
        m = u.get("market", None)
        if m is None:
            return None, basis
        return float(m), {**basis, "fallback_used": "market_proxy"}
    return float(official), basis


def compute_deltas(prev_snapshot: Optional[Dict[str, Any]], coins: List[Dict[str, Any]]) -> None:
    """Add delta_pp vs previous snapshot for each coin (in percentage points)."""
    if not prev_snapshot:
        for c in coins:
            c["delta_pp"] = None
        return
    prev_map = {x["symbol"]: x for x in (prev_snapshot.get("coins") or []) if isinstance(x, dict) and x.get("symbol")}
    for c in coins:
        p = prev_map.get(c["symbol"])
        if not p or p.get("premium_pct") is None:
            c["delta_pp"] = None
        else:
            c["delta_pp"] = round(float(c["premium_pct"]) - float(p["premium_pct"]), 3)


def run() -> None:
    ts = now_kst()
    ts_iso = ts.strftime("%Y-%m-%dT%H:%M:%S%z")
    ts_label = ts.strftime("%m/%d %H:%M KST")

    usdkrw, fx_meta = load_fx_official()
    if usdkrw is None:
        out = {
            "schema": "kimchi_top10_coinbase_v1",
            "timestamp_kst": ts_iso,
            "timestamp_label": ts_label,
            "status": "error",
            "error": "FX official missing (fx_latest.json usdkrw.official is null). Kimchi not computed.",
            "fx": fx_meta,
            "coins": [],
            "skipped": [],
        }
        write_json(OUT_LATEST, out)
        print("[WARN] FX official missing -> wrote error latest.json (no computation)")
        return

    history = safe_read_json(OUT_SNAPSHOTS)
    if not isinstance(history, list):
        history = []
    prev = history[-1] if history else None

    coins: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []

    for sym in TOP10:
        dom = upbit_price_krw(sym)
        if dom <= 0:
            skipped.append({"symbol": sym, "reason": "upbit_price_missing"})
            continue

        try:
            glob, pair = coinbase_price_usd(sym)
        except Exception as e:
            skipped.append({"symbol": sym, "reason": "coinbase_fetch_failed", "detail": str(e)})
            continue

        if glob <= 0:
            skipped.append({"symbol": sym, "reason": "coinbase_pair_missing_or_zero", "pair": pair})
            continue

        prem = kimchi_pct(dom, glob, usdkrw)

        coins.append({
            "symbol": sym,
            "premium_pct": round(prem, 3),
            "upbit_krw": round(dom, 0),
            "global_usd": round(glob, 6),
            "global_pair": pair,
        })

    # deltas vs previous
    compute_deltas(prev, coins)

    # sort: highest premium first (기회 탐색용 기본)
    coins.sort(key=lambda x: x.get("premium_pct", -9999), reverse=True)

    latest = {
        "schema": "kimchi_top10_coinbase_v1",
        "timestamp_kst": ts_iso,
        "timestamp_label": ts_label,
        "status": "ok",
        "fx": {
            "usdkrw_used": round(usdkrw, 4),
            "basis": "BOK_ECOS_official",
            "meta": fx_meta,
        },
        "coins": coins,
        "skipped": skipped,
        "notes": {
            "definition": "premium_pct = (Upbit_KRW - Global_USD*USDKRW_official) / (Global_USD*USDKRW_official) * 100",
            "global_reference": "Coinbase spot USD",
            "domestic_reference": "Upbit KRW spot",
            "top10": TOP10,
        },
    }

    # append history (dedupe exact timestamp)
    history = [x for x in history if x.get("timestamp_kst") != ts_iso]
    history.append(latest)
    history = history[-MAX_SNAPSHOTS:]

    write_json(OUT_LATEST, latest)
    write_json(OUT_SNAPSHOTS, history)

    print("[OK] Kimchi Top10 (Coinbase) saved")
    print(f"     {ts_label} | USDKRW_official={usdkrw}")
    if coins:
        top = coins[0]
        print(f"     Top premium: {top['symbol']} {top['premium_pct']}% (Δ {top['delta_pp']})")
    if skipped:
        print(f"     Skipped: {len(skipped)} (check latest.json -> skipped)")


if __name__ == "__main__":
    run()
