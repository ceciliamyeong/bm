#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Kimchi Premium Top10 (BOK FX + Kraken/Bitstamp USD) — sandbox build
- FX: fx_latest.json -> usdkrw.official (BOK ECOS 기준환율)
- Global price (USD): Kraken -> Bitstamp fallback
- Domestic price (KRW): Upbit

Outputs (keep separate from your existing v1):
  out/history/
    ├─ kimchi_top10_latest.json
    └─ kimchi_top10_snapshots.json
"""

import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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

# Kraken: https://api.kraken.com/0/public/Ticker?pair=XBTUSD
KRAKEN_TICKER = "https://api.kraken.com/0/public/Ticker"
# Bitstamp: https://www.bitstamp.net/api/v2/ticker/btcusd
BITSTAMP_TICKER = "https://www.bitstamp.net/api/v2/ticker/{pair}"

# ✅ 상위 10개(초기 고정). 필요하면 너희 BM20/Top10 기준으로 교체
TOP10 = ["BTC", "ETH", "XRP", "SOL", "ADA", "DOGE", "SUI", "TON", "TRX", "BNB"]

# ✅ 글로벌 거래소 심볼 매핑(필수!)
# Kraken은 BTC를 XBT로 씀. Bitstamp는 소문자.
SYMBOL_MAP = {
    "BTC": {"kraken": "XBTUSD", "bitstamp": "btcusd"},
    "ETH": {"kraken": "ETHUSD", "bitstamp": "ethusd"},
    "XRP": {"kraken": "XRPUSD", "bitstamp": "xrpusd"},
    "SOL": {"kraken": "SOLUSD", "bitstamp": "solusd"},
    "ADA": {"kraken": "ADAUSD", "bitstamp": "adausd"},
    "DOGE": {"kraken": "DOGEUSD", "bitstamp": "dogeusd"},
    "TRX": {"kraken": "TRXUSD", "bitstamp": "trxusd"},
    "BNB": {"kraken": "BNBUSD", "bitstamp": "bnbusd"},
    "TON": {"kraken": "TONUSD", "bitstamp": "tonusd"},
    "SUI": {"kraken": "SUIUSD", "bitstamp": "suiusd"},
}

# 정책: BOK official 없으면 스냅샷 계산하지 않음(축 변경 금지)
ALLOW_FX_LKG = True  # official 결측 시 직전 스냅샷의 official로만 대체


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
    j = http_get(UPBIT_TICKER, {"markets": f"KRW-{symbol}"})
    if isinstance(j, list) and j:
        return float(j[0].get("trade_price", 0) or 0)
    return 0.0


def load_fx_official() -> Tuple[Optional[float], Dict[str, Any]]:
    fx = safe_read_json(FX_LATEST_JSON) or {}
    u = (fx.get("usdkrw") or {})
    official = u.get("official", None)

    meta = {
        "file": str(FX_LATEST_JSON),
        "official": official,
        "official_date": u.get("official_date"),
        "official_source": u.get("official_source"),
        "timestamp_kst": fx.get("timestamp_kst"),
        "timestamp_label": fx.get("timestamp_label"),
    }
    if official is None:
        return None, meta
    return float(official), meta


def get_fx_lkg_from_history(history: List[Dict[str, Any]]) -> Optional[float]:
    """Last-known-good usdkrw_used from previous snapshots."""
    for prev in reversed(history):
        fx = (prev.get("fx") or {})
        used = fx.get("usdkrw_used")
        if used:
            try:
                return float(used)
            except Exception:
                continue
    return None


def kraken_price_usd(symbol: str) -> float:
    pair = SYMBOL_MAP.get(symbol, {}).get("kraken")
    if not pair:
        return 0.0
    j = http_get(KRAKEN_TICKER, {"pair": pair})
    # {"error":[],"result":{"XXBTZUSD":{"a":["..."],"c":["last",...],...}}}
    if not isinstance(j, dict) or j.get("error"):
        return 0.0
    res = (j.get("result") or {})
    if not res:
        return 0.0
    # result key is not always exactly pair; take first key
    k = next(iter(res.keys()), None)
    if not k:
        return 0.0
    ticker = res.get(k) or {}
    last = (ticker.get("c") or [None])[0]
    return float(last) if last else 0.0


def bitstamp_price_usd(symbol: str) -> float:
    pair = SYMBOL_MAP.get(symbol, {}).get("bitstamp")
    if not pair:
        return 0.0
    url = BITSTAMP_TICKER.format(pair=pair)
    j = http_get(url)
    # {"last":"89312.34", ...}
    last = (j.get("last") if isinstance(j, dict) else None)
    return float(last) if last else 0.0


def global_price_usd(symbol: str) -> Tuple[float, str]:
    """Try Kraken then Bitstamp. Return (price, source)."""
    p = kraken_price_usd(symbol)
    if p > 0:
        return p, "kraken"
    p = bitstamp_price_usd(symbol)
    if p > 0:
        return p, "bitstamp"
    return 0.0, "missing"


def kimchi_pct(dom_krw: float, glob_usd: float, usdkrw: float) -> float:
    fair_krw = float(glob_usd) * float(usdkrw)
    if fair_krw <= 0:
        return 0.0
    return (float(dom_krw) - fair_krw) / fair_krw * 100.0


def compute_deltas(prev_snapshot: Optional[Dict[str, Any]], coins: List[Dict[str, Any]]) -> None:
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

    history = safe_read_json(OUT_SNAPSHOTS)
    if not isinstance(history, list):
        history = []
    prev = history[-1] if history else None

    usdkrw, fx_meta = load_fx_official()
    fx_basis = "BOK_ECOS_official"

    if usdkrw is None and ALLOW_FX_LKG:
        lkg = get_fx_lkg_from_history(history)
        if lkg is not None:
            usdkrw = lkg
            fx_basis = "BOK_ECOS_official_LKG"

    if usdkrw is None:
        out = {
            "schema": "kimchi_top10_bok_v1",
            "timestamp_kst": ts_iso,
            "timestamp_label": ts_label,
            "status": "error",
            "error": "FX official missing and no LKG available. Kimchi not computed.",
            "fx": {"basis": fx_basis, "meta": fx_meta},
            "coins": [],
            "skipped": [],
        }
        write_json(OUT_LATEST, out)
        print("[WARN] FX missing -> wrote error latest.json (no computation)")
        return

    coins: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []

    for sym in TOP10:
        dom = upbit_price_krw(sym)
        if dom <= 0:
            skipped.append({"symbol": sym, "reason": "upbit_missing"})
            continue

        glob, src = global_price_usd(sym)
        if glob <= 0:
            skipped.append({"symbol": sym, "reason": "global_missing", "sources_tried": ["kraken", "bitstamp"]})
            continue

        prem = kimchi_pct(dom, glob, usdkrw)

        coins.append({
            "symbol": sym,
            "premium_pct": round(prem, 3),
            "upbit_krw": round(dom, 0),
            "global_usd": round(glob, 6),
            "global_source": src,
        })

    compute_deltas(prev, coins)

    # 기본: 김프 큰 순(기회 탐색)
    coins.sort(key=lambda x: x.get("premium_pct", -9999), reverse=True)

    latest = {
        "schema": "kimchi_top10_bok_v1",
        "timestamp_kst": ts_iso,
        "timestamp_label": ts_label,
        "status": "ok",
        "fx": {
            "usdkrw_used": round(float(usdkrw), 4),
            "basis": fx_basis,
            "meta": fx_meta,
        },
        "coins": coins,
        "skipped": skipped,
        "notes": {
            "definition": "premium_pct = (Upbit_KRW - Global_USD*USDKRW_BOK) / (Global_USD*USDKRW_BOK) * 100",
            "global_reference": "Kraken spot USD (fallback Bitstamp)",
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

    print("[OK] Kimchi Top10 (BOK+Kraken/Bitstamp) saved")
    print(f"     {ts_label} | USDKRW={usdkrw} ({fx_basis})")
    if coins:
        top = coins[0]
        print(f"     Top premium: {top['symbol']} {top['premium_pct']}% (Δ {top['delta_pp']})")
    if skipped:
        print(f"     Skipped: {len(skipped)} (see latest.json -> skipped)")


if __name__ == "__main__":
    run()
