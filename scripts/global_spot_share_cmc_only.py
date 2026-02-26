#!/usr/bin/env python3
"""
Compute Korea spot market share (KRW markets) vs Global SPOT volume (24h rolling).

- Numerator (Korea spot KRW markets):
    Reads out/history/krw_24h_latest.json -> totals.combined_24h (KRW)

- FX (USDKRW):
    Reads out/history/fx_latest.json (same format used in existing pipeline).
    If missing, falls back to CMC forex (if available) or a conservative hard-coded fallback.

- Denominator (Global SPOT volume):
    Uses CoinMarketCap "global-metrics/quotes/latest" and selects a SPOT-only volume field:
      1) total_volume_24h_adjusted (preferred; spot, excludes no-fee / txn-mining pairs)
      2) total_volume_24h_reported (spot, all spot markets)
      3) total_volume_24h (spot aggregate)

CoinMarketCap defines cryptoasset/exchange/aggregate volumes as SPOT volumes (derivatives are separate products/pages).
See: https://support.coinmarketcap.com/hc/en-us/articles/360043395912-Volume-Market-Pair-Cryptoasset-Exchange-Aggregate

Outputs:
  out/global/k_spot_share_24h_latest.json
  out/global/k_spot_share_24h_history.json (append by as_of timestamp; keeps last N rows)
"""
from __future__ import annotations

import os
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Tuple, Optional

import requests


ROOT = Path(__file__).resolve().parents[1] if (Path(__file__).resolve().parent.name == "scripts") else Path(".").resolve()
OUT_DIR = ROOT / "out"
HIST_DIR = OUT_DIR / "history"
GLOBAL_DIR = OUT_DIR / "global"
GLOBAL_DIR.mkdir(parents=True, exist_ok=True)

KRW_24H_LATEST = HIST_DIR / "krw_24h_latest.json"
FX_LATEST = HIST_DIR / "fx_latest.json"
LATEST_OUT = GLOBAL_DIR / "k_spot_share_24h_latest.json"
HISTORY_OUT = GLOBAL_DIR / "k_spot_share_24h_history.json"

CMC_GLOBAL_METRICS_URL = "https://pro-api.coinmarketcap.com/v1/global-metrics/quotes/latest"


def read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    tmp.replace(path)


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def get_usdkrw_from_fx_latest() -> Optional[float]:
    fx = read_json(FX_LATEST)
    # expected: {"as_of": "...", "usdkrw": 1442.40, ...}
    v = fx.get("usdkrw")
    try:
        if v is None:
            return None
        v = float(v)
        return v if v > 100 else None
    except Exception:
        return None


def get_usdkrw_from_cmc(api_key: str) -> Optional[float]:
    """
    Fallback: use CMC /tools/price-conversion?amount=1&symbol=USD&convert=KRW
    Not all plans include this endpoint; keep optional.
    """
    if not api_key:
        return None
    url = "https://pro-api.coinmarketcap.com/v2/tools/price-conversion"
    try:
        r = requests.get(
            url,
            headers={"X-CMC_PRO_API_KEY": api_key},
            params={"amount": 1, "symbol": "USD", "convert": "KRW"},
            timeout=15,
        )
        if not r.ok:
            return None
        j = r.json()
        # response: data[0].quote.KRW.price
        data = j.get("data")
        if isinstance(data, list) and data:
            price = (((data[0] or {}).get("quote") or {}).get("KRW") or {}).get("price")
            if price:
                price = float(price)
                return price if price > 100 else None
    except Exception:
        return None
    return None


def get_global_spot_volume_usd_24h(api_key: str) -> Tuple[float, str]:
    """
    Returns (volume_usd, field_used) from CMC global-metrics (spot aggregate).
    Preference order aligns with CMC definitions:
      - adjusted: spot excluding no-fee/txn-mining pairs
      - reported: all spot markets
      - total: spot aggregate
    """
    if not api_key:
        raise RuntimeError("Missing CMC API key (secrets.CMC_API_KEY).")

    # Basic retry/backoff for transient errors / rate limits.
    for attempt in range(5):
        r = requests.get(CMC_GLOBAL_METRICS_URL, headers={"X-CMC_PRO_API_KEY": api_key}, timeout=20)
        if r.status_code == 429:
            # exponential-ish backoff
            time.sleep(1.5 * (attempt + 1))
            continue
        r.raise_for_status()
        j = r.json()

        usd = (((j.get("data") or {}).get("quote") or {}).get("USD") or {})
        candidates = [
            ("total_volume_24h_adjusted", usd.get("total_volume_24h_adjusted")),
            ("total_volume_24h_reported", usd.get("total_volume_24h_reported")),
            ("total_volume_24h", usd.get("total_volume_24h")),
        ]
        for field, val in candidates:
            try:
                if val is not None and float(val) > 0:
                    return float(val), field
            except Exception:
                pass

        raise RuntimeError("CMC global-metrics response missing total_volume_24h* fields.")
    raise RuntimeError("CMC rate limited (429) after retries.")


def get_krw_spot_volume_krw_24h() -> float:
    krw = read_json(KRW_24H_LATEST)
    # expected: totals.combined_24h
    totals = (krw.get("totals") or {})
    v = totals.get("combined_24h")
    if v is None:
        raise RuntimeError("krw_24h_latest.json missing totals.combined_24h")
    return float(v)


def append_history(row: Dict[str, Any], keep_last: int = 1200) -> None:
    hist = []
    if HISTORY_OUT.exists():
        try:
            hist = read_json(HISTORY_OUT).get("rows") or []
        except Exception:
            hist = []

    # de-dup by as_of (keep latest)
    as_of = row.get("as_of")
    hist = [x for x in hist if x.get("as_of") != as_of]
    hist.append(row)

    # sort by as_of
    hist.sort(key=lambda x: x.get("as_of") or "")
    if keep_last and len(hist) > keep_last:
        hist = hist[-keep_last:]

    write_json(HISTORY_OUT, {"rows": hist})


def main() -> int:
    api_key = os.getenv("CMC_API_KEY", "").strip()

    # Numerator
    krw_24h = get_krw_spot_volume_krw_24h()

    # FX
    usdkrw = get_usdkrw_from_fx_latest()
    fx_source = "out/history/fx_latest.json"
    if usdkrw is None:
        usdkrw = get_usdkrw_from_cmc(api_key)
        fx_source = "cmc:tools/price-conversion" if usdkrw is not None else "fallback"
    if usdkrw is None:
        usdkrw = 1350.0  # conservative fallback to avoid crash; better than failing the workflow
        fx_source = "fallback:1350"

    krw_usd_24h = krw_24h / usdkrw

    # Denominator (Global SPOT)
    global_usd_24h, global_field = get_global_spot_volume_usd_24h(api_key)

    share_pct = (krw_usd_24h / global_usd_24h * 100.0) if global_usd_24h > 0 else 0.0

    as_of = now_iso()
    latest = {
        "as_of": as_of,
        "k_spot_share_pct_24h": round(share_pct, 4),
        "krw_spot_vol_24h_krw": round(krw_24h, 2),
        "krw_spot_vol_24h_usd": round(krw_usd_24h, 2),
        "usdkrw": round(usdkrw, 2),
        "global_spot_vol_24h_usd": round(global_usd_24h, 2),
        "global_volume_field": global_field,
        "fx_source": fx_source,
        "errors": [],
    }

    write_json(LATEST_OUT, latest)
    append_history(
        {"as_of": as_of, "k_spot_share_pct_24h": latest["k_spot_share_pct_24h"]},
        keep_last=1500
    )
    print(f"✅ wrote {LATEST_OUT} and appended {HISTORY_OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
