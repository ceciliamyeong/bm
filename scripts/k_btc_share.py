#!/usr/bin/env python3
"""
K-BTC Spot Share (24H)

Korea (Upbit + Bithumb + Coinone BTC/KRW spot traded value 24h) converted to USD
divided by
Global (CMC BTC volume_24h in USD)

Outputs:
  out/global/k_btc_share_24h_latest.json
  out/global/k_btc_share_24h_history.json

Notes:
- Uses exchange public ticker APIs for KRW traded value.
- Uses out/history/fx_latest.json for USDKRW if available; otherwise falls back to open.er-api.
- Uses CoinMarketCap /v1/cryptocurrency/quotes/latest for global BTC volume_24h (USD).
"""
from __future__ import annotations

import os, json, time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import requests

ROOT = Path(__file__).resolve().parents[1] if (Path(__file__).resolve().parent.name == "scripts") else Path(".").resolve()
OUT_DIR = ROOT / "out"
HIST_DIR = OUT_DIR / "history"
GLOBAL_DIR = OUT_DIR / "global"
GLOBAL_DIR.mkdir(parents=True, exist_ok=True)

FX_LATEST = HIST_DIR / "fx_latest.json"

LATEST_OUT = GLOBAL_DIR / "k_btc_share_24h_latest.json"
HISTORY_OUT = GLOBAL_DIR / "k_btc_share_24h_history.json"

CMC_QUOTES_URL = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"

UPBIT_TICKER = "https://api.upbit.com/v1/ticker"
BITHUMB_TICKER_ALL = "https://api.bithumb.com/public/ticker/ALL_KRW"
COINONE_TICKER_ALL = "https://api.coinone.co.kr/public/v2/ticker_new/KRW"


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


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


def append_history(row: Dict[str, Any], keep_last: int = 1500) -> None:
    hist = []
    if HISTORY_OUT.exists():
        try:
            hist = read_json(HISTORY_OUT).get("rows") or []
        except Exception:
            hist = []
    as_of = row.get("as_of")
    hist = [x for x in hist if x.get("as_of") != as_of]
    hist.append(row)
    hist.sort(key=lambda x: x.get("as_of") or "")
    if keep_last and len(hist) > keep_last:
        hist = hist[-keep_last:]
    write_json(HISTORY_OUT, {"rows": hist})


def get_usdkrw() -> Tuple[float, str]:
    fx = read_json(FX_LATEST)
    v = fx.get("usdkrw")
    try:
        if v is not None and float(v) > 100:
            return float(v), "out/history/fx_latest.json"
    except Exception:
        pass

    # fallback: open.er-api (free)
    try:
        r = requests.get("https://open.er-api.com/v6/latest/USD", timeout=15)
        r.raise_for_status()
        j = r.json()
        krw = (j.get("rates") or {}).get("KRW")
        if krw and float(krw) > 100:
            return float(krw), "open.er-api.com"
    except Exception:
        pass

    return 1350.0, "fallback:1350"


def upbit_btc_krw_24h() -> float:
    # Upbit market code for BTC is KRW-BTC
    r = requests.get(UPBIT_TICKER, params={"markets": "KRW-BTC"}, timeout=15)
    r.raise_for_status()
    arr = r.json()
    # acc_trade_price_24h is 24h traded value in KRW
    return float(arr[0]["acc_trade_price_24h"]) if arr else 0.0


def bithumb_btc_krw_24h() -> float:
    r = requests.get(BITHUMB_TICKER_ALL, timeout=15)
    r.raise_for_status()
    j = r.json()
    data = j.get("data") or {}
    btc = data.get("BTC") or {}
    # acc_trade_value_24H is 24h traded value (KRW) in Bithumb public API
    v = btc.get("acc_trade_value_24H") or btc.get("acc_trade_value_24h")
    return float(v) if v else 0.0


def coinone_btc_krw_24h() -> float:
    r = requests.get(COINONE_TICKER_ALL, timeout=15)
    r.raise_for_status()
    j = r.json()
    # In this endpoint, tickers list contains quote_volume (KRW) for 24h
    tickers = j.get("tickers") or []
    for t in tickers:
        if (t.get("target_currency") or "").upper() == "BTC":
            v = t.get("quote_volume")
            return float(v) if v else 0.0
    return 0.0


def global_btc_volume_usd_24h(cmc_key: str) -> float:
    if not cmc_key:
        raise RuntimeError("Missing CMC_API_KEY")
    # Try by symbol (works on CMC)
    for attempt in range(4):
        r = requests.get(
            CMC_QUOTES_URL,
            headers={"X-CMC_PRO_API_KEY": cmc_key},
            params={"symbol": "BTC", "convert": "USD"},
            timeout=20,
        )
        if r.status_code == 429:
            time.sleep(1.2 * (attempt + 1))
            continue
        r.raise_for_status()
        j = r.json()
        data = (j.get("data") or {}).get("BTC") or {}
        quote = (data.get("quote") or {}).get("USD") or {}
        vol = quote.get("volume_24h")
        if vol is None:
            raise RuntimeError("CMC response missing volume_24h for BTC")
        return float(vol)
    raise RuntimeError("CMC rate limited (429) after retries")


def main() -> int:
    errors = []
    usdkrw, fx_source = get_usdkrw()

    # Korea BTC traded value (KRW)
    up, bi, co = 0.0, 0.0, 0.0
    try:
        up = upbit_btc_krw_24h()
    except Exception as e:
        errors.append(f"upbit: {e}")
    try:
        bi = bithumb_btc_krw_24h()
    except Exception as e:
        errors.append(f"bithumb: {e}")
    try:
        co = coinone_btc_krw_24h()
    except Exception as e:
        errors.append(f"coinone: {e}")

    krw_total = up + bi + co
    krw_usd = krw_total / usdkrw if usdkrw > 0 else 0.0

    # Global BTC (USD)
    cmc_key = os.getenv("CMC_API_KEY", "").strip()
    try:
        global_usd = global_btc_volume_usd_24h(cmc_key)
    except Exception as e:
        errors.append(f"cmc: {e}")
        # don't crash pipeline; write latest with zeros so UI can show Errors
        global_usd = 0.0

    share = (krw_usd / global_usd * 100.0) if global_usd > 0 else 0.0

    as_of = now_iso()
    latest = {
        "as_of": as_of,
        "k_btc_share_pct_24h": round(share, 4),
        "krw_btc_vol_24h_krw": round(krw_total, 2),
        "krw_btc_vol_24h_usd": round(krw_usd, 2),
        "usdkrw": round(usdkrw, 2),
        "global_btc_vol_24h_usd": round(global_usd, 2),
        "errors": errors,
        "fx_source": fx_source,
    }

    write_json(LATEST_OUT, latest)
    append_history({"as_of": as_of, "k_btc_share_pct_24h": latest["k_btc_share_pct_24h"]})
    print(f"✅ wrote {LATEST_OUT} and appended {HISTORY_OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
