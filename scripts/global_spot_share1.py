#!/usr/bin/env python3
"""Compute Korea spot-market share vs global *spot* crypto trading volume (rolling 24h).

Why this exists
- CoinMarketCap "global-metrics" volume typically reflects broader market volume that may include derivatives.
- This script computes a spot-only proxy using CoinGecko's exchange spot volume fields.

Inputs
- out/history/krw_24h_latest.json (existing pipeline output)
  - reads totals.combined_24h (KRW)

Outputs
- out/global/k_spot_share_24h_latest.json
- out/global/k_spot_share_24h_history.json (append-only list)

Notes
- Global volume is computed as: sum(exchange.trade_volume_24h_btc_normalized) * BTC_USD
- CoinGecko exchange list is treated as "spot exchanges" (CoinGecko has a separate derivatives section).
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
import xml.etree.ElementTree as ET


# -----------------------------
# FX: same approach as update_bm20_full.py (live + fallback)
# -----------------------------

def get_usdkrw_live() -> Tuple[float, str]:
    # 1) open.er-api.com (no key)
    try:
        url = "https://open.er-api.com/v6/latest/USD"
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        j = r.json()
        krw = j.get("rates", {}).get("KRW")
        if krw and float(krw) > 0:
            return float(krw), "open.er-api.com"
    except Exception:
        pass

    # 2) ECB fallback (EUR base -> USDKRW)
    try:
        url = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml"
        r = requests.get(url, timeout=10)
        r.raise_for_status()

        root = ET.fromstring(r.text)
        ns = {
            "gesmes": "http://www.gesmes.org/xml/2002-08-01",
            "eurofxref": "http://www.ecb.int/vocabulary/2002-08-01/eurofxref",
        }
        cubes = root.findall(".//eurofxref:Cube/eurofxref:Cube/eurofxref:Cube", ns)

        rates: Dict[str, float] = {}
        for c in cubes:
            cur = c.attrib.get("currency")
            rate = c.attrib.get("rate")
            if cur and rate:
                rates[cur] = float(rate)

        usd_per_eur = rates.get("USD")
        krw_per_eur = rates.get("KRW")
        if usd_per_eur and krw_per_eur and usd_per_eur > 0:
            return krw_per_eur / usd_per_eur, "ecb.europa.eu"
    except Exception:
        pass

    return 1450.0, "fallback-fixed"


# -----------------------------
# Helpers
# -----------------------------

def read_json(path: Path) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)


def append_json_list(path: Path, item: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lst: List[dict] = []
    if path.exists():
        try:
            lst = read_json(path)
            if not isinstance(lst, list):
                lst = []
        except Exception:
            lst = []
    lst.append(item)
    write_json(path, lst)


# -----------------------------
# CoinGecko global spot volume
# -----------------------------

COINGECKO_API = "https://api.coingecko.com/api/v3"


def get_btc_price_usd() -> float:
    url = f"{COINGECKO_API}/simple/price"
    params = {"ids": "bitcoin", "vs_currencies": "usd"}
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    j = r.json()
    return float(j.get("bitcoin", {}).get("usd", 0.0))


def get_global_spot_volume_usd_24h(
    *,
    per_page: int = 250,
    max_pages: int = 20,
    sleep_s: float = 1.1,
) -> Tuple[float, dict]:
    """Returns (global_spot_usd_24h, debug_meta)."""

    btc_usd = get_btc_price_usd()
    if btc_usd <= 0:
        raise RuntimeError("BTC price USD returned 0")

    total_btc = 0.0
    pages_fetched = 0
    exchanges_count = 0

    for page in range(1, max_pages + 1):
        url = f"{COINGECKO_API}/exchanges"
        params = {"per_page": per_page, "page": page}
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
        if not data:
            break

        pages_fetched += 1
        exchanges_count += len(data)

        for ex in data:
            # CoinGecko provides both raw and normalized 24h volume in BTC terms.
            # Normalized is less biased by wash trading.
            v = ex.get("trade_volume_24h_btc_normalized")
            if v is None:
                v = ex.get("trade_volume_24h_btc")
            try:
                total_btc += float(v or 0.0)
            except Exception:
                continue

        time.sleep(sleep_s)

    global_usd = total_btc * btc_usd

    meta = {
        "btc_usd": btc_usd,
        "total_btc_volume_24h_normalized_sum": total_btc,
        "pages_fetched": pages_fetched,
        "exchanges_count": exchanges_count,
        "source": "coingecko:/exchanges + btc price",
        "field": "trade_volume_24h_btc_normalized (fallback trade_volume_24h_btc)",
    }
    return global_usd, meta


# -----------------------------
# Main
# -----------------------------


def main() -> None:
    # Resolve repo root similarly to other scripts (works in Actions and locally)
    base_dir = Path(__file__).resolve().parent
    # If you drop this file into bm/scripts/, base_dir.parent is repo root
    # If you keep it elsewhere, it will still work as long as you pass --krw-json.

    import argparse

    p = argparse.ArgumentParser()
    p.add_argument(
        "--krw-json",
        default=str((base_dir.parent / "out" / "history" / "krw_24h_latest.json")),
        help="Path to out/history/krw_24h_latest.json",
    )
    p.add_argument(
        "--out-latest",
        default=str((base_dir.parent / "out" / "global" / "k_spot_share_24h_latest.json")),
        help="Output path for latest snapshot JSON",
    )
    p.add_argument(
        "--out-history",
        default=str((base_dir.parent / "out" / "global" / "k_spot_share_24h_history.json")),
        help="Output path for history list JSON",
    )
    p.add_argument("--per-page", type=int, default=250)
    p.add_argument("--max-pages", type=int, default=20)
    p.add_argument("--sleep", type=float, default=1.1)
    args = p.parse_args()

    krw_path = Path(args.krw_json)
    if not krw_path.exists():
        raise FileNotFoundError(f"KRW volume json not found: {krw_path}")

    data = read_json(krw_path)
    krw_total_24h = float((data.get("totals") or {}).get("combined_24h", 0.0))

    usdkrw, fx_source = get_usdkrw_live()
    korea_usd_24h = (krw_total_24h / usdkrw) if usdkrw > 0 else 0.0

    global_spot_usd_24h, meta = get_global_spot_volume_usd_24h(
        per_page=args.per_page,
        max_pages=args.max_pages,
        sleep_s=args.sleep,
    )

    share = (korea_usd_24h / global_spot_usd_24h) * 100 if global_spot_usd_24h > 0 else 0.0

    now_iso = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")

    out = {
        "as_of": now_iso,
        "usdkrw": usdkrw,
        "fx_source": fx_source,
        "korea": {
            "krw_total_24h": round(krw_total_24h, 2),
            "usd_total_24h": round(korea_usd_24h, 2),
        },
        "global": {
            "spot_volume_usd_24h": round(global_spot_usd_24h, 2),
            "meta": meta,
        },
        "k_spot_share_pct_24h": round(share, 4),
        "notes": [
            "Korea: KRW spot traded value(rolling 24h) from krw_24h_latest.json totals.combined_24h converted to USD",
            "Global: CoinGecko exchange list normalized spot volume (BTC) converted to USD using BTC/USD",
        ],
    }

    out_latest = Path(args.out_latest)
    out_hist = Path(args.out_history)

    write_json(out_latest, out)
    append_json_list(out_hist, out)

    print(
        f"[OK] {out['as_of']} | K-Spot share: {out['k_spot_share_pct_24h']}% "
        f"(KRW→USD {out['korea']['usd_total_24h']:,} / Global spot {out['global']['spot_volume_usd_24h']:,})"
    )


if __name__ == "__main__":
    main()
