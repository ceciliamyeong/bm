#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
FX Updater (8h)
- Fetch USD/KRW official rate from BOK ECOS (daily reference)
- Fetch market USD/KRW proxy from Upbit KRW-USDT
- Save single source of truth: fx_latest.json
"""

import os
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

KST = timezone(timedelta(hours=9))

BASE_DIR = Path(__file__).resolve().parent.parent
HIST_DIR = BASE_DIR / "out" / "history"
HIST_DIR.mkdir(parents=True, exist_ok=True)

FX_LATEST_JSON = HIST_DIR / "fx_latest.json"

ECOS_API_KEY = os.environ.get("ECOS_API_KEY")

# ECOS: 731Y001 = 환율, 0000001 = USD, 0000002 = KRW
ECOS_URL = (
    "https://ecos.bok.or.kr/api/StatisticSearch/"
    "{key}/json/kr/1/10/731Y001/D/{date}/{date}/0000001/0000002"
)

UPBIT_TICKER = "https://api.upbit.com/v1/ticker"


def now_kst():
    return datetime.now(tz=KST)


def http_get(url, params=None):
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    return r.json()


def upbit_usdt_krw() -> float:
    j = http_get(UPBIT_TICKER, {"markets": "KRW-USDT"})
    if isinstance(j, list) and j:
        return float(j[0].get("trade_price", 0) or 0)
    return 0.0


def fetch_ecos_usdkrw(date: str) -> float:
    if not ECOS_API_KEY:
        return 0.0
    url = ECOS_URL.format(key=ECOS_API_KEY, date=date)
    j = http_get(url)
    rows = (
        j.get("StatisticSearch", {})
         .get("row", [])
    )
    if not rows:
        return 0.0
    # 매매기준율은 DATA_VALUE
    return float(rows[0].get("DATA_VALUE", 0) or 0)


def run():
    ts = now_kst()
    ts_iso = ts.strftime("%Y-%m-%dT%H:%M:%S%z")
    ts_label = ts.strftime("%m/%d %H:%M KST")
    date = ts.strftime("%Y%m%d")

    market = upbit_usdt_krw()

    official = 0.0
    official_date = date
    if ECOS_API_KEY:
        try:
            official = fetch_ecos_usdkrw(date)
        except Exception:
            official = 0.0

    fx = {
        "timestamp_kst": ts_iso,
        "timestamp_label": ts_label,
        "usdkrw": {
            "market": round(market, 2) if market > 0 else None,
            "official": round(official, 2) if official > 0 else None,
            "official_date": official_date if official > 0 else None,
            "official_source": "BOK_ECOS" if official > 0 else None,
        }
    }

    FX_LATEST_JSON.write_text(
        json.dumps(fx, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    print("[OK] FX updated")
    print(f"     market={fx['usdkrw']['market']} official={fx['usdkrw']['official']}")


if __name__ == "__main__":
    run()
