#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
KRW Liquidity Pipeline (8h snapshots → daily proxy → weekly)
- Exchanges: Upbit, Bithumb, Coinone
- Outputs:
  out/history/
    ├─ krw_liq_snapshots.csv
    ├─ krw_liq_daily.csv
    ├─ krw_liq_weekly.json
    └─ krw_liq_weekly_top10.json
"""

import csv
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import requests

# =========================
# Time / Paths
# =========================
KST = timezone(timedelta(hours=9))

BASE_DIR = Path(__file__).resolve().parent.parent
OUT_DIR = BASE_DIR / "out"
HIST_DIR = OUT_DIR / "history"
HIST_DIR.mkdir(parents=True, exist_ok=True)

SNAPSHOT_CSV = HIST_DIR / "krw_liq_snapshots.csv"
DAILY_CSV = HIST_DIR / "krw_liq_daily.csv"
WEEKLY_JSON = HIST_DIR / "krw_liq_weekly.json"
WEEKLY_TOP10_JSON = HIST_DIR / "krw_liq_weekly_top10.json"

# =========================
# API Endpoints
# =========================
UPBIT_MARKETS = "https://api.upbit.com/v1/market/all"
UPBIT_TICKER = "https://api.upbit.com/v1/ticker"

BITHUMB_TICKER_ALL = "https://api.bithumb.com/public/ticker/ALL_KRW"

COINONE_TICKER = "https://api.coinone.co.kr/public/v2/ticker_new/KRW"

# =========================
# Helpers
# =========================
def now_kst():
    return datetime.now(tz=KST)

def iso_week(date_str: str) -> str:
    d = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=KST)
    y, w, _ = d.isocalendar()
    return f"{y}-W{w:02d}"

def http_get(url, params=None):
    for _ in range(3):
        try:
            r = requests.get(url, params=params, timeout=20)
            r.raise_for_status()
            return r.json()
        except Exception:
            time.sleep(1)
    raise RuntimeError(f"Failed request: {url}")

# =========================
# Fetch per exchange (pairs)
# =========================
def fetch_upbit_pairs() -> List[Tuple[str, float]]:
    markets = http_get(UPBIT_MARKETS, {"isDetails": "false"})
    krw_markets = [m["market"] for m in markets if m["market"].startswith("KRW-")]
    out = []
    for i in range(0, len(krw_markets), 100):
        chunk = krw_markets[i:i+100]
        tickers = http_get(UPBIT_TICKER, {"markets": ",".join(chunk)})
        for t in tickers:
            out.append((t["market"], float(t.get("acc_trade_price_24h", 0))))
        time.sleep(0.1)
    return out

def fetch_bithumb_pairs() -> List[Tuple[str, float]]:
    j = http_get(BITHUMB_TICKER_ALL)
    data = j.get("data", {})
    out = []
    for sym, v in data.items():
        if sym == "date":
            continue
        val = (
            v.get("acc_trade_value_24H")
            or v.get("acc_trade_value")
            or 0
        )
        out.append((f"KRW-{sym}", float(val)))
    return out

def fetch_coinone_pairs() -> List[Tuple[str, float]]:
    j = http_get(COINONE_TICKER)
    out = []
    for t in j.get("tickers", []):
        sym = t.get("target_currency", "").upper()
        val = float(t.get("quote_volume", 0))
        out.append((f"KRW-{sym}", val))
    return out

# =========================
# Aggregation logic
# =========================
def compute_total(pairs):
    return sum(v for _, v in pairs)

def compute_top10_rest(pairs):
    agg: Dict[str, float] = {}
    for k, v in pairs:
        agg[k] = agg.get(k, 0) + v
    items = sorted(agg.items(), key=lambda x: x[1], reverse=True)
    total = sum(v for _, v in items)
    top10 = sum(v for _, v in items[:10])
    rest = max(0, total - top10)
    return total, top10, rest

# =========================
# Snapshot → Daily → Weekly
# =========================
def append_snapshot(row: dict):
    exists = SNAPSHOT_CSV.exists()
    with SNAPSHOT_CSV.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=row.keys())
        if not exists:
            w.writeheader()
        w.writerow(row)

def rebuild_daily():
    rows = []
    with SNAPSHOT_CSV.open() as f:
        for r in csv.DictReader(f):
            rows.append(r)

    by_date: Dict[str, List[dict]] = {}
    for r in rows:
        d = r["date"]
        by_date.setdefault(d, []).append(r)

    with DAILY_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader()
        for d in sorted(by_date):
            # 대표 스냅샷: 09시 기준 가장 가까운 것
            w.writerow(by_date[d][0])

def rebuild_weekly():
    rows = []
    with DAILY_CSV.open() as f:
        for r in csv.DictReader(f):
            rows.append(r)

    by_week: Dict[str, dict] = {}
    for r in rows:
        wk = iso_week(r["date"])
        b = by_week.setdefault(wk, {
            "week": wk,
            "upbit": 0, "bithumb": 0, "coinone": 0,
            "top10": 0, "rest": 0
        })
        b["upbit"] += float(r["upbit_total"])
        b["bithumb"] += float(r["bithumb_total"])
        b["coinone"] += float(r["coinone_total"])
        b["top10"] += float(r["combined_top10"])
        b["rest"] += float(r["combined_rest"])

    weeks = sorted(by_week.keys())
    out_main = []
    out_top = []

    prev_total = None
    for wk in weeks:
        b = by_week[wk]
        total = b["upbit"] + b["bithumb"] + b["coinone"]
        wow = None if prev_total is None else (total / prev_total - 1) * 100
        prev_total = total

        out_main.append({
            "week": wk,
            "upbit": b["upbit"],
            "bithumb": b["bithumb"],
            "coinone": b["coinone"],
            "wow_pct": wow
        })

        out_top.append({
            "week": wk,
            "top10": b["top10"],
            "rest": b["rest"]
        })

    WEEKLY_JSON.write_text(json.dumps(out_main, ensure_ascii=False, indent=2))
    WEEKLY_TOP10_JSON.write_text(json.dumps(out_top, ensure_ascii=False, indent=2))

# =========================
# Main run
# =========================
def run():
    dt = now_kst()
    date = dt.strftime("%Y-%m-%d")

    up = fetch_upbit_pairs()
    bt = fetch_bithumb_pairs()
    co = fetch_coinone_pairs()

    up_total = compute_total(up)
    bt_total = compute_total(bt)
    co_total = compute_total(co)

    combined_pairs = up + bt + co
    _, top10, rest = compute_top10_rest(combined_pairs)

    append_snapshot({
        "date": date,
        "upbit_total": up_total,
        "bithumb_total": bt_total,
        "coinone_total": co_total,
        "combined_top10": top10,
        "combined_rest": rest
    })

    rebuild_daily()
    rebuild_weekly()

    print("[OK] KRW liquidity pipeline updated")

if __name__ == "__main__":
    run()
