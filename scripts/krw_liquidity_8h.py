#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
KRW Liquidity Collector (8-hour snapshots) + Daily/Weekly aggregates
- Sources: Upbit, Bithumb, Coinone (public endpoints)
- Snapshot frequency: run via cron every 8 hours (e.g., 01:05 / 09:05 / 17:05 KST)
- Output:
  out/history/krw_liq_snapshots.csv  (timestamp-level, 3 exchanges totals + top10 share)
  out/history/krw_liq_daily.csv      (calendar daily representative snapshot near 09:00 KST)
  out/history/krw_liq_weekly.csv     (ISO week aggregates + WoW)
  out/history/krw_liq_weekly.json    (for ECharts stacked bar; exchange-based)
"""

import csv
import json
import math
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import requests

KST = timezone(timedelta(hours=9))

# ----------------------------
# Endpoints (public)
# ----------------------------
UPBIT_MARKET_ALL = "https://api.upbit.com/v1/market/all"
UPBIT_TICKER = "https://api.upbit.com/v1/ticker"

BITHUMB_TICKER_ALL_KRW = "https://api.bithumb.com/public/ticker/ALL_KRW"

COINONE_TICKER_KRW = "https://api.coinone.co.kr/public/v2/ticker_new/KRW"


# ----------------------------
# Paths (BM20-style)
# ----------------------------
BASE_DIR = Path(__file__).resolve().parent
OUT_DIR = BASE_DIR / "out"
HIST_DIR = OUT_DIR / "history"
HIST_DIR.mkdir(parents=True, exist_ok=True)

SNAP_CSV = HIST_DIR / "krw_liq_snapshots.csv"
DAILY_CSV = HIST_DIR / "krw_liq_daily.csv"
WEEKLY_CSV = HIST_DIR / "krw_liq_weekly.csv"
WEEKLY_JSON = HIST_DIR / "krw_liq_weekly.json"


# ----------------------------
# Helpers
# ----------------------------
def now_kst() -> datetime:
    return datetime.now(tz=KST)

def ts_kst_str(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S%z")

def date_kst_str(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")

def iso_week_key(date_str: str) -> str:
    # date_str: YYYY-MM-DD in KST
    d = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=KST)
    y, w, _ = d.isocalendar()
    return f"{y}-W{w:02d}"

def safe_float(x, default=0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default

def sleep_backoff(attempt: int):
    # exponential backoff with jitter-ish
    s = min(10.0, (2 ** attempt) * 0.5)
    time.sleep(s)

def http_get_json(url: str, params: Optional[dict] = None, timeout: int = 20, max_retries: int = 4) -> dict:
    last_err = None
    for attempt in range(max_retries):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            # retry on 429/5xx
            if r.status_code in (429, 500, 502, 503, 504):
                last_err = RuntimeError(f"HTTP {r.status_code} for {url}")
                sleep_backoff(attempt)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            sleep_backoff(attempt)
    raise RuntimeError(f"Failed GET {url}: {last_err}")

def chunked(lst: List[str], n: int):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]


# ----------------------------
# Upbit
# ----------------------------
def upbit_get_krw_markets() -> List[str]:
    data = http_get_json(UPBIT_MARKET_ALL, params={"isDetails": "false"})
    mkts = [x["market"] for x in data if x.get("market", "").startswith("KRW-")]
    mkts.sort()
    return mkts

def upbit_fetch_trade_values(mkts: List[str]) -> List[Tuple[str, float]]:
    # acc_trade_price_24h: 24h 누적 거래대금 (KRW)
    out: List[Tuple[str, float]] = []
    for chunk in chunked(mkts, 100):
        data = http_get_json(UPBIT_TICKER, params={"markets": ",".join(chunk)})
        for t in data:
            m = t.get("market")
            v = safe_float(t.get("acc_trade_price_24h"), 0.0)
            if m:
                out.append((m, v))
        time.sleep(0.12)  # gentle pacing
    return out


# ----------------------------
# Bithumb
# ----------------------------
def bithumb_fetch_trade_values() -> List[Tuple[str, float]]:
    """
    Bithumb public ticker ALL_KRW response structure:
      { "status":"0000", "data": { "BTC": {...}, ..., "date":"..." } }
    Fields vary by version. Commonly includes:
      - acc_trade_value (24h 누적 거래대금, KRW)
      - acc_trade_value_24H (some variants)
    We'll try both.
    """
    j = http_get_json(BITHUMB_TICKER_ALL_KRW)
    if str(j.get("status")) != "0000":
        raise RuntimeError(f"Bithumb status not 0000: {j.get('status')}")
    data = j.get("data", {})
    out: List[Tuple[str, float]] = []
    for sym, payload in data.items():
        if sym == "date":
            continue
        if not isinstance(payload, dict):
            continue
        v = payload.get("acc_trade_value")
        if v is None:
            v = payload.get("acc_trade_value_24H")
        if v is None:
            v = payload.get("acc_trade_value_24h")
        vv = safe_float(v, 0.0)
        out.append((f"KRW-{sym}", vv))
    return out


# ----------------------------
# Coinone
# ----------------------------
def coinone_fetch_trade_values() -> List[Tuple[str, float]]:
    """
    Coinone public v2 ticker_new/KRW:
      { "result":"success", "tickers":[{ "target_currency":"btc", ..., "quote_volume":"..." }, ...] }
    quote_volume = 24h 기준 '종목 체결 금액(원화)' 성격
    """
    j = http_get_json(COINONE_TICKER_KRW)
    if str(j.get("result")) != "success":
        raise RuntimeError(f"Coinone result not success: {j.get('result')}")
    tickers = j.get("tickers", [])
    out: List[Tuple[str, float]] = []
    for t in tickers:
        if not isinstance(t, dict):
            continue
        sym = t.get("target_currency")
        if not sym:
            continue
        v = t.get("quote_volume")
        vv = safe_float(v, 0.0)
        out.append((f"KRW-{str(sym).upper()}", vv))
    return out


# ----------------------------
# Metrics
# ----------------------------
@dataclass
class ExchangeSnapshot:
    total_krw_24h: float
    top10_krw_24h: float
    top10_share_pct: float
    active_markets: int
    total_markets: int

def compute_exchange_snapshot(pairs: List[Tuple[str, float]]) -> ExchangeSnapshot:
    pairs_sorted = sorted(pairs, key=lambda x: x[1], reverse=True)
    total = sum(v for _, v in pairs_sorted)
    top10 = sum(v for _, v in pairs_sorted[:10])
    share = (top10 / total * 100.0) if total > 0 else 0.0
    active = sum(1 for _, v in pairs_sorted if v > 0)
    return ExchangeSnapshot(
        total_krw_24h=total,
        top10_krw_24h=top10,
        top10_share_pct=share,
        active_markets=active,
        total_markets=len(pairs_sorted),
    )


# ----------------------------
# CSV IO
# ----------------------------
def ensure_snapshot_header():
    if SNAP_CSV.exists():
        return
    with SNAP_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "timestamp_kst",
            # upbit
            "upbit_total_krw_24h", "upbit_top10_share_pct", "upbit_active_markets", "upbit_total_markets",
            # bithumb
            "bithumb_total_krw_24h", "bithumb_top10_share_pct", "bithumb_active_markets", "bithumb_total_markets",
            # coinone
            "coinone_total_krw_24h", "coinone_top10_share_pct", "coinone_active_markets", "coinone_total_markets",
            # combined
            "combined_total_krw_24h"
        ])

def append_snapshot_row(ts: str, up: ExchangeSnapshot, bt: ExchangeSnapshot, co: ExchangeSnapshot):
    ensure_snapshot_header()
    combined = up.total_krw_24h + bt.total_krw_24h + co.total_krw_24h
    with SNAP_CSV.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            ts,
            f"{up.total_krw_24h:.6f}", f"{up.top10_share_pct:.4f}", up.active_markets, up.total_markets,
            f"{bt.total_krw_24h:.6f}", f"{bt.top10_share_pct:.4f}", bt.active_markets, bt.total_markets,
            f"{co.total_krw_24h:.6f}", f"{co.top10_share_pct:.4f}", co.active_markets, co.total_markets,
            f"{combined:.6f}"
        ])

def load_snapshots() -> List[dict]:
    if not SNAP_CSV.exists():
        return []
    rows = []
    with SNAP_CSV.open("r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append(row)
    # ensure chronological
    rows.sort(key=lambda x: x["timestamp_kst"])
    return rows

def ensure_daily_header():
    with DAILY_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "date_kst",
            "upbit_total_proxy", "bithumb_total_proxy", "coinone_total_proxy",
            "combined_total_proxy",
            "upbit_top10_share_pct", "bithumb_top10_share_pct", "coinone_top10_share_pct"
        ])

def build_daily_from_snapshots(target_hour: int = 9) -> List[dict]:
    """
    Select 1 representative snapshot per calendar day: the one closest to target_hour (KST).
    """
    snaps = load_snapshots()
    if not snaps:
        return []

    by_date: Dict[str, List[dict]] = {}
    for s in snaps:
        # timestamp_kst like "YYYY-MM-DD HH:MM:SS+0900"
        dt = datetime.strptime(s["timestamp_kst"], "%Y-%m-%d %H:%M:%S%z").astimezone(KST)
        d = date_kst_str(dt)
        s["_dt"] = dt
        by_date.setdefault(d, []).append(s)

    daily: List[dict] = []
    for d, items in sorted(by_date.items()):
        # choose closest to target_hour
        def dist(it):
            dt = it["_dt"]
            return abs((dt.hour + dt.minute/60.0) - target_hour)

        pick = sorted(items, key=dist)[0]

        up = safe_float(pick["upbit_total_krw_24h"])
        bt = safe_float(pick["bithumb_total_krw_24h"])
        co = safe_float(pick["coinone_total_krw_24h"])
        comb = up + bt + co

        daily.append({
            "date_kst": d,
            "upbit_total_proxy": up,
            "bithumb_total_proxy": bt,
            "coinone_total_proxy": co,
            "combined_total_proxy": comb,
            "upbit_top10_share_pct": safe_float(pick["upbit_top10_share_pct"]),
            "bithumb_top10_share_pct": safe_float(pick["bithumb_top10_share_pct"]),
            "coinone_top10_share_pct": safe_float(pick["coinone_top10_share_pct"]),
        })

    return daily

def write_daily_csv(daily: List[dict]):
    ensure_daily_header()
    with DAILY_CSV.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        for r in daily:
            w.writerow([
                r["date_kst"],
                f"{r['upbit_total_proxy']:.6f}",
                f"{r['bithumb_total_proxy']:.6f}",
                f"{r['coinone_total_proxy']:.6f}",
                f"{r['combined_total_proxy']:.6f}",
                f"{r['upbit_top10_share_pct']:.4f}",
                f"{r['bithumb_top10_share_pct']:.4f}",
                f"{r['coinone_top10_share_pct']:.4f}",
            ])

def rebuild_daily_csv():
    daily = build_daily_from_snapshots(target_hour=9)
    if not daily:
        return
    # rewrite fully (so reruns are deterministic)
    ensure_daily_header()
    with DAILY_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "date_kst",
            "upbit_total_proxy", "bithumb_total_proxy", "coinone_total_proxy",
            "combined_total_proxy",
            "upbit_top10_share_pct", "bithumb_top10_share_pct", "coinone_top10_share_pct"
        ])
        for r in daily:
            w.writerow([
                r["date_kst"],
                f"{r['upbit_total_proxy']:.6f}",
                f"{r['bithumb_total_proxy']:.6f}",
                f"{r['coinone_total_proxy']:.6f}",
                f"{r['combined_total_proxy']:.6f}",
                f"{r['upbit_top10_share_pct']:.4f}",
                f"{r['bithumb_top10_share_pct']:.4f}",
                f"{r['coinone_top10_share_pct']:.4f}",
            ])

def load_daily() -> List[dict]:
    if not DAILY_CSV.exists():
        return []
    out = []
    with DAILY_CSV.open("r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            out.append({
                "date_kst": row["date_kst"],
                "upbit_total_proxy": safe_float(row["upbit_total_proxy"]),
                "bithumb_total_proxy": safe_float(row["bithumb_total_proxy"]),
                "coinone_total_proxy": safe_float(row["coinone_total_proxy"]),
                "combined_total_proxy": safe_float(row["combined_total_proxy"]),
            })
    out.sort(key=lambda x: x["date_kst"])
    return out

def aggregate_weekly_from_daily(daily: List[dict]) -> List[dict]:
    buckets: Dict[str, dict] = {}
    for r in daily:
        wk = iso_week_key(r["date_kst"])
        b = buckets.setdefault(wk, {
            "week": wk,
            "upbit": 0.0,
            "bithumb": 0.0,
            "coinone": 0.0,
            "total": 0.0,
            "days": 0,
        })
        b["upbit"] += r["upbit_total_proxy"]
        b["bithumb"] += r["bithumb_total_proxy"]
        b["coinone"] += r["coinone_total_proxy"]
        b["total"] += r["combined_total_proxy"]
        b["days"] += 1

    weeks = sorted(buckets.keys())
    out: List[dict] = []
    prev_total = None
    for wk in weeks:
        b = buckets[wk]
        wow = None
        if prev_total and prev_total > 0:
            wow = (b["total"] / prev_total - 1.0) * 100.0
        out.append({
            "week": wk,
            "upbit": b["upbit"],
            "bithumb": b["bithumb"],
            "coinone": b["coinone"],
            "total": b["total"],
            "wow_pct": wow,
            "days_covered": b["days"],
        })
        prev_total = b["total"]
    return out

def write_weekly_csv_and_json(weekly: List[dict]):
    with WEEKLY_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["week", "upbit", "bithumb", "coinone", "total", "wow_pct", "days_covered"])
        for r in weekly:
            w.writerow([
                r["week"],
                f"{r['upbit']:.6f}",
                f"{r['bithumb']:.6f}",
                f"{r['coinone']:.6f}",
                f"{r['total']:.6f}",
                "" if r["wow_pct"] is None else f"{r['wow_pct']:.4f}",
                r["days_covered"],
            ])

    # For ECharts stacked bar (exchange-based)
    slim = [{"week": r["week"], "upbit": r["upbit"], "bithumb": r["bithumb"], "coinone": r["coinone"], "wow_pct": r["wow_pct"]} for r in weekly]
    WEEKLY_JSON.write_text(json.dumps(slim, ensure_ascii=False, indent=2), encoding="utf-8")


# ----------------------------
# Main run
# ----------------------------
def fetch_all_exchanges() -> Tuple[ExchangeSnapshot, ExchangeSnapshot, ExchangeSnapshot]:
    # Upbit
    mkts = upbit_get_krw_markets()
    up_pairs = upbit_fetch_trade_values(mkts)
    up = compute_exchange_snapshot(up_pairs)

    # Bithumb
    bt_pairs = bithumb_fetch_trade_values()
    bt = compute_exchange_snapshot(bt_pairs)

    # Coinone
    co_pairs = coinone_fetch_trade_values()
    co = compute_exchange_snapshot(co_pairs)

    return up, bt, co

def run_once():
    dt = now_kst()
    ts = ts_kst_str(dt)

    up, bt, co = fetch_all_exchanges()
    append_snapshot_row(ts, up, bt, co)

    # Rebuild daily (pick closest-to-09 snapshot per day), then weekly
    rebuild_daily_csv()
    daily = load_daily()
    weekly = aggregate_weekly_from_daily(daily)
    write_weekly_csv_and_json(weekly)

    # Console summary
    last = weekly[-1] if weekly else None
    print(f"[ok] snapshot saved: {SNAP_CSV.name} @ {ts}")
    print(f"     upbit={up.total_krw_24h:,.0f}  bithumb={bt.total_krw_24h:,.0f}  coinone={co.total_krw_24h:,.0f} (KRW, 24h rolling)")
    if last:
        wow_s = "NA" if last["wow_pct"] is None else f"{last['wow_pct']:.2f}%"
        print(f"[ok] weekly updated: {WEEKLY_CSV.name}, {WEEKLY_JSON.name}")
        print(f"     last_week={last['week']} total={last['total']:,.0f} wow={wow_s} days={last['days_covered']}")

if __name__ == "__main__":
    run_once()
