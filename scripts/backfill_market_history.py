#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
backfill_market_history.py
──────────────────────────
market_history.csv 의 빈 구간(03-26~04-06)을 채웁니다.

채울 수 있는 컬럼:
  - bm20_level, bm20_chg_pct → backfill_current_basket.csv
  - kimchi_pct, usdkrw       → kimchi_snapshots.json (날짜별 평균)
  - sentiment_value/label     → alternative.me API (과거 조회)
  - 나머지                    → null

실행 방법:
  cd C:\\Users\\econo\\bm
  pip install requests
  python backfill_market_history.py
"""

import csv
import json
import time
import requests
from pathlib import Path
from datetime import datetime, timedelta

ROOT     = Path(__file__).resolve().parent.parent  # scripts/ 의 상위 = 레포 루트
BACKFILL = ROOT / "out" / "backfill_current_basket.csv"
KIMCHI   = ROOT / "out" / "history" / "kimchi_snapshots.json"
MARKET   = ROOT / "out" / "history" / "market_history.csv"

COLUMNS = [
    "date", "bm20_level", "bm20_chg_pct",
    "sentiment_value", "sentiment_label",
    "kimchi_pct", "usdkrw", "k_share_percent",
    "btc_funding_bin", "eth_funding_bin",
    "btc_funding_byb", "eth_funding_byb",
    "btc_dominance",
]


def load_backfill() -> dict:
    """date → {index, ret}"""
    result = {}
    with BACKFILL.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames and "date" in reader.fieldnames:
            for row in reader:
                d = row.get("date", "").strip()
                if d:
                    result[d] = {
                        "index": float(row.get("index", 0) or 0),
                        "ret":   float(row.get("ret", 0) or 0),
                    }
        else:
            # 헤더 없는 경우
            f.seek(0)
            for line in f:
                parts = line.strip().split(",")
                if len(parts) >= 3:
                    result[parts[0]] = {
                        "index": float(parts[1]),
                        "ret":   float(parts[2]),
                    }
    return result


def load_kimchi_by_date() -> dict:
    """date → {kimchi_pct 평균, usdkrw 평균}"""
    if not KIMCHI.exists():
        return {}
    snaps = json.loads(KIMCHI.read_text(encoding="utf-8"))
    by_date = {}
    for s in snaps:
        date = s.get("timestamp_kst", "")[:10]
        if not date:
            continue
        btc_pct = s.get("kimchi_premium_pct", {}).get("BTC")
        usdkrw  = s.get("prices", {}).get("fx", {}).get("USDKRW")
        if btc_pct is None or usdkrw is None:
            continue
        by_date.setdefault(date, {"kimchi": [], "usdkrw": []})
        by_date[date]["kimchi"].append(float(btc_pct))
        by_date[date]["usdkrw"].append(float(usdkrw))

    result = {}
    for date, vals in by_date.items():
        result[date] = {
            "kimchi_pct": round(sum(vals["kimchi"]) / len(vals["kimchi"]), 4),
            "usdkrw":     round(sum(vals["usdkrw"]) / len(vals["usdkrw"]), 2),
        }
    return result


def fetch_sentiment_history(start_date: str, end_date: str) -> dict:
    """alternative.me FNG API로 과거 공포탐욕 지수 가져오기"""
    result = {}
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt   = datetime.strptime(end_date, "%Y-%m-%d")
        days = (end_dt - start_dt).days + 1

        r = requests.get(
            "https://api.alternative.me/fng/",
            params={"limit": days + 5, "format": "json"},
            timeout=10,
        )
        r.raise_for_status()
        data = r.json().get("data", [])
        for item in data:
            ts   = int(item.get("timestamp", 0))
            date = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
            if start_date <= date <= end_date:
                result[date] = {
                    "sentiment_value": int(item.get("value", 0)),
                    "sentiment_label": item.get("value_classification", ""),
                }
        print(f"[INFO] sentiment 조회: {len(result)}개")
    except Exception as e:
        print(f"[WARN] sentiment fetch failed: {e}")
    return result


def main():
    # 1. 기존 market_history 로드
    existing = {}
    with MARKET.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            existing[row["date"]] = row

    last_date = max(existing.keys())
    print(f"[INFO] market_history 마지막: {last_date} / 총 {len(existing)}행")

    # 2. 채울 날짜 목록
    last_dt  = datetime.strptime(last_date, "%Y-%m-%d")
    today_dt = datetime.now()
    missing  = []
    d = last_dt + timedelta(days=1)
    while d <= today_dt:
        date_str = d.strftime("%Y-%m-%d")
        if date_str not in existing:
            missing.append(date_str)
        d += timedelta(days=1)

    print(f"[INFO] 채울 날짜: {len(missing)}개 → {missing}")
    if not missing:
        print("[OK] 빈 구간 없음")
        return

    # 3. 데이터 소스 로드
    backfill = load_backfill()
    kimchi   = load_kimchi_by_date()
    sentiment = fetch_sentiment_history(missing[0], missing[-1])

    # 4. 빈 날짜 채우기
    added = 0
    for date_str in missing:
        bf = backfill.get(date_str, {})
        ki = kimchi.get(date_str, {})
        se = sentiment.get(date_str, {})

        bm20_level   = round(bf["index"], 4) if bf.get("index") else None
        bm20_chg_pct = round(bf["ret"] * 100, 4) if bf.get("ret") is not None else None

        row = {
            "date":             date_str,
            "bm20_level":       bm20_level,
            "bm20_chg_pct":     bm20_chg_pct,
            "sentiment_value":  se.get("sentiment_value"),
            "sentiment_label":  se.get("sentiment_label"),
            "kimchi_pct":       ki.get("kimchi_pct"),
            "usdkrw":           ki.get("usdkrw"),
            "k_share_percent":  None,
            "btc_funding_bin":  None,
            "eth_funding_bin":  None,
            "btc_funding_byb":  None,
            "eth_funding_byb":  None,
            "btc_dominance":    None,
        }
        existing[date_str] = row
        print(f"  [ADD] {date_str}: bm20={bm20_level} / kimchi={ki.get('kimchi_pct')} / sentiment={se.get('sentiment_value')}")
        added += 1

    # 5. 저장
    sorted_rows = [existing[d] for d in sorted(existing.keys())]
    with MARKET.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        for row in sorted_rows:
            writer.writerow({col: row.get(col, "") for col in COLUMNS})

    print(f"\n[OK] {added}개 날짜 추가 → market_history.csv 총 {len(sorted_rows)}행")


if __name__ == "__main__":
    main()
