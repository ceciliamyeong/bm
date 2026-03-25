#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
backfill_korea_daily.py
───────────────────────
기존 krw_24h_snapshots.json + kimchi_snapshots.json 으로
korea_daily.csv 60일치 백필 (1회 실행용)

실행:
  python scripts/backfill_korea_daily.py
"""

import json
import pandas as pd
from pathlib import Path
from collections import defaultdict

# ── 경로 ──────────────────────────────────────────────────────
ROOT        = Path(__file__).resolve().parent.parent
HIST_DIR    = ROOT / "out" / "history"
KRW_JSON    = HIST_DIR / "krw_24h_snapshots.json"
KIMCHI_JSON = HIST_DIR / "kimchi_snapshots.json"
OUT_CSV     = HIST_DIR / "korea_daily.csv"

COLUMNS = [
    "date",
    "krw_total", "upbit", "bithumb", "coinone",
    "stable_dom_pct", "usdt_vol", "usdc_vol",
    "top10_share_pct",
    "kimchi_btc", "kimchi_eth", "kimchi_xrp",
    "kimchi_driver", "kimchi_type",
    "usdkrw",
]


def aggregate_krw(snapshots: list) -> dict:
    """
    일별 스냅샷 여러 개 → 하루 대표값 1개
    rolling 24h 데이터는 마지막 스냅샷(가장 최신)을 대표값으로 사용
    """
    by_date = defaultdict(list)
    for snap in snapshots:
        date = snap["timestamp_kst"][:10]
        by_date[date].append(snap)

    result = {}
    for date, snaps in by_date.items():
        # 마지막 스냅샷 = 그날 가장 최신 rolling 24h 값
        s = snaps[-1]
        totals     = s.get("totals", {})
        stables    = s.get("stablecoins", {})
        by_asset   = stables.get("by_asset", {})
        top10      = s.get("top10", {})

        result[date] = {
            "krw_total":      round(totals.get("combined_24h", 0)),
            "upbit":          round(totals.get("upbit_24h", 0)),
            "bithumb":        round(totals.get("bithumb_24h", 0)),
            "coinone":        round(totals.get("coinone_24h", 0)),
            "stable_dom_pct": round(stables.get("stable_dominance_pct", 0), 4),
            "usdt_vol":       round(by_asset.get("USDT", 0)),
            "usdc_vol":       round(by_asset.get("USDC", 0)),
            "top10_share_pct":round(top10.get("top10_share_pct", 0), 4),
        }
    return result


def aggregate_kimchi(snapshots: list) -> dict:
    """
    일별 스냅샷 여러 개 → 하루 대표값 1개
    김치 프리미엄은 일별 평균값 사용 (변동성 반영)
    """
    by_date = defaultdict(list)
    for snap in snapshots:
        date = snap["timestamp_kst"][:10]
        by_date[date].append(snap)

    result = {}
    for date, snaps in by_date.items():
        btc_vals = [s["kimchi_premium_pct"]["BTC"] for s in snaps if s.get("kimchi_premium_pct", {}).get("BTC") is not None]
        eth_vals = [s["kimchi_premium_pct"]["ETH"] for s in snaps if s.get("kimchi_premium_pct", {}).get("ETH") is not None]
        xrp_vals = [s["kimchi_premium_pct"]["XRP"] for s in snaps if s.get("kimchi_premium_pct", {}).get("XRP") is not None]

        # driver는 마지막 스냅샷 기준
        last = snaps[-1]
        driver_dict = last.get("driver_share_pct", {})
        driver = max(driver_dict, key=driver_dict.get) if driver_dict else None
        kimchi_type = (last.get("smart_kimchi") or {}).get("type")

        # usdkrw는 마지막 스냅샷 기준
        usdkrw = (last.get("prices") or {}).get("fx", {}).get("USDKRW")

        result[date] = {
            "kimchi_btc":    round(sum(btc_vals) / len(btc_vals), 4) if btc_vals else None,
            "kimchi_eth":    round(sum(eth_vals) / len(eth_vals), 4) if eth_vals else None,
            "kimchi_xrp":    round(sum(xrp_vals) / len(xrp_vals), 4) if xrp_vals else None,
            "kimchi_driver": driver,
            "kimchi_type":   kimchi_type,
            "usdkrw":        round(float(usdkrw), 2) if usdkrw else None,
        }
    return result


def main():
    print("=" * 55)
    print("korea_daily.csv 백필 시작")
    print("=" * 55)

    # 1) 로드
    if not KRW_JSON.exists():
        print(f"[ERROR] {KRW_JSON} 없음")
        return
    if not KIMCHI_JSON.exists():
        print(f"[ERROR] {KIMCHI_JSON} 없음")
        return

    with open(KRW_JSON, encoding="utf-8") as f:
        krw_snaps = json.load(f)
    with open(KIMCHI_JSON, encoding="utf-8") as f:
        kimchi_snaps = json.load(f)

    print(f"[OK] krw 스냅샷: {len(krw_snaps)}개")
    print(f"[OK] kimchi 스냅샷: {len(kimchi_snaps)}개")

    # 2) 일별 집계
    krw_by_date    = aggregate_krw(krw_snaps)
    kimchi_by_date = aggregate_kimchi(kimchi_snaps)

    # 3) 날짜 합집합으로 병합
    all_dates = sorted(set(krw_by_date) | set(kimchi_by_date))
    rows = []
    for date in all_dates:
        row = {"date": date}
        row.update(krw_by_date.get(date, {
            "krw_total": None, "upbit": None, "bithumb": None, "coinone": None,
            "stable_dom_pct": None, "usdt_vol": None, "usdc_vol": None,
            "top10_share_pct": None,
        }))
        row.update(kimchi_by_date.get(date, {
            "kimchi_btc": None, "kimchi_eth": None, "kimchi_xrp": None,
            "kimchi_driver": None, "kimchi_type": None, "usdkrw": None,
        }))
        rows.append(row)

    df = pd.DataFrame(rows, columns=COLUMNS)

    # 4) 기존 파일 있으면 병합 (daily append 데이터 보호)
    if OUT_CSV.exists():
        existing = pd.read_csv(OUT_CSV, dtype={"date": str})
        df = pd.concat([df, existing]).drop_duplicates(
            subset="date", keep="first"
        ).sort_values("date").reset_index(drop=True)
        print(f"[INFO] 기존 {OUT_CSV.name} 와 병합")

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_CSV, index=False, encoding="utf-8")

    print()
    print("=" * 55)
    print(f"[완료] {OUT_CSV}")
    print(f"  전체 행수  : {len(df)}일")
    print(f"  기간       : {df['date'].iloc[0]} ~ {df['date'].iloc[-1]}")
    print(f"  krw_total  : {df['krw_total'].notna().sum()}일 채워짐")
    print(f"  kimchi_btc : {df['kimchi_btc'].notna().sum()}일 채워짐")
    print(f"  stable_dom : {df['stable_dom_pct'].notna().sum()}일 채워짐")
    print("=" * 55)


if __name__ == "__main__":
    main()
