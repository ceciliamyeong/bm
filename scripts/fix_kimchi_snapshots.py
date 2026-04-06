#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fix_kimchi_snapshots.py
USDKRW=1450 으로 잘못 찍힌 스냅샷을 yfinance 실제 환율로 재계산
"""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yfinance as yf

KST = timezone(timedelta(hours=9))
BASE_DIR = Path(__file__).resolve().parent.parent
KIMCHI_SNAPSHOTS_JSON = BASE_DIR / "out" / "history" / "kimchi_snapshots.json"


def kimchi_premium_pct(krw_price: float, usdt_price: float, usdkrw: float) -> float:
    fair_krw = float(usdt_price) * float(usdkrw)
    if fair_krw <= 0:
        return 0.0
    return (float(krw_price) - fair_krw) / fair_krw * 100.0


def fetch_fx_history() -> dict:
    """yfinance 일별 환율 (1d 봉 → 날짜별 종가)"""
    ticker = yf.Ticker("USDKRW=X")
    h = ticker.history(start="2026-02-06", end="2026-04-07", interval="1d")
    h = h["Close"].dropna()

    fx_map = {}
    for ts, val in h.items():
        date_str = ts.strftime("%Y-%m-%d")
        fx_map[date_str] = round(float(val), 2)

    print(f"환율 데이터 포인트: {len(fx_map)}개")
    for d, v in sorted(fx_map.items()):
        print(f"  {d}: {v}")
    return fx_map


def find_nearest_rate(fx_map: dict, target_ts: float) -> float:
    """타임스탬프 → 날짜 변환 후 해당 날짜 또는 가장 가까운 영업일 환율 반환"""
    if not fx_map:
        return 0.0
    dt = datetime.fromtimestamp(target_ts, tz=KST)
    date_str = dt.strftime("%Y-%m-%d")

    if date_str in fx_map:
        return fx_map[date_str]

    # 주말/공휴일 → 최대 4일 전까지 탐색
    for delta in range(1, 5):
        d_prev = (dt - timedelta(days=delta)).strftime("%Y-%m-%d")
        if d_prev in fx_map:
            print(f"    [{date_str}] → {d_prev} 환율 사용 (주말/공휴일)")
            return fx_map[d_prev]

    return 0.0


def run():
    snapshots = json.loads(KIMCHI_SNAPSHOTS_JSON.read_text(encoding="utf-8"))

    targets = [s for s in snapshots if s.get("prices", {}).get("fx", {}).get("USDKRW") == 1450.0]
    print(f"수정 대상 스냅샷: {len(targets)}개")
    for t in targets:
        print(f"  {t['timestamp_label']}")

    if not targets:
        print("수정할 스냅샷 없음")
        return

    print("\nyfinance에서 환율 히스토리 가져오는 중...")
    fx_map = fetch_fx_history()

    for snap in snapshots:
        if snap.get("prices", {}).get("fx", {}).get("USDKRW") != 1450.0:
            continue

        ts_str = snap["timestamp_kst"]
        dt = datetime.fromisoformat(ts_str)
        unix_ts = dt.timestamp()

        rate = find_nearest_rate(fx_map, unix_ts)
        if not (900 <= rate <= 2000):
            print(f"  [SKIP] {snap['timestamp_label']} 환율 이상: {rate}")
            continue

        print(f"\n  {snap['timestamp_label']}")
        print(f"    환율: 1450.0 → {rate:.1f}")

        prices = snap["prices"]
        upbit = prices["upbit"]
        binance = prices["binance"]

        krw_btc = upbit["KRW-BTC"]
        krw_eth = upbit["KRW-ETH"]
        krw_xrp = upbit["KRW-XRP"]
        usdt_btc = binance["BTCUSDT"]
        usdt_eth = binance["ETHUSDT"]
        usdt_xrp = binance["XRPUSDT"]

        old_btc = snap["kimchi_premium_pct"]["BTC"]
        old_eth = snap["kimchi_premium_pct"]["ETH"]
        old_xrp = snap["kimchi_premium_pct"]["XRP"]

        new_btc = round(kimchi_premium_pct(krw_btc, usdt_btc, rate), 3)
        new_eth = round(kimchi_premium_pct(krw_eth, usdt_eth, rate), 3)
        new_xrp = round(kimchi_premium_pct(krw_xrp, usdt_xrp, rate), 3)

        print(f"    BTC: {old_btc} → {new_btc}")
        print(f"    ETH: {old_eth} → {new_eth}")
        print(f"    XRP: {old_xrp} → {new_xrp}")

        snap["prices"]["fx"]["USDKRW"] = round(rate, 1)
        snap["prices"]["fx"]["source"] = "yfinance:USDKRW=X (retrofix)"
        snap["kimchi_premium_pct"]["BTC"] = new_btc
        snap["kimchi_premium_pct"]["ETH"] = new_eth
        snap["kimchi_premium_pct"]["XRP"] = new_xrp

    KIMCHI_SNAPSHOTS_JSON.write_text(
        json.dumps(snapshots, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    print("\n[OK] kimchi_snapshots.json 저장 완료")


if __name__ == "__main__":
    run()
