#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fix_korea_daily.py
──────────────────
korea_daily.csv 에서 usdkrw=1450.0 으로 잘못 찍힌 행을
yfinance 일별 환율로 재계산 (kimchi_btc/eth/xrp, usdkrw 컬럼 수정)

kimchi_snapshots.json 에서 해당 날짜 스냅샷 가격 데이터를 참조해
kimchi 프리미엄을 재계산합니다.
"""

import json
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta, timezone
from pathlib import Path

KST     = timezone(timedelta(hours=9))
ROOT    = Path(__file__).resolve().parent.parent
HIST_DIR = ROOT / "out" / "history"
OUT_CSV  = HIST_DIR / "korea_daily.csv"
KIMCHI_JSON = HIST_DIR / "kimchi_snapshots.json"


def kimchi_premium_pct(krw_price: float, usdt_price: float, usdkrw: float) -> float:
    fair_krw = float(usdt_price) * float(usdkrw)
    if fair_krw <= 0:
        return 0.0
    return (float(krw_price) - fair_krw) / fair_krw * 100.0


def fetch_fx_daily() -> dict:
    """yfinance 일별 환율 (1d 봉 → 날짜별 종가)"""
    ticker = yf.Ticker("USDKRW=X")
    h = ticker.history(start="2026-02-06", end="2026-04-07", interval="1d")
    h = h["Close"].dropna()

    fx_map = {}
    for ts, val in h.items():
        date_str = ts.strftime("%Y-%m-%d")
        fx_map[date_str] = round(float(val), 2)

    print(f"[FX] 일별 환율 데이터: {len(fx_map)}개")
    for d, v in sorted(fx_map.items()):
        print(f"  {d}: {v}")
    return fx_map


def load_kimchi_by_date() -> dict:
    """kimchi_snapshots.json → 날짜별 스냅샷 목록"""
    if not KIMCHI_JSON.exists():
        return {}
    snaps = json.loads(KIMCHI_JSON.read_text(encoding="utf-8"))
    by_date = {}
    for s in snaps:
        date = s.get("timestamp_kst", "")[:10]
        if date:
            by_date.setdefault(date, []).append(s)
    return by_date


def recalc_kimchi(snaps: list, usdkrw: float) -> dict:
    """스냅샷 목록과 수정된 환율로 kimchi 평균 재계산"""
    btc_vals, eth_vals, xrp_vals = [], [], []
    for s in snaps:
        upbit   = s.get("prices", {}).get("upbit", {})
        binance = s.get("prices", {}).get("binance", {})
        krw_btc  = upbit.get("KRW-BTC", 0)
        krw_eth  = upbit.get("KRW-ETH", 0)
        krw_xrp  = upbit.get("KRW-XRP", 0)
        usdt_btc = binance.get("BTCUSDT", 0)
        usdt_eth = binance.get("ETHUSDT", 0)
        usdt_xrp = binance.get("XRPUSDT", 0)
        if krw_btc and usdt_btc:
            btc_vals.append(kimchi_premium_pct(krw_btc, usdt_btc, usdkrw))
        if krw_eth and usdt_eth:
            eth_vals.append(kimchi_premium_pct(krw_eth, usdt_eth, usdkrw))
        if krw_xrp and usdt_xrp:
            xrp_vals.append(kimchi_premium_pct(krw_xrp, usdt_xrp, usdkrw))

    return {
        "kimchi_btc": round(sum(btc_vals) / len(btc_vals), 4) if btc_vals else None,
        "kimchi_eth": round(sum(eth_vals) / len(eth_vals), 4) if eth_vals else None,
        "kimchi_xrp": round(sum(xrp_vals) / len(xrp_vals), 4) if xrp_vals else None,
    }


def main():
    df = pd.read_csv(OUT_CSV, dtype={"date": str})
    print(f"[INFO] korea_daily.csv 로드: {len(df)}행")

    # 수정 대상: usdkrw == 1450.0
    mask = df["usdkrw"] == 1450.0
    targets = df[mask]["date"].tolist()
    print(f"[INFO] 수정 대상 날짜: {len(targets)}개")
    for d in targets:
        print(f"  {d}")

    if not targets:
        print("[OK] 수정할 행 없음")
        return

    fx_map      = fetch_fx_daily()
    kimchi_map  = load_kimchi_by_date()

    for date in targets:
        rate = fx_map.get(date)
        if not rate:
            # 주말/공휴일 → 가장 가까운 날짜 환율 사용
            for delta in range(1, 5):
                d_prev = (datetime.strptime(date, "%Y-%m-%d") - timedelta(days=delta)).strftime("%Y-%m-%d")
                if fx_map.get(d_prev):
                    rate = fx_map[d_prev]
                    print(f"  [{date}] 주말/공휴일 → {d_prev} 환율 사용: {rate}")
                    break

        if not rate or not (900 <= rate <= 2000):
            print(f"  [SKIP] {date} 환율 없음 또는 이상값")
            continue

        print(f"\n  {date}: 1450.0 → {rate}")

        # kimchi 재계산
        snaps = kimchi_map.get(date, [])
        if snaps:
            new_kimchi = recalc_kimchi(snaps, rate)
            print(f"    BTC: {df.loc[df['date']==date, 'kimchi_btc'].values[0]} → {new_kimchi['kimchi_btc']}")
            print(f"    ETH: {df.loc[df['date']==date, 'kimchi_eth'].values[0]} → {new_kimchi['kimchi_eth']}")
            print(f"    XRP: {df.loc[df['date']==date, 'kimchi_xrp'].values[0]} → {new_kimchi['kimchi_xrp']}")
            df.loc[df["date"] == date, "kimchi_btc"] = new_kimchi["kimchi_btc"]
            df.loc[df["date"] == date, "kimchi_eth"] = new_kimchi["kimchi_eth"]
            df.loc[df["date"] == date, "kimchi_xrp"] = new_kimchi["kimchi_xrp"]
        else:
            print(f"    [WARN] {date} kimchi 스냅샷 없음 → kimchi 값 유지")

        df.loc[df["date"] == date, "usdkrw"] = rate

    df.to_csv(OUT_CSV, index=False, encoding="utf-8")
    print(f"\n[OK] korea_daily.csv 저장 완료 ({len(df)}행)")


if __name__ == "__main__":
    main()
