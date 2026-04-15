#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
update_bm20_latest.py
뉴스레터 렌더 직전 실행 — CMC API로 20개 코인 현재가를 가져와
BM20 지수 레벨과 1D 등락률을 실시간으로 갱신합니다.
의존: requests (pip install requests)
"""

import json
import os
import requests
from datetime import datetime, timedelta, timezone
from pathlib import Path

KST = timezone(timedelta(hours=9))
ROOT = Path(__file__).resolve().parent

# ── BM20 유니버스 & 가중치 ──────────────────────────────────────────
WEIGHTS = {
    "BTC":  0.30,
    "ETH":  0.20,
    "XRP":  0.05,
    "USDT": 0.05,
    "BNB":  0.05,
    "SOL":  0.35 / 15,
    "USDC": 0.35 / 15,
    "DOGE": 0.35 / 15,
    "TRX":  0.35 / 15,
    "ADA":  0.35 / 15,
    "HYPE": 0.35 / 15,
    "LINK": 0.35 / 15,
    "SUI":  0.35 / 15,
    "AVAX": 0.35 / 15,
    "XLM":  0.35 / 15,
    "BCH":  0.35 / 15,
    "HBAR": 0.35 / 15,
    "LTC":  0.35 / 15,
    "SHIB": 0.35 / 15,
    "TON":  0.35 / 15,
}
ALL_SYMBOLS = list(WEIGHTS.keys())

# ── CMC API 가격 조회 ───────────────────────────────────────────────
def fetch_cmc_prices(api_key: str) -> dict:
    """CMC /quotes/latest 로 현재가 + 24h 전 가격 한 번에 가져오기"""
    symbol_str = ",".join(ALL_SYMBOLS)
    r = requests.get(
        "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest",
        headers={"X-CMC_PRO_API_KEY": api_key},
        params={"symbol": symbol_str, "convert": "USD"},
        timeout=15,
    )
    r.raise_for_status()
    data = r.json().get("data", {})
    print(f"[INFO] CMC 응답 코인 수: {len(data)}개")

    prices = {}
    for sym, entries in data.items():
        entry = entries[0] if isinstance(entries, list) else entries
        quote = entry.get("quote", {}).get("USD", {})
        price = quote.get("price")
        chg24 = quote.get("percent_change_24h")
        if price is None:
            continue
        price = float(price)
        chg24 = float(chg24) if chg24 is not None else 0.0
        # CMC percent_change_24h 기준으로 24h 전 가격 역산
        prev_price = price / (1.0 + chg24 / 100.0) if chg24 != -100 else price
        prices[sym.upper()] = {"current": price, "prev": prev_price}

    return prices

# ── 메인 ───────────────────────────────────────────────────────────
def main():
    now_kst = datetime.now(KST)
    print(f"[START] update_bm20_latest.py — {now_kst.strftime('%Y-%m-%d %H:%M:%S')} KST")

    api_key = os.getenv("CMC_API_KEY", "")
    if not api_key:
        print("[ERROR] CMC_API_KEY 없음. 종료.")
        return

    # bm20_latest.json 읽기
    latest_path = ROOT / "bm20_latest.json"
    try:
        existing = json.loads(latest_path.read_text(encoding="utf-8"))
    except Exception:
        print("[ERROR] bm20_latest.json 읽기 실패. 종료.")
        return

    # CMC 현재가 조회
    try:
        prices = fetch_cmc_prices(api_key)
    except Exception as e:
        print(f"[ERROR] CMC 가격 조회 실패: {e}. 종료.")
        return

    # 현재/24h 전 포트폴리오 가치 계산
    curr_value = sum(
        WEIGHTS[s] * prices[s]["current"]
        for s in ALL_SYMBOLS if s in prices
    )
    prev_value = sum(
        WEIGHTS[s] * prices[s]["prev"]
        for s in ALL_SYMBOLS if s in prices
    )

    if not curr_value or not prev_value:
        print("[ERROR] 포트폴리오 가치 계산 실패. 종료.")
        return

    # bm20_series.json 마지막 레벨로 base_value 역산 → 레벨 연속성 유지
    last_level = None
    try:
        series_path = ROOT / "bm20_series.json"
        series = json.loads(series_path.read_text(encoding="utf-8"))
        if isinstance(series, list):
            last_level = float(series[-1]["level"])
            print(f"[INFO] 시리즈 마지막 레벨: {last_level} ({series[-1]['date']})")
        elif isinstance(series, dict) and "series" in series:
            s = series["series"]
            last_level = float(s[-1]["level"])
            print(f"[INFO] 시리즈 마지막 레벨: {last_level} ({s[-1]['date']})")
    except Exception as e:
        print(f"[WARN] bm20_series.json 읽기 실패: {e} → 기존 bm20Level 사용")
        last_level = existing.get("bm20Level")

    if not last_level:
        print("[ERROR] 기준 레벨을 가져올 수 없습니다. 종료.")
        return

    # base_value 역산 (시리즈 마지막 레벨 기준)
    base_value = curr_value / last_level * 100

    # 실시간 레벨: 시리즈 마지막 레벨에서 연속
    bm20_level = round(curr_value / base_value * 100, 4)  # == last_level

    # 1D: CMC 24h 기준 (정확한 24h 변동)
    bm20_prev  = round(prev_value / base_value * 100, 4)
    ret_1d     = round((bm20_level / bm20_prev - 1), 8)
    point_chg  = round(bm20_level - bm20_prev, 4)

    # bm20_latest.json 갱신
    existing["bm20Level"]       = bm20_level
    existing["bm20PrevLevel"]   = bm20_prev
    existing["bm20PointChange"] = point_chg
    existing["bm20ChangePct"]   = ret_1d
    existing["returns"]["1D"]   = ret_1d
    existing["updatedAt"]       = now_kst.strftime("%Y-%m-%dT%H:%M:%S+09:00")

    latest_path.write_text(
        json.dumps(existing, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"[OK] bm20_latest.json 갱신 — level={bm20_level}, 1D={ret_1d*100:+.4f}%")

    missing = [s for s in ALL_SYMBOLS if s not in prices]
    if missing:
        print(f"[WARN] 가격 없는 코인: {missing}")

if __name__ == "__main__":
    main()
