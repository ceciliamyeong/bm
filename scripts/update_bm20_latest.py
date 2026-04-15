#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
update_bm20_latest.py
뉴스레터 렌더 직전에 실행 — BM20 지수 레벨과 1D 등락률만 빠르게 계산해서
bm20_latest.json 을 갱신합니다.
의존: requests (pip install requests)
"""

import json
import requests
from datetime import datetime, timedelta, timezone
from pathlib import Path

KST = timezone(timedelta(hours=9))
ROOT = Path(__file__).resolve().parent

# ── BM20 유니버스 & 가중치 ──────────────────────────────────────────
FIXED = {
    "BTC-USD": 0.30,
    "ETH-USD": 0.20,
    "XRP-USD": 0.05,
    "USDT-USD": 0.05,
    "BNB-USD": 0.05,
}
EQUAL = [
    "SOL-USD", "USDC-USD", "DOGE-USD", "TRX-USD", "ADA-USD",
    "HYPE-USD", "LINK-USD", "SUI20947-USD", "AVAX-USD", "XLM-USD",
    "BCH-USD", "HBAR-USD", "LTC-USD", "SHIB-USD", "TON11419-USD",
]
EQUAL_W = round(0.35 / len(EQUAL), 8)
WEIGHTS = {**FIXED, **{s: EQUAL_W for s in EQUAL}}
ALL_SYMBOLS = list(WEIGHTS.keys())

# ── Yahoo Finance 가격 조회 ──────────────────────────────────────────
def fetch_prices(symbols: list[str]) -> dict[str, dict]:
    """Yahoo Finance v8 API로 현재가 + 전일 종가를 가져옵니다."""
    prices = {}
    for sym in symbols:
        try:
            r = requests.get(
                f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}",
                headers={"User-Agent": "Mozilla/5.0"},
                params={"interval": "1d", "range": "5d"},
                timeout=10,
            )
            r.raise_for_status()
            result = r.json()["chart"]["result"][0]
            closes = [c for c in result["indicators"]["quote"][0]["close"] if c is not None]
            if len(closes) >= 2:
                prices[sym] = {"current": closes[-1], "prev": closes[-2]}
            elif len(closes) == 1:
                prices[sym] = {"current": closes[-1], "prev": closes[-1]}
        except Exception as e:
            print(f"[WARN] {sym} fetch failed: {e}")
    return prices

# ── BM20 기준값 로드 ─────────────────────────────────────────────────
def load_base(prices: dict[str, dict]) -> float:
    """bm20_base.json 에서 기준 포트폴리오 가치를 읽습니다.
    없으면 현재 포트폴리오 가치를 기준값으로 저장합니다 (지수=100).
    """
    base_path = ROOT / "base" / "bm20_base.json"
    base_path.parent.mkdir(parents=True, exist_ok=True)
    if base_path.exists():
        try:
            return float(json.loads(base_path.read_text())["base_value"])
        except Exception:
            pass
    # 기준값 없으면 현재 포트폴리오 가치로 초기화
    base_value = sum(
        WEIGHTS[s] * prices[s]["current"]
        for s in ALL_SYMBOLS if s in prices
    )
    base_path.write_text(
        json.dumps({"base_value": base_value, "base_date": datetime.now(KST).strftime("%Y-%m-%d")}),
        encoding="utf-8",
    )
    print(f"[INFO] bm20_base.json 초기화: base_value={base_value:.4f}")
    return base_value

# ── 메인 ─────────────────────────────────────────────────────────────
def main():
    now_kst = datetime.now(KST)
    print(f"[START] update_bm20_latest.py — {now_kst.strftime('%Y-%m-%d %H:%M:%S')} KST")

    prices = fetch_prices(ALL_SYMBOLS)
    if not prices:
        print("[ERROR] 가격 데이터를 가져오지 못했습니다. 종료.")
        return

    base_value = load_base(prices)

    # 현재 포트폴리오 가치
    curr_value = sum(
        WEIGHTS[s] * prices[s]["current"]
        for s in ALL_SYMBOLS if s in prices
    )
    prev_value = sum(
        WEIGHTS[s] * prices[s]["prev"]
        for s in ALL_SYMBOLS if s in prices
    )

    bm20_level = round(curr_value / base_value * 100, 2) if base_value else 0
    bm20_prev  = round(prev_value / base_value * 100, 2) if base_value else 0
    ret_1d     = round((bm20_level / bm20_prev - 1) * 100, 4) if bm20_prev else 0

    # bm20_latest.json 읽어서 레벨/1D만 덮어쓰기 (나머지 필드 유지)
    latest_path = ROOT / "bm20_latest.json"
    try:
        existing = json.loads(latest_path.read_text(encoding="utf-8"))
    except Exception:
        existing = {}

    existing["bm20Level"] = bm20_level
    existing["updatedAt"] = now_kst.strftime("%Y-%m-%dT%H:%M:%S+09:00")
    existing.setdefault("returns", {})["1D"] = ret_1d

    latest_path.write_text(
        json.dumps(existing, ensure_ascii=False),
        encoding="utf-8",
    )
    print(f"[OK] bm20_latest.json 갱신 — level={bm20_level}, 1D={ret_1d:+.4f}%")

if __name__ == "__main__":
    main()
