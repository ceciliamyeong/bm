#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
update_bm20_latest.py
뉴스레터 렌더 직전 실행 — CMC API로 20개 코인 현재가를 가져와
전일 KST 확정 레벨에 CMC rolling 24h 수익률을 적용한 추정치를
bm20_realtime_latest.json에 저장합니다. 일별 확정 bm20_latest.json은 변경하지 않습니다.
이는 전일 확정 시점부터의 정확한 가격 수익률이 아닌 24h proxy입니다.
의존: requests (pip install requests)
"""

import json
import os
import math
import requests
from datetime import datetime, timedelta, timezone
from pathlib import Path

from bm20_level_utils import select_realtime_reference

KST = timezone(timedelta(hours=9))
ROOT = Path(__file__).resolve().parent.parent  # scripts/ 기준 상위

# ── BM20 유니버스 & 가중치 (bm20_daily.py 동일) ────────────────────
SYMBOL_MAP = {
    "bitcoin": "BTC", "ethereum": "ETH", "ripple": "XRP", "tether": "USDT",
    "binancecoin": "BNB", "solana": "SOL", "dogecoin": "DOGE",
    "tron": "TRX", "cardano": "ADA", "hyperliquid": "HYPE", "chainlink": "LINK",
    "avalanche-2": "AVAX", "stellar": "XLM", "bitcoin-cash": "BCH",
    "litecoin": "LTC", "zcash": "ZEC", "canton": "CC",
    "monero": "XMR", "near": "NEAR", "uniswap": "UNI",
}
FIXED_WEIGHTS = {
    "bitcoin": 0.32, "ethereum": 0.20, "ripple": 0.05,
    "tether": 0.05, "binancecoin": 0.05, "solana": 0.05,
}
BM20_IDS = list(SYMBOL_MAP.keys())

def compute_weights(ids: list) -> dict:
    fixed_sum = sum(FIXED_WEIGHTS.values())  # 0.72
    ids_rest = [cid for cid in ids if cid not in FIXED_WEIGHTS]
    w_rest = (1.0 - fixed_sum) / max(1, len(ids_rest))
    w = {cid: FIXED_WEIGHTS.get(cid, w_rest) for cid in ids}
    s = sum(w.values())
    if abs(s - 1.0) > 1e-12:
        w[ids[-1]] += (1.0 - s)
    return w

# ── CMC API 가격 조회 ───────────────────────────────────────────────
def fetch_cmc_prices(api_key: str) -> dict:
    symbols = [SYMBOL_MAP[cid] for cid in BM20_IDS]
    r = requests.get(
        "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest",
        headers={"X-CMC_PRO_API_KEY": api_key},
        params={"symbol": ",".join(symbols), "convert": "USD"},
        timeout=15,
    )
    r.raise_for_status()
    data = r.json().get("data", {})
    print(f"[INFO] CMC 응답 코인 수: {len(data)}개")

    sym_to_cid = {v: k for k, v in SYMBOL_MAP.items()}
    prices = {}
    for sym, entries in data.items():
        entry = entries[0] if isinstance(entries, list) else entries
        quote = entry.get("quote", {}).get("USD", {})
        price = quote.get("price")
        chg24 = quote.get("percent_change_24h")
        if price is None or chg24 is None:
            raise ValueError(f"Incomplete CMC quote: {sym}")
        price, chg24 = float(price), float(chg24)
        if not math.isfinite(chg24) or chg24 <= -100:
            raise ValueError(f"Invalid CMC 24h return: {sym}")
        prev_price = price / (1.0 + chg24 / 100.0)
        cid = sym_to_cid.get(sym.upper())
        if cid:
            prices[cid] = {"current": price, "prev": prev_price}

    return prices

def build_realtime_snapshot(series, prices, now_kst):
    today = now_kst.astimezone(KST).date().isoformat()
    base_level, official_today = select_realtime_reference(series, today)
    weights = compute_weights(BM20_IDS)
    port_ret = 0.0
    for cid, weight in weights.items():
        quote = prices[cid]  # Missing quotes must fail, not imply zero return.
        p0, p1 = float(quote["prev"]), float(quote["current"])
        if not all(math.isfinite(p) and p > 0 for p in (p0, p1)):
            raise ValueError(f"Invalid quote: {cid}")
        port_ret += weight * (p1 / p0 - 1.0)
    level = round(base_level * (1.0 + port_ret), 6)
    ret = round(level / base_level - 1.0, 8)
    snapshot = {
        "asOf": today,
        "bm20Mode": "realtime_24h_proxy",
        "bm20ReferenceDate": (now_kst.astimezone(KST).date() - timedelta(days=1)).isoformat(),
        "bm20Level": level,
        "bm20PrevLevel": round(base_level, 6),
        "bm20PointChange": round(level - base_level, 6),
        "bm20ChangePct": ret,
        "returns": {"1D": ret},
        "updatedAt": now_kst.astimezone(KST).isoformat(timespec="seconds"),
    }
    if official_today is not None:
        snapshot["bm20OfficialDate"] = today
        snapshot["bm20OfficialLevel"] = official_today
    return snapshot

# ── 메인 ───────────────────────────────────────────────────────────
def main():
    now_kst = datetime.now(KST)
    print(f"[START] update_bm20_latest.py — {now_kst.strftime('%Y-%m-%d %H:%M:%S')} KST")

    api_key = os.getenv("CMC_API_KEY", "")
    if not api_key:
        print("[ERROR] CMC_API_KEY 없음. 종료.")
        raise SystemExit(1)

    try:
        series = json.loads((ROOT / "bm20_series.json").read_text(encoding="utf-8"))
        select_realtime_reference(series, now_kst.date().isoformat())
        prices = fetch_cmc_prices(api_key)
        snapshot = build_realtime_snapshot(series, prices, now_kst)
    except Exception as e:
        raise SystemExit(f"[ERROR] BM20 realtime snapshot failed: {e}") from e

    realtime_path = ROOT / "bm20_realtime_latest.json"
    temp_path = realtime_path.with_suffix(".json.tmp")
    temp_path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temp_path.replace(realtime_path)
    print(f"[OK] realtime proxy — asOf={snapshot['asOf']}, base={snapshot['bm20ReferenceDate']}, level={snapshot['bm20Level']}")

if __name__ == "__main__":
    main()
