#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
update_bm20_latest.py
뉴스레터 렌더 직전 실행 — CMC API로 20개 코인 현재가를 가져와
bm20_daily.py와 동일한 방식으로 BM20 레벨과 1D를 실시간 갱신합니다.
의존: requests (pip install requests)
"""

import json
import os
import requests
from datetime import datetime, timedelta, timezone
from pathlib import Path

KST = timezone(timedelta(hours=9))
ROOT = Path(__file__).resolve().parent.parent  # scripts/ 기준 상위

# ── BM20 유니버스 & 가중치 (bm20_daily.py 동일) ────────────────────
SYMBOL_MAP = {
    "bitcoin": "BTC", "ethereum": "ETH", "ripple": "XRP", "tether": "USDT",
    "binancecoin": "BNB", "solana": "SOL", "usd-coin": "USDC", "dogecoin": "DOGE",
    "tron": "TRX", "cardano": "ADA", "hyperliquid": "HYPE", "chainlink": "LINK",
    "sui": "SUI", "avalanche-2": "AVAX", "stellar": "XLM", "bitcoin-cash": "BCH",
    "hedera-hashgraph": "HBAR", "litecoin": "LTC", "shiba-inu": "SHIB", "toncoin": "TON",
}
FIXED_WEIGHTS = {
    "bitcoin": 0.30, "ethereum": 0.20, "ripple": 0.05,
    "tether": 0.05, "binancecoin": 0.05,
}
BM20_IDS = list(SYMBOL_MAP.keys())

def compute_weights(ids: list) -> dict:
    fixed_sum = sum(FIXED_WEIGHTS.values())  # 0.65
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
        if price is None:
            continue
        price = float(price)
        chg24 = float(chg24) if chg24 is not None else 0.0
        prev_price = price / (1.0 + chg24 / 100.0) if chg24 != -100 else price
        cid = sym_to_cid.get(sym.upper())
        if cid:
            prices[cid] = {"current": price, "prev": prev_price}

    return prices

# ── bm20_series.json 마지막 레벨 읽기 ──────────────────────────────
def load_last_level() -> float | None:
    for p in [ROOT / "bm20_series.json", ROOT / "data" / "bm20_series.json"]:
        try:
            if not p.exists():
                continue
            series = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(series, list) and series:
                return float(series[-1]["level"])
        except Exception as e:
            print(f"[WARN] {p} 읽기 실패: {e}")
    return None

# ── 메인 ───────────────────────────────────────────────────────────
def main():
    now_kst = datetime.now(KST)
    print(f"[START] update_bm20_latest.py — {now_kst.strftime('%Y-%m-%d %H:%M:%S')} KST")

    api_key = os.getenv("CMC_API_KEY", "")
    if not api_key:
        print("[ERROR] CMC_API_KEY 없음. 종료.")
        return

    # bm20_latest.json 읽기
    for latest_path in [ROOT / "data" / "bm20_latest.json", ROOT / "bm20_latest.json"]:
        if latest_path.exists():
            break
    try:
        existing = json.loads(latest_path.read_text(encoding="utf-8"))
    except Exception:
        print("[ERROR] bm20_latest.json 읽기 실패. 종료.")
        return

    # 시리즈 마지막 레벨
    last_level = load_last_level() or existing.get("bm20Level")
    if not last_level:
        print("[ERROR] 기준 레벨을 가져올 수 없습니다. 종료.")
        return
    print(f"[INFO] 기준 레벨: {last_level}")

    # CMC 현재가 조회
    try:
        prices = fetch_cmc_prices(api_key)
    except Exception as e:
        print(f"[ERROR] CMC 가격 조회 실패: {e}. 종료.")
        return

    # bm20_daily.py 동일 방식으로 1D 수익률 계산
    weights = compute_weights(BM20_IDS)
    port_ret_1d = 0.0
    for cid, w in weights.items():
        if cid not in prices:
            continue
        p0 = prices[cid]["prev"]
        p1 = prices[cid]["current"]
        if p0 > 0 and p1 > 0:
            port_ret_1d += w * ((p1 / p0) - 1.0)

    # 레벨 & 1D 갱신
    bm20_now  = round(last_level * (1.0 + port_ret_1d), 6)
    ret_1d    = round((bm20_now / last_level) - 1.0, 8)
    point_chg = round(bm20_now - last_level, 6)

    existing["bm20Level"]       = bm20_now
    existing["bm20PrevLevel"]   = round(last_level, 6)
    existing["bm20PointChange"] = point_chg
    existing["bm20ChangePct"]   = ret_1d
    existing["returns"]["1D"]   = ret_1d
    existing["updatedAt"]       = now_kst.strftime("%Y-%m-%dT%H:%M:%S+09:00")

    latest_path.write_text(
        json.dumps(existing, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"[OK] bm20_latest.json 갱신 — level={bm20_now}, 1D={ret_1d*100:+.4f}%")

    missing = [cid for cid in BM20_IDS if cid not in prices]
    if missing:
        print(f"[WARN] 가격 없는 코인: {[SYMBOL_MAP[c] for c in missing]}")

if __name__ == "__main__":
    main()
