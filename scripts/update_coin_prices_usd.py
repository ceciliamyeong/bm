# scripts/update_coin_prices_usd.py
# - out/history/coin_prices_usd.csv를 "증분 업데이트"
# - CoinGecko PRO API: 최근 2년(730일)만 유지 + 레이트리밋 대응
from __future__ import annotations

import os, time, json, random
from datetime import datetime, timedelta, timezone
from typing import Dict, List

import pandas as pd
import requests

# -----------------------------
# ENV (GitHub Secrets)
# -----------------------------
COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY")
if not COINGECKO_API_KEY:
    raise RuntimeError("Missing COINGECKO_API_KEY (check GitHub Secrets + workflow env)")

# -----------------------------
# OUTPUT PATHS
# -----------------------------
OUT_DIR = "out/history"
OUT_CSV = os.path.join(OUT_DIR, "coin_prices_usd.csv")
OUT_META = os.path.join(OUT_DIR, "coin_prices_usd.meta.json")

# -----------------------------
# BM20 UNIVERSE (20)
# -----------------------------
TICKERS: List[str] = [
    "BTC","ETH","BNB","XRP","USDT",
    "SOL","TON","ADA","DOGE","DOT",
    "LINK","AVAX","NEAR","ICP","ATOM",
    "LTC","OP","ARB","MATIC","SUI",
]

# CoinGecko ID mapping (your verified table)
CG_ID: Dict[str, str] = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "BNB": "binancecoin",
    "XRP": "ripple",
    "USDT": "tether",
    "SOL": "solana",
    "TON": "toncoin",
    "ADA": "cardano",
    "DOGE": "dogecoin",
    "DOT": "polkadot",
    "LINK": "chainlink",
    "AVAX": "avalanche-2",
    "NEAR": "near",
    "ICP": "internet-computer",
    "ATOM": "cosmos-hub",
    "LTC": "litecoin",
    "OP": "optimism",
    "ARB": "arbitrum",
    "MATIC": "polygon",
    "SUI": "sui",
}

# -----------------------------
# SETTINGS
# -----------------------------
MAX_LOOKBACK_DAYS = 730          # ✅ 최근 2년만 유지 (플랜 제한 대응)
CALLS_PER_MIN = 200              # ✅ 레이트리밋(분당 250)보다 여유 있게
MIN_INTERVAL = 60.0 / CALLS_PER_MIN
_last_call_ts = 0.0

# CoinGecko Pro API
BASE_URL = "https://pro-api.coingecko.com/api/v3"
PRO_HEADERS = {
    "x-cg-pro-api-key": COINGECKO_API_KEY,  # ✅ Pro는 헤더만
    "accept": "application/json",
}

def rate_limit_sleep():
    global _last_call_ts
    now = time.time()
    wait = (_last_call_ts + MIN_INTERVAL) - now
    if wait > 0:
        time.sleep(wait)
    _last_call_ts = time.time()

def get_with_retry(url: str, params: dict, headers: dict, timeout: int = 30, max_retries: int = 6):
    """
    429(Too Many Requests) / 5xx 대응용 재시도
    """
    for i in range(max_retries):
        rate_limit_sleep()
        r = requests.get(url, params=params, headers=headers, timeout=timeout)

        if r.status_code == 429 or (500 <= r.status_code < 600):
            # exponential backoff + jitter
            sleep_s = min(30, (2 ** i) + random.random())
            time.sleep(sleep_s)
            continue

        return r

    # 마지막 응답 반환(raise_for_status에서 에러 표시)
    return r

def to_unix(dt: datetime) -> int:
    return int(dt.astimezone(timezone.utc).timestamp())

def cg_range_daily_close(coin_id: str, start_utc: datetime, end_utc: datetime) -> pd.Series:
    """
    CoinGecko PRO range endpoint
    - 최근 2년 구간만 사용 (start_utc는 main에서 clamp)
    - 긴 기간은 chunk로 쪼개 호출
    - (UTC) 일 단위 resample 후 'last' = 종가
    """
    url = f"{BASE_URL}/coins/{coin_id}/market_chart/range"

    # 2년이면 90일 청크로도 충분히 안정적
    MAX_DAYS_PER_CALL = 90

    all_points = []
    cur = start_utc

    while cur < end_utc:
        chunk_end = min(cur + timedelta(days=MAX_DAYS_PER_CALL), end_utc)

        params = {
            "vs_currency": "usd",
            "from": to_unix(cur),
            "to": to_unix(chunk_end),
        }

        r = get_with_retry(url, params=params, headers=PRO_HEADERS, timeout=30, max_retries=6)

        try:
            r.raise_for_status()
        except requests.HTTPError as e:
            raise RuntimeError(
                f"[CoinGecko PRO ERROR] {coin_id} {cur.date()} ~ {chunk_end.date()} | "
                f"status={r.status_code} | body={r.text[:220]}"
            ) from e

        data = r.json().get("prices", [])
        if data:
            all_points.extend(data)

        cur = chunk_end

    if not all_points:
        raise ValueError(f"No price data for {coin_id}")

    s = pd.Series(
        [p[1] for p in all_points],
        index=pd.to_datetime([p[0] for p in all_points], unit="ms", utc=True),
        name=coin_id,
    )

    daily = s.resample("1D").last()
    daily.index = daily.index.date
    return daily

def main(
    lookback_days: int = 3,
    sleep_sec_between_coins: float = 0.0,  # 코인 단위로 추가 쉬고 싶으면 0.2~0.5
):
    os.makedirs(OUT_DIR, exist_ok=True)

    now_utc = datetime.now(timezone.utc)
    min_date = (now_utc - timedelta(days=MAX_LOOKBACK_DAYS)).date()

    # ✅ start_date 결정: 기존 파일 있으면 last_date - lookback, 없으면 min_date
    if os.path.exists(OUT_CSV):
        old = pd.read_csv(OUT_CSV)
        old["date"] = pd.to_datetime(old["date"]).dt.date
        old = old.sort_values("date")
        last_date = old["date"].max()

        start_date = (pd.to_datetime(last_date) - pd.Timedelta(days=lookback_days)).date()
        if start_date < min_date:
            start_date = min_date

        df_old = old.set_index("date")
    else:
        start_date = min_date
        df_old = None

    start_utc = datetime.combine(start_date, datetime.min.time(), tzinfo=timezone.utc)
    end_utc = now_utc

    series_map: Dict[str, pd.Series] = {}

    for t in TICKERS:
        coin_id = CG_ID.get(t)
        if not coin_id:
            raise KeyError(f"Missing CG_ID for {t}")

        s = cg_range_daily_close(coin_id, start_utc, end_utc)
        series_map[t] = s

        print(f"[OK] {t} rows={len(s)} ({start_date}~{end_utc.date()})")

        if sleep_sec_between_coins > 0:
            time.sleep(sleep_sec_between_coins)

    df_new = pd.DataFrame(series_map)
    df_new.index.name = "date"

    # ✅ 기존 데이터와 결합: new가 우선(덮어쓰기) + 최근 2년 유지
    if df_old is not None:
        df_old2 = df_old.copy()
        df_old2.index = pd.to_datetime(df_old2.index).date

        df_new2 = df_new.copy()
        df_new2.index = pd.to_datetime(df_new2.index).date

        combined = pd.concat([df_old2, df_new2], axis=0)
        combined = combined[~combined.index.duplicated(keep="last")]
        out = combined.sort_index()
    else:
        out = df_new.sort_index()

    # 최근 2년만 남기기 (최종 clamp)
    out = out[out.index >= min_date]

    out2 = out.reset_index()
    out2["date"] = out2["date"].astype(str)
    out2.to_csv(OUT_CSV, index=False)

    meta = {
        "tickers": TICKERS,
        "cg_ids": CG_ID,
        "start": str(out.index.min()),
        "end": str(out.index.max()),
        "generated_utc": now_utc.isoformat(),
        "source": "CoinGecko PRO market_chart/range (USD, daily close via resample last)",
        "base_url": BASE_URL,
        "auth_header": "x-cg-pro-api-key",
        "max_lookback_days": MAX_LOOKBACK_DAYS,
        "lookback_days": lookback_days,
        "calls_per_min": CALLS_PER_MIN,
    }
    with open(OUT_META, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print(f"\nSaved: {OUT_CSV}")
    print(f"Saved: {OUT_META}")

if __name__ == "__main__":
    main()
