# scripts/update_coin_prices_usd.py
# - out/history/coin_prices_usd.csv를 "증분 업데이트"
# - CoinGecko range로 필요한 날짜만 갱신 (EOD용)
from __future__ import annotations

import os, time, json
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
    "BTC",
    "ETH",
    "BNB",
    "XRP",
    "USDT",
    "SOL",
    "TON",
    "ADA",
    "DOGE",
    "DOT",
    "LINK",
    "AVAX",
    "NEAR",
    "ICP",
    "ATOM",
    "LTC",
    "OP",
    "ARB",
    "MATIC",
    "SUI",
]

# CoinGecko ID mapping (validated from your table)
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

def to_unix(dt: datetime) -> int:
    return int(dt.replace(tzinfo=timezone.utc).timestamp())

def cg_range_daily_close(coin_id: str, start_utc: datetime, end_utc: datetime) -> pd.Series:
    """
    CoinGecko Pro range endpoint
    - Pro API는 반드시 pro-api.coingecko.com 사용
    - 긴 기간 요청 시 400 방지를 위해 기간 분할 호출
    - (UTC) 일 단위 resample 후 'last' = 종가
    """

    # ✅ Pro API BASE URL (중요)
    BASE_URL = "https://pro-api.coingecko.com/api/v3"
    url = f"{BASE_URL}/coins/{coin_id}/market_chart/range"

    MAX_DAYS_PER_CALL = 90  # 필요시 30으로 축소 가능

    headers = {
        "x-cg-pro-api-key": COINGECKO_API_KEY,   # 🔑 Pro Key Header
        "accept": "application/json",
    }

    all_points = []

    cur = start_utc
    while cur < end_utc:
        chunk_end = min(cur + timedelta(days=MAX_DAYS_PER_CALL), end_utc)

        params = {
            "vs_currency": "usd",
            "from": int(cur.replace(tzinfo=timezone.utc).timestamp()),
            "to": int(chunk_end.replace(tzinfo=timezone.utc).timestamp()),
        }

        r = requests.get(url, params=params, headers=headers, timeout=30)

        try:
            r.raise_for_status()
        except requests.HTTPError as e:
            raise RuntimeError(
                f"[CoinGecko PRO ERROR] {coin_id} "
                f"{cur.date()} ~ {chunk_end.date()} | "
                f"status={r.status_code} | body={r.text[:200]}"
            ) from e

        data = r.json().get("prices", [])
        if data:
            all_points.extend(data)

        cur = chunk_end
        time.sleep(0.8)  # Pro는 호출 여유 있음

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
    first_start: str = "2018-01-01",
    lookback_days: int = 3,
    sleep_sec: float = 1.4,
):
    os.makedirs(OUT_DIR, exist_ok=True)

    now_utc = datetime.now(timezone.utc)

    # EOD 안정성: 최근 며칠(lookback_days)은 다시 덮어쓰기
    if os.path.exists(OUT_CSV):
        old = pd.read_csv(OUT_CSV)
        old["date"] = pd.to_datetime(old["date"]).dt.date
        old = old.sort_values("date")
        last_date = old["date"].max()
        start_date = (pd.to_datetime(last_date) - pd.Timedelta(days=lookback_days)).date()
        df_old = old.set_index("date")
    else:
        start_date = pd.to_datetime(first_start).date()
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
        print(f"[OK] {t} {len(s)} rows ({start_date}~)")
        time.sleep(sleep_sec)

    df_new = pd.DataFrame(series_map)
    df_new.index.name = "date"

    # 기존 데이터와 결합: new가 우선(덮어쓰기)
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

    out2 = out.reset_index()
    out2["date"] = out2["date"].astype(str)
    out2.to_csv(OUT_CSV, index=False)

    meta = {
        "tickers": TICKERS,
        "cg_ids": CG_ID,
        "start": str(out.index.min()),
        "end": str(out.index.max()),
        "generated_utc": now_utc.isoformat(),
        "source": "CoinGecko market_chart/range (USD, daily close via resample last)",
        "lookback_days": lookback_days,
        "sleep_sec": sleep_sec,
    }
    with open(OUT_META, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print(f"\nSaved: {OUT_CSV}")
    print(f"Saved: {OUT_META}")

if __name__ == "__main__":
    main()
