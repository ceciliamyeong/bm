# scripts/update_coin_prices_usd.py
# - out/history/coin_prices_usd.csv를 "증분 업데이트"
# - CoinGecko range로 필요한 날짜만 갱신 (EOD용)
from __future__ import annotations

import os, time, json
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import pandas as pd
import requests

OUT_DIR = "out/history"
OUT_CSV = os.path.join(OUT_DIR, "coin_prices_usd.csv")
OUT_META = os.path.join(OUT_DIR, "coin_prices_usd.meta.json")

# 너희 BM20 구성에 맞게 확정 리스트로 교체해줘 (예시는 임시)
TICKERS = [
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


# CoinGecko ID 매핑 (필수)
CG_ID = {
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
    CoinGecko range endpoint → (UTC) 일 단위로 resample 후 'last' = 종가.
    index는 date(YYYY-MM-DD)
    """
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart/range"
    params = {"vs_currency": "usd", "from": to_unix(start_utc), "to": to_unix(end_utc)}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json().get("prices", [])
    if not data:
        raise ValueError(f"No prices for {coin_id}")

    s = pd.Series(
        [p[1] for p in data],
        index=pd.to_datetime([p[0] for p in data], unit="ms", utc=True),
        name=coin_id,
    )
    daily = s.resample("1D").last()
    daily.index = daily.index.date
    return daily

def main(
    first_start: str = "2018-01-01",
    lookback_days: int = 3,
    sleep_sec: float = 1.2,
):
    os.makedirs(OUT_DIR, exist_ok=True)

    now_utc = datetime.now(timezone.utc)
    # EOD 안정성: 최근 2~3일은 다시 덮어써서(lookback) 누락/수정 대비
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

    # 기존이 있으면 lookback 구간부터 교체(덮어쓰기)
    if df_old is not None:
        # 기존 index(date)로 맞춘 뒤, new와 결합하여 new 우선
        df_old2 = df_old.copy()
        df_old2.index = pd.to_datetime(df_old2.index).date
        df_new2 = df_new.copy()
        df_new2.index = pd.to_datetime(df_new2.index).date

        combined = pd.concat([df_old2, df_new2], axis=0)
        # 동일 날짜 중복은 마지막(new)이 이기게
        combined = combined[~combined.index.duplicated(keep="last")]
        combined = combined.sort_index()
        out = combined
    else:
        out = df_new.sort_index()

    out2 = out.reset_index()
    out2["date"] = out2["date"].astype(str)
    out2.to_csv(OUT_CSV, index=False)

    meta = {
        "tickers": TICKERS,
        "start": str(out.index.min()),
        "end": str(out.index.max()),
        "generated_utc": now_utc.isoformat(),
        "source": "CoinGecko market_chart/range (USD, daily close via resample last)",
        "lookback_days": lookback_days,
    }
    with open(OUT_META, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print(f"\nSaved: {OUT_CSV}")
    print(f"Saved: {OUT_META}")

if __name__ == "__main__":
    main()
