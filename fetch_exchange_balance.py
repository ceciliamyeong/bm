"""
Santiment exchange_balance fetcher
BM20 코인들의 거래소 순유입(매도압력) 데이터를 가져옴

사용법:
  export SANTIMENT_API_KEY="your_api_key"
  python fetch_exchange_balance.py
"""

import os
import json
import httpx
from datetime import datetime, timedelta, timezone

SANTIMENT_API_KEY = os.environ["SANTIMENT_API_KEY"]
GRAPHQL_URL = "https://api.santiment.net/graphql"

# BM20 코인 slug 매핑 (Santiment slug 기준)
BM20_SLUGS = {
    "BTC":  "bitcoin",
    "ETH":  "ethereum",
    "BNB":  "binance-coin",
    "XRP":  "ripple",
    "SOL":  "solana",
    "ADA":  "cardano",
    "DOGE": "dogecoin",
    "AVAX": "avalanche",
    "DOT":  "polkadot",
    "LINK": "chainlink",
    "LTC":  "litecoin",
    "BCH":  "bitcoin-cash",
    "UNI":  "uniswap",
    "XLM":  "stellar",
    "ATOM": "cosmos",
    "SUI":  "sui",
    "TRX":  "tron",
    "TON":  "the-open-network",
    "SHIB": "shiba-inu",
    "NEAR": "near-protocol",
}

def fetch_exchange_balance(slug: str, days: int = 30, interval: str = "1d") -> list:
    """단일 코인 exchange_balance 조회"""
    now = datetime.now(timezone.utc)
    from_dt = (now - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")
    to_dt = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    query = """
    {
      getMetric(metric: "exchange_balance") {
        timeseriesDataJson(
          slug: "%s"
          from: "%s"
          to: "%s"
          interval: "%s"
        )
      }
    }
    """ % (slug, from_dt, to_dt, interval)

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Apikey {SANTIMENT_API_KEY}",
    }

    with httpx.Client(timeout=30) as client:
        res = client.post(GRAPHQL_URL, json={"query": query}, headers=headers)
        res.raise_for_status()
        data = res.json()

    raw = data.get("data", {}).get("getMetric", {}).get("timeseriesDataJson")
    if not raw:
        return []
    return json.loads(raw)  # [{datetime, value}, ...]


def fetch_all_bm20(days: int = 7) -> dict:
    """BM20 전체 코인 exchange_balance 조회"""
    results = {}
    for symbol, slug in BM20_SLUGS.items():
        try:
            rows = fetch_exchange_balance(slug, days=days)
            if rows:
                latest = rows[-1]
                prev = rows[-2] if len(rows) >= 2 else None
                change_1d = None
                if prev and prev["value"] and latest["value"]:
                    change_1d = latest["value"] - prev["value"]

                results[symbol] = {
                    "slug": slug,
                    "latest_date": latest["datetime"],
                    "exchange_balance": latest["value"],
                    "change_1d": change_1d,  # 양수=유입증가(매도압력↑), 음수=유출(매도압력↓)
                    "history": rows,
                }
                signal = "🔴 매도압력↑" if (change_1d or 0) > 0 else "🟢 유출(매도압력↓)"
                print(f"{symbol:6s} | balance: {latest['value']:>15,.0f} | 1d변화: {change_1d:>+15,.0f} | {signal}")
        except Exception as e:
            print(f"{symbol:6s} | ERROR: {e}")
            results[symbol] = {"error": str(e)}

    return results


if __name__ == "__main__":
    print("=" * 70)
    print(f"Santiment Exchange Balance (BM20) — {datetime.now().strftime('%Y-%m-%d %H:%M KST')}")
    print("양수=거래소 순유입(매도압력↑) / 음수=거래소 순유출(매도압력↓)")
    print("=" * 70)

    results = fetch_all_bm20(days=7)

    # JSON으로 저장
    output_path = "exchange_balance_bm20.json"
    with open(output_path, "w", encoding="utf-8") as f:
        # history 제외하고 저장 (용량)
        summary = {k: {ek: ev for ek, ev in v.items() if ek != "history"}
                   for k, v in results.items()}
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(f"\n저장 완료: {output_path}")
