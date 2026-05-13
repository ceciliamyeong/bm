"""
Santiment MVRV 커버리지 체크 v2
- slug 교정 버전
"""

import os
import json
import requests
from datetime import datetime, timedelta

API_KEY = os.environ.get("SANTIMENT_API_KEY")
API_URL = "https://api.santiment.net/graphql"

# slug 수정본 - Santiment 공식 slug 기준
BM20_COINS = [
    {"symbol": "BTC",  "slug": "bitcoin"},
    {"symbol": "ETH",  "slug": "ethereum"},
    {"symbol": "XRP",  "slug": "ripple"},
    {"symbol": "SOL",  "slug": "solana"},
    {"symbol": "BNB",  "slug": "binance-coin"},
    {"symbol": "ADA",  "slug": "cardano"},
    {"symbol": "AVAX", "slug": "avalanche"},
    {"symbol": "DOT",  "slug": "polkadot-new"},
    {"symbol": "LINK", "slug": "chainlink"},
    {"symbol": "TRX",  "slug": "tron"},
    {"symbol": "XLM",  "slug": "stellar"},
    {"symbol": "HBAR", "slug": "hedera-hashgraph"},
    {"symbol": "UNI",  "slug": "uniswap"},
    {"symbol": "LTC",  "slug": "litecoin"},
    {"symbol": "ATOM", "slug": "cosmos"},
    {"symbol": "NEAR", "slug": "near-protocol"},
    {"symbol": "HYPE", "slug": "hyperliquid"},
    {"symbol": "SUI",  "slug": "sui"},
    {"symbol": "ZEC",  "slug": "zcash"},
    {"symbol": "CC",   "slug": "canton-network"},
]

# 안 됐던 코인들 대체 slug 후보
SLUG_ALTERNATIVES = {
    "SOL":  ["solana"],
    "BNB":  ["binance-coin", "bnb"],
    "AVAX": ["avalanche", "avalanche-2"],
    "DOT":  ["polkadot-new", "polkadot"],
    "TRX":  ["tron"],
    "XLM":  ["stellar"],
    "HBAR": ["hedera-hashgraph", "hedera"],
    "ATOM": ["cosmos", "cosmos-hub"],
    "NEAR": ["near-protocol", "near"],
    "HYPE": ["hyperliquid", "hype"],
    "SUI":  ["sui"],
    "ZEC":  ["zcash"],
    "CC":   ["canton-network", "canton"],
}

def fetch_mvrv(slug: str, from_date: str, to_date: str) -> list:
    query = """
    {
      getMetric(metric: "mvrv_usd") {
        timeseriesData(
          slug: "%s"
          from: "%s"
          to: "%s"
          interval: "1d"
        ) {
          datetime
          value
        }
      }
    }
    """ % (slug, from_date, to_date)

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Apikey {API_KEY}"
    }

    try:
        resp = requests.post(API_URL, json={"query": query}, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if "errors" in data:
            return []
        rows = data.get("data", {}).get("getMetric", {}).get("timeseriesData", [])
        return [r for r in rows if r.get("value") is not None]
    except Exception:
        return []


def main():
    to_date   = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    from_date = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")

    print(f"\n{'='*60}")
    print(f"  Santiment MVRV 커버리지 체크 v2 (slug 교정)")
    print(f"  기간: {from_date[:10]} ~ {to_date[:10]}")
    print(f"{'='*60}\n")

    covered     = []
    not_covered = []

    for coin in BM20_COINS:
        symbol = coin["symbol"]
        slugs_to_try = SLUG_ALTERNATIVES.get(symbol, [coin["slug"]])

        success = False
        for slug in slugs_to_try:
            print(f"  {symbol:6s} ({slug}) ... ", end="", flush=True)
            rows = fetch_mvrv(slug, from_date, to_date)
            if rows:
                latest = rows[-1]
                print(f"✅  MVRV: {latest['value']:+.4f} | {latest['datetime'][:10]}")
                covered.append({
                    "symbol": symbol,
                    "slug": slug,
                    "latest_mvrv": round(latest["value"], 4),
                    "latest_date": latest["datetime"][:10]
                })
                success = True
                break
            else:
                print(f"❌")

        if not success:
            not_covered.append({"symbol": symbol})

    print(f"\n{'='*60}")
    print(f"✅ 커버 가능 ({len(covered)}종):")
    for c in covered:
        print(f"   {c['symbol']:6s} | {c['slug']:30s} | MVRV {c['latest_mvrv']:+.4f}")

    print(f"\n❌ 최종 미지원 ({len(not_covered)}종):")
    for c in not_covered:
        print(f"   {c['symbol']}")

    result = {
        "checked_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "covered": covered,
        "not_covered": not_covered
    }
    with open("mvrv_coverage_v2.json", "w") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    print(f"\n  → mvrv_coverage_v2.json 저장 완료\n")


if __name__ == "__main__":
    main()
