"""
Santiment MVRV 커버리지 체크 스크립트
- BM20 구성 코인 대상으로 mvrv_ratio 데이터 반환 여부 확인
- 최근 7일치만 요청해서 빠르게 체크
- 결과: 커버 가능 코인 / N/A 코인 분류 출력
"""

import os
import json
import requests
from datetime import datetime, timedelta

API_KEY = os.environ.get("SANTIMENT_API_KEY")
API_URL = "https://api.santiment.net/graphql"

# BM20 구성 코인 (Santiment slug 기준)
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

def fetch_mvrv(slug: str, from_date: str, to_date: str) -> list:
    query = """
    {
      getMetric(metric: "mvrv_ratio") {
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
        resp = requests.post(
            API_URL,
            json={"query": query},
            headers=headers,
            timeout=15
        )
        resp.raise_for_status()
        data = resp.json()

        # 에러 체크
        if "errors" in data:
            return []

        rows = data.get("data", {}).get("getMetric", {}).get("timeseriesData", [])
        # null값 필터
        rows = [r for r in rows if r.get("value") is not None]
        return rows

    except Exception as e:
        print(f"  [ERROR] {slug}: {e}")
        return []


def main():
    to_date = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    from_date = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")

    print(f"\n{'='*55}")
    print(f"  Santiment MVRV 커버리지 체크")
    print(f"  기간: 최근 7일 ({from_date[:10]} ~ {to_date[:10]})")
    print(f"{'='*55}\n")

    covered = []
    not_covered = []

    for coin in BM20_COINS:
        symbol = coin["symbol"]
        slug   = coin["slug"]
        print(f"  Checking {symbol:6s} ({slug}) ... ", end="", flush=True)

        rows = fetch_mvrv(slug, from_date, to_date)

        if rows:
            latest = rows[-1]
            print(f"✅  {len(rows)}일치 | 최신 MVRV: {latest['value']:.4f} ({latest['datetime'][:10]})")
            covered.append({
                "symbol": symbol,
                "slug": slug,
                "days": len(rows),
                "latest_mvrv": round(latest["value"], 4),
                "latest_date": latest["datetime"][:10]
            })
        else:
            print("❌  데이터 없음")
            not_covered.append({"symbol": symbol, "slug": slug})

    # 결과 요약
    print(f"\n{'='*55}")
    print(f"  결과 요약")
    print(f"{'='*55}")
    print(f"\n✅ 커버 가능 ({len(covered)}종):")
    for c in covered:
        print(f"   {c['symbol']:6s} | MVRV {c['latest_mvrv']:+.4f} | {c['latest_date']}")

    print(f"\n❌ 데이터 없음 ({len(not_covered)}종):")
    for c in not_covered:
        print(f"   {c['symbol']:6s} ({c['slug']})")

    # JSON 저장
    result = {
        "checked_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "covered": covered,
        "not_covered": not_covered
    }
    with open("mvrv_coverage.json", "w") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    print(f"\n  → mvrv_coverage.json 저장 완료")
    print(f"{'='*55}\n")


if __name__ == "__main__":
    main()
