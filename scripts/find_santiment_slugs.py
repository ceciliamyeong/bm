"""
Santiment 정확한 slug 조회 스크립트
- allProjects에서 ticker 기준으로 BM20 코인 slug 찾기
"""

import os
import json
import requests

API_KEY = os.environ.get("SANTIMENT_API_KEY")
API_URL = "https://api.santiment.net/graphql"

TARGET_TICKERS = {"BTC","ETH","XRP","SOL","BNB","ADA","AVAX","DOT","LINK","TRX","XLM","HBAR","UNI","LTC","ATOM","NEAR","HYPE","SUI","ZEC","CC"}

query = """
{
  allProjects {
    name
    slug
    ticker
  }
}
"""

headers = {
    "Content-Type": "application/json",
    "Authorization": f"Apikey {API_KEY}"
}

print("Santiment 전체 프로젝트 조회 중...")
resp = requests.post(API_URL, json={"query": query}, headers=headers, timeout=30)
data = resp.json()

projects = data.get("data", {}).get("allProjects", [])
print(f"총 {len(projects)}개 프로젝트 조회됨\n")

found = {}
for p in projects:
    ticker = (p.get("ticker") or "").upper()
    if ticker in TARGET_TICKERS:
        if ticker not in found:
            found[ticker] = []
        found[ticker].append({"name": p["name"], "slug": p["slug"]})

print(f"{'심볼':<8} {'slug':<35} {'name'}")
print("-" * 70)
for ticker in sorted(TARGET_TICKERS):
    matches = found.get(ticker, [])
    if matches:
        for m in matches:
            print(f"{ticker:<8} {m['slug']:<35} {m['name']}")
    else:
        print(f"{ticker:<8} ❌ 없음")

# JSON 저장
with open("santiment_slugs.json", "w") as f:
    json.dump(found, f, indent=2, ensure_ascii=False)
print("\n→ santiment_slugs.json 저장 완료")
