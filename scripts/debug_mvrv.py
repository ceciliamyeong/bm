"""
Santiment MVRV 디버그 스크립트
- BTC 하나만 테스트
- raw 응답 전체 출력
"""

import os
import json
import requests
from datetime import datetime, timedelta

API_KEY = os.environ.get("SANTIMENT_API_KEY")
API_URL = "https://api.santiment.net/graphql"

print(f"API_KEY 존재 여부: {'✅ 있음' if API_KEY else '❌ 없음 (환경변수 미설정)'}")
if API_KEY:
    print(f"API_KEY 앞 8자리: {API_KEY[:8]}...")

to_date   = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
from_date = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")

query = """
{
  getMetric(metric: "mvrv_ratio") {
    timeseriesData(
      slug: "bitcoin"
      from: "%s"
      to: "%s"
      interval: "1d"
    ) {
      datetime
      value
    }
  }
}
""" % (from_date, to_date)

headers = {
    "Content-Type": "application/json",
    "Authorization": f"Apikey {API_KEY}"
}

print(f"\n요청 URL: {API_URL}")
print(f"요청 기간: {from_date} ~ {to_date}")
print(f"헤더: {headers}")
print(f"\n쿼리:\n{query}")

try:
    resp = requests.post(API_URL, json={"query": query}, headers=headers, timeout=15)
    print(f"\nHTTP 상태코드: {resp.status_code}")
    print(f"\nRaw 응답:\n{json.dumps(resp.json(), indent=2)}")
except Exception as e:
    print(f"\n[EXCEPTION] {e}")
