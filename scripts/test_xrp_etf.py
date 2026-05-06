"""
XRP ETF API 테스트 스크립트
============================
실행: SOSOVALUE_API_KEY=your_key python test_xrp_etf.py
"""

import requests
import json
import os
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

API_KEY = os.environ.get("SOSOVALUE_API_KEY", "")
BASE_URL = "https://api.sosovalue.xyz"
HEADERS = {
    "x-soso-api-key": API_KEY,
    "Content-Type": "application/json"
}

ETF_TYPE = "us-xrp-spot"

print("=" * 50)
print(f"XRP ETF API 테스트: {ETF_TYPE}")
print("=" * 50)

# 1. currentEtfDataMetrics 테스트
print("\n[1] currentEtfDataMetrics")
try:
    r = requests.post(
        f"{BASE_URL}/openapi/v2/etf/currentEtfDataMetrics",
        headers=HEADERS,
        json={"type": ETF_TYPE},
        timeout=15,
        verify=False
    )
    print(f"  HTTP status: {r.status_code}")
    data = r.json()
    print(f"  code: {data.get('code')}")
    print(f"  msg:  {data.get('msg')}")
    if data.get("code") == 0:
        print("  ✅ metrics 성공!")
        d = data["data"]
        print(f"  AUM:       {d.get('totalNetAssets')}")
        print(f"  일별순유입: {d.get('dailyNetInflow')}")
        print(f"  누적순유입: {d.get('cumNetInflow')}")
    else:
        print(f"  ❌ API 에러: {json.dumps(data, indent=2, ensure_ascii=False)}")
except Exception as e:
    print(f"  ❌ 예외: {e}")

# 2. historicalInflowChart 테스트
print("\n[2] historicalInflowChart")
try:
    r = requests.post(
        f"{BASE_URL}/openapi/v2/etf/historicalInflowChart",
        headers=HEADERS,
        json={"type": ETF_TYPE},
        timeout=15,
        verify=False
    )
    print(f"  HTTP status: {r.status_code}")
    data = r.json()
    print(f"  code: {data.get('code')}")
    print(f"  msg:  {data.get('msg')}")
    if data.get("code") == 0:
        records = data["data"]
        print(f"  ✅ history 성공! 레코드 수: {len(records)}")
        if records:
            print(f"  첫 레코드: {records[0]}")
            print(f"  마지막:   {records[-1]}")
    else:
        print(f"  ❌ API 에러: {json.dumps(data, indent=2, ensure_ascii=False)}")
except Exception as e:
    print(f"  ❌ 예외: {e}")

print("\n" + "=" * 50)
print("테스트 완료")
