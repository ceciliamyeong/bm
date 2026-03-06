"""
SoSoValue API 테스트 스크립트
==============================
실행 전: pip install requests
사용법:  python test_sosovalue_api.py

API 키를 환경변수로 넣거나 아래 API_KEY 에 직접 입력
"""

import requests
import json
import os

# ✅ 여기에 API 키 입력 (또는 환경변수 SOSOVALUE_API_KEY 사용)
API_KEY = os.environ.get("SOSOVALUE_API_KEY", "여기에_API_키_입력")

BASE_URL = "https://openapi.sosovalue.com"
HEADERS = {"x-soso-api-key": API_KEY}

# -------------------------------------------------------
# 테스트할 엔드포인트 목록
# -------------------------------------------------------
ENDPOINTS = [
    # ETF 히스토리컬 인플로우 차트
    {
        "name": "BTC ETF Historical Inflow",
        "url": f"{BASE_URL}/api/v1/etf/inflow/history",
        "params": {"etfIndex": "us-btc-spot"},
    },
    {
        "name": "ETH ETF Historical Inflow",
        "url": f"{BASE_URL}/api/v1/etf/inflow/history",
        "params": {"etfIndex": "us-eth-spot"},
    },
    {
        "name": "SOL ETF Historical Inflow",
        "url": f"{BASE_URL}/api/v1/etf/inflow/history",
        "params": {"etfIndex": "us-sol-spot"},
    },
    {
        "name": "XRP ETF Historical Inflow",
        "url": f"{BASE_URL}/api/v1/etf/inflow/history",
        "params": {"etfIndex": "us-xrp-spot"},
    },
    # ETF 현재 메트릭 (AUM, 일별 순유입 등)
    {
        "name": "BTC ETF Current Metrics",
        "url": f"{BASE_URL}/api/v1/etf/metrics",
        "params": {"etfIndex": "us-btc-spot"},
    },
    {
        "name": "ETH ETF Current Metrics",
        "url": f"{BASE_URL}/api/v1/etf/metrics",
        "params": {"etfIndex": "us-eth-spot"},
    },
    {
        "name": "SOL ETF Current Metrics",
        "url": f"{BASE_URL}/api/v1/etf/metrics",
        "params": {"etfIndex": "us-sol-spot"},
    },
    {
        "name": "XRP ETF Current Metrics",
        "url": f"{BASE_URL}/api/v1/etf/metrics",
        "params": {"etfIndex": "us-xrp-spot"},
    },
]

# -------------------------------------------------------
# 테스트 실행
# -------------------------------------------------------
def test_endpoint(name, url, params):
    print(f"\n{'='*60}")
    print(f"🔍 {name}")
    print(f"URL: {url}")
    print(f"Params: {params}")
    print("-" * 60)

    try:
        r = requests.get(url, headers=HEADERS, params=params, timeout=10)
        print(f"Status: {r.status_code}")

        if r.status_code == 200:
            data = r.json()
            print(f"✅ 성공!")
            # 데이터 구조 출력 (처음 500자)
            pretty = json.dumps(data, indent=2, ensure_ascii=False)
            print(pretty[:800] + ("..." if len(pretty) > 800 else ""))
        elif r.status_code == 404:
            print("❌ 404 - 엔드포인트 없음")
        elif r.status_code == 401:
            print("❌ 401 - API 키 인증 실패")
        elif r.status_code == 429:
            print("⚠️  429 - Rate limit 초과")
        else:
            print(f"❌ 에러: {r.text[:300]}")

    except Exception as e:
        print(f"❌ 예외 발생: {e}")


if __name__ == "__main__":
    print("=" * 60)
    print("SoSoValue API 엔드포인트 테스트")
    print("=" * 60)

    if API_KEY == "여기에_API_키_입력":
        print("⚠️  API_KEY 를 입력해주세요!")
        print("   방법 1: 스크립트 상단 API_KEY 변수에 직접 입력")
        print("   방법 2: 환경변수로 실행:")
        print("           SOSOVALUE_API_KEY=your_key python test_sosovalue_api.py")
        exit(1)

    for ep in ENDPOINTS:
        test_endpoint(ep["name"], ep["url"], ep["params"])

    print("\n" + "=" * 60)
    print("테스트 완료!")
