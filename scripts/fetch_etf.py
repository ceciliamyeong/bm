"""
SoSoValue ETF 데이터 수집 스크립트
====================================
GitHub Actions에서 6시간마다 실행
BTC/ETH Spot ETF 데이터를 가져와 data/ 폴더에 JSON으로 저장
"""

import requests
import json
import os
from datetime import datetime, timezone

API_KEY = os.environ["SOSOVALUE_API_KEY"]
BASE_URL = "https://api.sosovalue.xyz"
HEADERS = {
    "x-soso-api-key": API_KEY,
    "Content-Type": "application/json"
}

ETF_TYPES = {
    "btc": "us-btc-spot",
    "eth": "us-eth-spot",
}

def fetch_current_metrics(etf_type):
    """현재 ETF 메트릭 (AUM, 일별 순유입 등)"""
    r = requests.post(
        f"{BASE_URL}/openapi/v2/etf/currentEtfDataMetrics",
        headers=HEADERS,
        json={"type": etf_type},
        timeout=15
    )
    r.raise_for_status()
    data = r.json()
    if data.get("code") != 0:
        raise Exception(f"API error: {data.get('msg')}")
    return data["data"]

def fetch_historical_inflow(etf_type):
    """히스토리컬 인플로우 차트 (최근 300일)"""
    r = requests.post(
        f"{BASE_URL}/openapi/v2/etf/historicalInflowChart",
        headers=HEADERS,
        json={"type": etf_type},
        timeout=15
    )
    r.raise_for_status()
    data = r.json()
    if data.get("code") != 0:
        raise Exception(f"API error: {data.get('msg')}")
    return data["data"]

def save_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  ✅ 저장: {path}")

def main():
    updated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[{updated_at}] ETF 데이터 수집 시작")

    all_summary = {"updatedAt": updated_at, "btc": {}, "eth": {}}

    for coin, etf_type in ETF_TYPES.items():
        print(f"\n--- {coin.upper()} ({etf_type}) ---")

        # 현재 메트릭
        try:
            metrics = fetch_current_metrics(etf_type)
            save_json(f"data/etf_{coin}_metrics.json", {
                "updatedAt": updated_at,
                "type": etf_type,
                **metrics
            })
            # 요약용
            all_summary[coin] = {
                "totalNetAssets": metrics.get("totalNetAssets", {}).get("value"),
                "dailyNetInflow": metrics.get("dailyNetInflow", {}).get("value"),
                "cumNetInflow": metrics.get("cumNetInflow", {}).get("value"),
                "dailyTotalValueTraded": metrics.get("dailyTotalValueTraded", {}).get("value"),
                "totalTokenHoldings": metrics.get("totalTokenHoldings", {}).get("value"),
                "lastUpdateDate": metrics.get("dailyNetInflow", {}).get("lastUpdateDate"),
            }
            print(f"  AUM: ${float(metrics['totalNetAssets']['value'])/1e9:.2f}B")
            print(f"  일별 순유입: ${float(metrics['dailyNetInflow']['value'])/1e6:.1f}M")
        except Exception as e:
            print(f"  ❌ metrics 실패: {e}")

        # 히스토리컬
        try:
            history = fetch_historical_inflow(etf_type)
            # 최신순 정렬 (최근 90일만 저장해서 용량 절약)
            history_sorted = sorted(history, key=lambda x: x["date"], reverse=True)[:90]
            save_json(f"data/etf_{coin}_history.json", {
                "updatedAt": updated_at,
                "type": etf_type,
                "data": history_sorted
            })
            print(f"  히스토리: {len(history_sorted)}일치 저장")
        except Exception as e:
            print(f"  ❌ history 실패: {e}")

    # 메인 페이지용 요약 파일
    save_json("data/etf_summary.json", all_summary)
    print(f"\n✅ 완료: {updated_at}")

if __name__ == "__main__":
    main()
