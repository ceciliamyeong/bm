"""
SoSoValue ETF 데이터 수집 스크립트
====================================
GitHub Actions에서 6시간마다 실행

- BTC / ETH / SOL : 공식 API (api.sosovalue.xyz)
- XRP / DOGE / HYPE / LINK / LTC / AVAX / HBAR / DOT :
  SoSoValue _next/data JSON (비공식 내부 API)

저장 포맷은 기존과 동일:
  data/etf_{coin}_metrics.json
  data/etf_{coin}_history.json
  data/etf_summary.json
"""

import requests
import json
import os
import re
import urllib3
from datetime import datetime, timezone

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── 공식 API 설정 (BTC/ETH/SOL) ──────────────────────────────
API_KEY  = os.environ.get("SOSOVALUE_API_KEY", "")
BASE_URL = "https://api.sosovalue.xyz"
API_HEADERS = {
    "x-soso-api-key": API_KEY,
    "Content-Type": "application/json"
}

OFFICIAL_COINS = {
    "btc": "us-btc-spot",
    "eth": "us-eth-spot",
    "sol": "us-sol-spot",
}

# ── _next/data 대상 코인 (slug 기준) ─────────────────────────
NEXT_COINS = {
    "xrp":  "us-xrp-spot",
    "doge": "us-doge-spot",
    "hype": "us-hype-spot",
    "link": "us-link-spot",
    "ltc":  "us-ltc-spot",
    "avax": "us-avax-spot",
    "hbar": "us-hbar-spot",
    "dot":  "us-dot-spot",
}

WEB_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://sosovalue.com/",
    "Accept": "application/json",
}

# ────────────────────────────────────────────────────────────
# 유틸
# ────────────────────────────────────────────────────────────
def load_json(path):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None

def save_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  ✅ 저장: {path}")

def merge_history(existing_records, new_records, date_key="date"):
    """date 기준 중복 제거 후 정렬 (신규 우선)"""
    merged = {}
    for row in existing_records:
        merged[row[date_key]] = row
    for row in new_records:
        merged[row[date_key]] = row
    return sorted(merged.values(), key=lambda x: x[date_key])

# ────────────────────────────────────────────────────────────
# BUILD_ID 자동 추출
# ────────────────────────────────────────────────────────────
def get_build_id():
    r = requests.get("https://sosovalue.com/", headers=WEB_HEADERS, timeout=15)
    r.raise_for_status()
    m = re.search(r'"buildId"\s*:\s*"([^"]+)"', r.text)
    if not m:
        raise Exception("BUILD_ID를 찾을 수 없습니다")
    build_id = m.group(1)
    print(f"  BUILD_ID: {build_id}")
    return build_id

# ────────────────────────────────────────────────────────────
# 공식 API (BTC/ETH/SOL)
# ────────────────────────────────────────────────────────────
def fetch_current_metrics(etf_type):
    r = requests.post(
        f"{BASE_URL}/openapi/v2/etf/currentEtfDataMetrics",
        headers=API_HEADERS,
        json={"type": etf_type},
        timeout=15,
        verify=False
    )
    r.raise_for_status()
    data = r.json()
    if data.get("code") != 0:
        raise Exception(f"API error: {data.get('msg')}")
    return data["data"]

def fetch_historical_inflow(etf_type):
    r = requests.post(
        f"{BASE_URL}/openapi/v2/etf/historicalInflowChart",
        headers=API_HEADERS,
        json={"type": etf_type},
        timeout=15,
        verify=False
    )
    r.raise_for_status()
    data = r.json()
    if data.get("code") != 0:
        raise Exception(f"API error: {data.get('msg')}")
    return data["data"]

def process_official_coin(coin, etf_type, updated_at, all_summary):
    print(f"\n--- {coin.upper()} (공식 API) ---")
    metrics = None
    try:
        metrics = fetch_current_metrics(etf_type)
        all_summary[coin] = {
            "totalNetAssets":        metrics.get("totalNetAssets", {}).get("value"),
            "dailyNetInflow":        metrics.get("dailyNetInflow", {}).get("value"),
            "cumNetInflow":          None,
            "dailyTotalValueTraded": metrics.get("dailyTotalValueTraded", {}).get("value"),
            "totalTokenHoldings":    metrics.get("totalTokenHoldings", {}).get("value"),
            "lastUpdateDate":        metrics.get("dailyNetInflow", {}).get("lastUpdateDate"),
        }
        print(f"  AUM: ${float(metrics['totalNetAssets']['value'])/1e9:.2f}B")
        print(f"  일별 순유입: ${float(metrics['dailyNetInflow']['value'])/1e6:.1f}M")
    except Exception as e:
        print(f"  ❌ metrics 실패: {e}")

    try:
        new_records   = fetch_historical_inflow(etf_type)
        hist_path     = f"data/etf_{coin}_history.json"
        existing      = load_json(hist_path)
        existing_recs = existing.get("data", []) if existing else []
        prev_count    = len(existing_recs)
        merged_recs   = merge_history(existing_recs, new_records)

        save_json(hist_path, {"updatedAt": updated_at, "type": etf_type, "data": merged_recs})

        cum = merged_recs[-1].get("cumNetInflow") if merged_recs else None
        all_summary[coin]["cumNetInflow"] = cum

        if metrics is not None and cum is not None:
            metrics["cumNetInflow"]["value"] = str(cum)
        if metrics is not None:
            save_json(f"data/etf_{coin}_metrics.json", {
                "updatedAt": updated_at, "type": etf_type, **metrics
            })
            print(f"  누적 순유입: ${cum/1e9:.2f}B (history 기준)")

        print(f"  히스토리: {prev_count}일 → {len(merged_recs)}일 (+ {len(merged_recs) - prev_count}일 추가)")
    except Exception as e:
        print(f"  ❌ history 실패: {e}")

# ────────────────────────────────────────────────────────────
# _next/data (알트코인)
# ────────────────────────────────────────────────────────────
def fetch_next_data(slug, build_id):
    url = f"https://sosovalue.com/_next/data/{build_id}/assets/etf/{slug}.json"
    r = requests.get(url, headers=WEB_HEADERS, timeout=15)
    r.raise_for_status()
    return r.json()["pageProps"]

def convert_next_metrics(props, coin, updated_at):
    """
    _next/data pageProps → 공식 API metrics 포맷으로 변환
    HTML의 renderKPI / renderTable 이 그대로 동작하도록 맞춤
    """
    # 집계 합산 (data 리스트의 최신 레코드 기준)
    data_list   = props.get("data", [])           # 개별 ETF 목록
    hist_list   = props.get("historyData", {}).get("list", [])

    # 최신 히스토리 레코드 (날짜 내림차순 첫번째)
    latest_hist = hist_list[0] if hist_list else {}

    total_nav    = latest_hist.get("totalNetAssets", 0) or 0
    daily_inflow = latest_hist.get("totalNetInflow", 0) or 0
    cum_inflow   = latest_hist.get("cumNetInflow", 0) or 0
    total_vol    = latest_hist.get("totalVolume", 0) or 0
    last_date    = (latest_hist.get("dataDate") or "")[:10]

    # 개별 ETF 리스트를 공식 API 포맷으로 변환
    etf_list = []
    for etf in data_list:
        etf_list.append({
            "ticker":   etf.get("ticker"),
            "institute": etf.get("inst"),
            "netAssets":      {"value": etf.get("totalNav")},
            "dailyNetInflow": {"value": etf.get("netInflow"), "lastUpdateDate": last_date},
            "cumNetInflow":   {"value": etf.get("cumNetInflow")},
            "dailyValueTraded": {"value": etf.get("volume")},
            "fee":            {"value": etf.get("fee")},
        })

    return {
        "updatedAt": updated_at,
        "type":      props.get("cate", {}).get("slug", coin),
        "totalNetAssets":        {"value": str(total_nav),    "lastUpdateDate": last_date},
        "dailyNetInflow":        {"value": str(daily_inflow), "lastUpdateDate": last_date},
        "cumNetInflow":          {"value": str(cum_inflow)},
        "dailyTotalValueTraded": {"value": str(total_vol)},
        "totalTokenHoldings":    {"value": None},
        "totalNetAssetsPercentage": {"value": None},
        "list": etf_list,
    }

def convert_next_history(props):
    """
    historyData.list → 공식 API history 포맷으로 변환
    { date, totalNetInflow, cumNetInflow, totalValueTraded }
    """
    records = []
    for row in props.get("historyData", {}).get("list", []):
        records.append({
            "date":           (row.get("dataDate") or "")[:10],
            "totalNetInflow": row.get("totalNetInflow", 0) or 0,
            "cumNetInflow":   row.get("cumNetInflow", 0) or 0,
            "totalValueTraded": row.get("totalVolume", 0) or 0,
        })
    return records

def process_next_coin(coin, slug, build_id, updated_at, all_summary):
    print(f"\n--- {coin.upper()} (_next/data) ---")
    try:
        props       = fetch_next_data(slug, build_id)

        if coin == "xrp":
            data_list = props.get("data", [])
            if data_list:
                print("coinPerShare 샘플:", data_list[0].get("coinPerShare"))
            hist_list = props.get("historyData", {}).get("list", [])
            if hist_list:
                print("historyData 키 목록:", list(hist_list[0].keys()))
                print("historyData 최신:", hist_list[0])
        metrics_out = convert_next_metrics(props, coin, updated_at)
        new_records = convert_next_history(props)

        # 히스토리 머지
        hist_path     = f"data/etf_{coin}_history.json"
        existing      = load_json(hist_path)
        existing_recs = existing.get("data", []) if existing else []
        prev_count    = len(existing_recs)
        merged_recs   = merge_history(existing_recs, new_records)

        save_json(hist_path, {"updatedAt": updated_at, "type": slug, "data": merged_recs})
        save_json(f"data/etf_{coin}_metrics.json", metrics_out)

        daily = float(metrics_out["dailyNetInflow"]["value"] or 0)
        aum   = float(metrics_out["totalNetAssets"]["value"] or 0)
        cum   = float(metrics_out["cumNetInflow"]["value"] or 0)
        print(f"  AUM: ${aum/1e6:.1f}M")
        print(f"  일별 순유입: ${daily/1e6:.1f}M")
        print(f"  누적 순유입: ${cum/1e6:.1f}M")
        print(f"  히스토리: {prev_count}일 → {len(merged_recs)}일 (+ {len(merged_recs) - prev_count}일 추가)")

        all_summary[coin] = {
            "totalNetAssets":        metrics_out["totalNetAssets"]["value"],
            "dailyNetInflow":        metrics_out["dailyNetInflow"]["value"],
            "cumNetInflow":          metrics_out["cumNetInflow"]["value"],
            "dailyTotalValueTraded": metrics_out["dailyTotalValueTraded"]["value"],
            "lastUpdateDate":        metrics_out["dailyNetInflow"]["lastUpdateDate"],
        }
    except Exception as e:
        print(f"  ❌ 실패: {e}")

# ────────────────────────────────────────────────────────────
# main
# ────────────────────────────────────────────────────────────
def main():
    updated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[{updated_at}] ETF 데이터 수집 시작")

    all_summary = {"updatedAt": updated_at}
    for coin in list(OFFICIAL_COINS) + list(NEXT_COINS):
        all_summary[coin] = {}

    # 1) 공식 API 코인
    for coin, etf_type in OFFICIAL_COINS.items():
        process_official_coin(coin, etf_type, updated_at, all_summary)

    # 2) _next/data 코인 — BUILD_ID 한 번만 추출
    print("\n[BUILD_ID 추출 중...]")
    try:
        build_id = get_build_id()
        try:
            funding_url = f"https://sosovalue.com/_next/data/{build_id}/dashboard/funding-rate.json"
            r = requests.get(funding_url, headers=WEB_HEADERS, timeout=15)
            if r.status_code == 200:
                props = r.json().get("pageProps", {})
                res_list = props.get("resList", [])
                print(f"[funding-rate] resList 길이: {len(res_list)}")
                if res_list:
                    print(f"[funding-rate] 첫번째 항목 키: {list(res_list[0].keys())}")
                    print(f"[funding-rate] 샘플: {res_list[0]}")
        except Exception as e:
            print(f"[funding-rate] failed: {e}")
      
        for coin, slug in NEXT_COINS.items():
            process_next_coin(coin, slug, build_id, updated_at, all_summary)
    except Exception as e:
        print(f"❌ BUILD_ID 추출 실패: {e}")

    save_json("data/etf_summary.json", all_summary)
    print(f"\n✅ 완료: {updated_at}")

if __name__ == "__main__":
    main()
