"""
SoSoValue ETF 데이터 수집 스크립트
====================================
실행 시점
- etf_update.yml : 매일 KST 13/14/15시 (UTC 04/05/06시)
- render-letter.yml : 뉴스레터 렌더 직전 (최신 가용 데이터로 갱신)

- BTC / ETH / SOL : 공식 API
    1순위) v2 (openapi.sosovalue.com/openapi/v2/etf/...)
           → 합계 + 개별 ETF 표(list) + 총 보유량 + 시총 비중
    2순위) v1 (openapi.sosovalue.com/openapi/v1/etfs/summary-history)
           → 합계만 제공 (v2 실패 시 대체. 표·보유량은 이번 실행에서 갱신 안 됨)
- XRP / DOGE / HYPE / LINK / LTC / AVAX / HBAR / DOT :
  SoSoValue _next/data JSON (비공식 내부 API)

저장 포맷은 기존과 동일:
  data/etf_{coin}_metrics.json
  data/etf_{coin}_history.json
  data/etf_summary.json   (코인별 lastUpdateDate = 기준일, source = v2|v1)

변경 이력
- 2026-10-13: api.sosovalue.xyz(v2) 404 → openapi.sosovalue.com 으로 도메인 이전
- 2026-10-13: 수집 실패 시 etf_summary.json 이전 값 유지 (뉴스레터 "—" 방지)
- 2026-10-14: v2 우선 + v1 대체, 403/429/5xx 재시도(3회), User-Agent, 키 공백 제거,
              실패 시 응답 본문 [debug] 출력, summary 에 source 기록
"""

import requests
import json
import os
import re
import time
import urllib3
from datetime import datetime, timezone

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── 공식 API 설정 (BTC/ETH/SOL) ──────────────────────────────
API_KEY      = os.environ.get("SOSOVALUE_API_KEY", "").strip()   # Secret 앞뒤 공백/개행 방지
BASE_URL_V2  = "https://openapi.sosovalue.com"                   # + /openapi/v2/etf/...
BASE_URL_V1  = "https://openapi.sosovalue.com/openapi/v1"        # + /etfs/summary-history
API_HEADERS = {
    "x-soso-api-key": API_KEY,
    "Content-Type": "application/json",
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
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

# 일시적 오류로 보고 재시도할 상태코드 (404 는 경로 소멸이므로 재시도 안 함)
RETRY_STATUS  = {403, 429, 500, 502, 503, 504}
RETRY_DELAYS  = [5, 15]          # 1차 실패 후 5초, 2차 실패 후 15초 대기 → 최대 3회 시도

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

def _debug_http(r):
    """실패 원인 파악용: 200이 아니면 상태코드와 응답 본문 앞부분 출력 (키는 출력 안 함)"""
    if r.status_code != 200:
        print(f"  [debug] HTTP {r.status_code}: {r.text[:300]}")

def _request(method, url, **kwargs):
    """공식 API 호출 — 일시적 오류(403/429/5xx/연결 오류)는 최대 3회 시도"""
    last = len(RETRY_DELAYS)
    for attempt in range(last + 1):
        try:
            r = requests.request(method, url, timeout=15, **kwargs)
        except requests.RequestException as e:
            if attempt == last:
                raise
            print(f"  [retry] {type(e).__name__} → {RETRY_DELAYS[attempt]}초 후 재시도 ({attempt + 1}/{last + 1})")
            time.sleep(RETRY_DELAYS[attempt])
            continue
        if r.status_code in RETRY_STATUS and attempt < last:
            print(f"  [retry] HTTP {r.status_code} → {RETRY_DELAYS[attempt]}초 후 재시도 ({attempt + 1}/{last + 1})")
            time.sleep(RETRY_DELAYS[attempt])
            continue
        return r

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
# 공식 API v2 (BTC/ETH/SOL) — 합계 + 개별 ETF 표 + 보유량
# ────────────────────────────────────────────────────────────
def fetch_current_metrics(etf_type):
    r = _request("POST", f"{BASE_URL_V2}/openapi/v2/etf/currentEtfDataMetrics",
                 headers=API_HEADERS, json={"type": etf_type})
    _debug_http(r)
    r.raise_for_status()
    data = r.json()
    if data.get("code") != 0:
        raise Exception(f"API error: {data.get('msg') or data.get('message')}")
    return data["data"]

def fetch_historical_inflow(etf_type):
    r = _request("POST", f"{BASE_URL_V2}/openapi/v2/etf/historicalInflowChart",
                 headers=API_HEADERS, json={"type": etf_type})
    _debug_http(r)
    r.raise_for_status()
    data = r.json()
    if data.get("code") != 0:
        raise Exception(f"API error: {data.get('msg') or data.get('message')}")
    return data["data"]

def process_official_coin_v2(coin, etf_type, updated_at, all_summary):
    """v2 전체 처리. 하나라도 실패하면 예외 → 호출부에서 v1 대체 경로로 (저장은 둘 다 성공한 뒤에만)."""
    metrics     = fetch_current_metrics(etf_type)
    new_records = fetch_historical_inflow(etf_type)

    hist_path     = f"data/etf_{coin}_history.json"
    existing      = load_json(hist_path)
    existing_recs = existing.get("data", []) if existing else []
    prev_count    = len(existing_recs)
    merged_recs   = merge_history(existing_recs, new_records)

    cum = merged_recs[-1].get("cumNetInflow") if merged_recs else None
    if cum is not None:
        metrics.setdefault("cumNetInflow", {})["value"] = str(cum)

    save_json(hist_path, {"updatedAt": updated_at, "type": etf_type, "data": merged_recs})
    save_json(f"data/etf_{coin}_metrics.json", {
        "updatedAt": updated_at, "type": etf_type, **metrics
    })

    daily_obj = metrics.get("dailyNetInflow") or {}
    aum_obj   = metrics.get("totalNetAssets") or {}
    all_summary[coin] = {
        "totalNetAssets":        aum_obj.get("value"),
        "dailyNetInflow":        daily_obj.get("value"),
        "cumNetInflow":          cum,
        "dailyTotalValueTraded": (metrics.get("dailyTotalValueTraded") or {}).get("value"),
        "totalTokenHoldings":    (metrics.get("totalTokenHoldings") or {}).get("value"),
        "lastUpdateDate":        daily_obj.get("lastUpdateDate") or aum_obj.get("lastUpdateDate"),
        "source":                "v2",
    }
    aum   = float(all_summary[coin]["totalNetAssets"] or 0)
    daily = float(all_summary[coin]["dailyNetInflow"] or 0)
    print(f"  AUM: ${aum/1e9:.2f}B / 일별 순유입: ${daily/1e6:.1f}M / 기준일 {all_summary[coin]['lastUpdateDate']} (v2)")
    print(f"  개별 ETF 수: {len(metrics.get('list') or [])} / 보유량 기준일: {(metrics.get('totalTokenHoldings') or {}).get('lastUpdateDate')}")
    print(f"  히스토리: {prev_count}일 → {len(merged_recs)}일 (+ {len(merged_recs) - prev_count}일 추가)")

# ────────────────────────────────────────────────────────────
# 공식 API v1 (대체 경로) — 합계만 제공
# ────────────────────────────────────────────────────────────
def fetch_summary_history(symbol, limit=300):
    """GET /etfs/summary-history — 최신 날짜가 먼저 오는 리스트 반환"""
    r = _request("GET", f"{BASE_URL_V1}/etfs/summary-history",
                 headers=API_HEADERS,
                 params={"symbol": symbol.upper(), "country_code": "US", "limit": limit})
    _debug_http(r)
    r.raise_for_status()
    body = r.json()
    if isinstance(body, dict):
        if body.get("code") not in (0, None):
            raise Exception(f"API error: code={body.get('code')} msg={body.get('message')}")
        body = body.get("data", body)
    return body

def process_official_coin_v1(coin, etf_type, updated_at, all_summary):
    try:
        rows = fetch_summary_history(coin)
        if not rows:
            raise Exception("빈 응답")

        new_records = [{
            "date":             r["date"],
            "totalNetInflow":   float(r["total_net_inflow"]),
            "cumNetInflow":     float(r["cum_net_inflow"]),
            "totalValueTraded": float(r["total_value_traded"]),
        } for r in rows]

        hist_path     = f"data/etf_{coin}_history.json"
        existing      = load_json(hist_path)
        existing_recs = existing.get("data", []) if existing else []
        prev_count    = len(existing_recs)
        merged_recs   = merge_history(existing_recs, new_records)
        save_json(hist_path, {"updatedAt": updated_at, "type": etf_type, "data": merged_recs})

        latest = max(rows, key=lambda x: x["date"])
        aum    = float(latest["total_net_assets"])
        daily  = float(latest["total_net_inflow"])
        cum    = float(latest["cum_net_inflow"])
        vol    = float(latest["total_value_traded"])
        d      = latest["date"]

        # 기존 metrics 파일(개별 ETF list·보유량 등)은 보존하고 합계 필드만 갱신
        metrics_path = f"data/etf_{coin}_metrics.json"
        metrics = load_json(metrics_path) or {}
        metrics.update({
            "updatedAt": updated_at,
            "type":      etf_type,
            "totalNetAssets":        {"value": str(aum),   "lastUpdateDate": d},
            "dailyNetInflow":        {"value": str(daily), "lastUpdateDate": d},
            "cumNetInflow":          {"value": str(cum)},
            "dailyTotalValueTraded": {"value": str(vol)},
        })
        save_json(metrics_path, metrics)

        all_summary[coin] = {
            "totalNetAssets":        str(aum),
            "dailyNetInflow":        str(daily),
            "cumNetInflow":          str(cum),
            "dailyTotalValueTraded": str(vol),
            "totalTokenHoldings":    None,
            "lastUpdateDate":        d,
            "source":                "v1",
        }
        print(f"  AUM: ${aum/1e9:.2f}B / 일별 순유입: ${daily/1e6:.1f}M / 기준일 {d} (v1)")
        print(f"  히스토리: {prev_count}일 → {len(merged_recs)}일 (+ {len(merged_recs) - prev_count}일 추가)")
    except Exception as e:
        print(f"  ❌ v1도 실패: {e}")

def process_official_coin(coin, etf_type, updated_at, all_summary):
    print(f"\n--- {coin.upper()} (공식 API) ---")
    try:
        process_official_coin_v2(coin, etf_type, updated_at, all_summary)
        return
    except Exception as e:
        print(f"  ⚠️ v2 실패: {e}")
    print("  ↪ v1 요약으로 대체 (개별 ETF 표·보유량·시총 비중은 이번 실행에서 갱신 안 됨)")
    process_official_coin_v1(coin, etf_type, updated_at, all_summary)

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

    if not API_KEY:
        print("⚠️ SOSOVALUE_API_KEY 미설정 — BTC/ETH/SOL 공식 API 호출은 실패할 수 있음")
    else:
        print(f"[debug] API 키 길이: {len(API_KEY)}자")

    # 실패 시 이전 값을 유지하기 위해 기존 summary 로드
    prev = load_json("data/etf_summary.json") or {}

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
        for coin, slug in NEXT_COINS.items():
            process_next_coin(coin, slug, build_id, updated_at, all_summary)
    except Exception as e:
        print(f"❌ BUILD_ID 추출 실패: {e}")

    # 3) 수집 실패한 코인은 이전 summary 값 유지 (뉴스레터 "—" 방지)
    for coin in list(OFFICIAL_COINS) + list(NEXT_COINS):
        if not all_summary[coin].get("totalNetAssets") and prev.get(coin):
            all_summary[coin] = prev[coin]
            print(f"⚠️ {coin.upper()} 수집 실패 → 이전 값 유지")

    save_json("data/etf_summary.json", all_summary)
    print(f"\n✅ 완료: {updated_at}")

if __name__ == "__main__":
    main()
