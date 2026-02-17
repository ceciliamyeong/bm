import requests
import json
import os
from datetime import datetime
from pathlib import Path
import xml.etree.ElementTree as ET

# ---- 환율: 실시간 우선 + 실패 시 fallback ----
def get_usdkrw_live():
    # 1) open.er-api.com (키 없이)
    try:
        url = "https://open.er-api.com/v6/latest/USD"
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        j = r.json()
        krw = j.get("rates", {}).get("KRW")
        if krw and float(krw) > 0:
            return float(krw), "open.er-api.com"
    except Exception:
        pass

    # 2) ECB fallback (EUR base -> USDKRW 계산)
    try:
        url = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml"
        r = requests.get(url, timeout=10)
        r.raise_for_status()

        root = ET.fromstring(r.text)
        ns = {
            "gesmes": "http://www.gesmes.org/xml/2002-08-01",
            "eurofxref": "http://www.ecb.int/vocabulary/2002-08-01/eurofxref"
        }
        cubes = root.findall(".//eurofxref:Cube/eurofxref:Cube/eurofxref:Cube", ns)

        rates = {}
        for c in cubes:
            cur = c.attrib.get("currency")
            rate = c.attrib.get("rate")
            if cur and rate:
                rates[cur] = float(rate)

        usd_per_eur = rates.get("USD")
        krw_per_eur = rates.get("KRW")
        if usd_per_eur and krw_per_eur and usd_per_eur > 0:
            return krw_per_eur / usd_per_eur, "ecb.europa.eu"
    except Exception:
        pass

    # 최후 fallback
    return 1450.0, "fallback-fixed"


def get_fear_and_greed():
    """심리 지수 가져오기"""
    try:
        url = "https://api.alternative.me/fng/?limit=1"
        res = requests.get(url, timeout=10).json()
        return {"value": int(res['data'][0]['value']), "status": res['data'][0]['value_classification']}
    except Exception as e:
        print(f"[DEBUG] Fear & Greed API Error: {e}")
        return {"value": 5, "status": "Extreme Fear"}


def get_k_share(api_key, krw_total_24h, usdkrw):
    """한국 시장 점유율 계산 (한국 전체 현물 / 글로벌 현물(집계) )"""
    my_vol_usd = krw_total_24h / usdkrw if usdkrw > 0 else 0

    if not api_key:
        print("[DEBUG] CMC_API_KEY missing")
        return {"global_vol_usd": 0, "krw_vol_usd": my_vol_usd, "k_share_percent": 0}

    try:
        url = "https://pro-api.coinmarketcap.com/v1/global-metrics/quotes/latest"
        headers = {'X-CMC_PRO_API_KEY': api_key}
        response = requests.get(url, headers=headers, timeout=15)
        res = response.json()

        data_root = res.get('data', {}).get('quote', {})
        usd_data = data_root.get('USD') or data_root.get('usd') or {}

        global_vol_usd = usd_data.get('total_volume_24h', 0)
        k_share = (my_vol_usd / global_vol_usd) * 100 if global_vol_usd > 0 else 0

        return {
            "global_vol_usd": round(global_vol_usd, 2),
            "krw_vol_usd": round(my_vol_usd, 2),
            "k_share_percent": round(k_share, 2)
        }
    except Exception as e:
        print(f"[DEBUG] CMC API parsing error: {e}")
        return {"global_vol_usd": 0, "krw_vol_usd": my_vol_usd, "k_share_percent": 0}


# ---- XRP: 한국 3거래소 XRP/KRW 24h 거래대금(KRW) ----
def get_upbit_xrp_krw_24h():
    url = "https://api.upbit.com/v1/ticker"
    params = {"markets": "KRW-XRP"}
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    data = r.json()
    return float(data[0].get("acc_trade_price_24h", 0.0))


def get_bithumb_xrp_krw_24h():
    url = "https://api.bithumb.com/public/ticker/ALL_KRW"
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    j = r.json()
    xrp = (j.get("data") or {}).get("XRP") or {}
    return float(xrp.get("acc_trade_value_24H", 0.0))


def get_coinone_xrp_krw_24h():
    url = "https://api.coinone.co.kr/public/v2/ticker_new/KRW/XRP"
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    j = r.json()
    tickers = j.get("tickers") or []
    return float(tickers[0].get("quote_volume", 0.0)) if tickers else 0.0


def get_cmc_global_xrp_usd_24h(api_key):
    if not api_key:
        raise ValueError("CMC_API_KEY missing")

    url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"
    headers = {'X-CMC_PRO_API_KEY': api_key}
    params = {"symbol": "XRP", "convert": "USD"}

    r = requests.get(url, headers=headers, params=params, timeout=15)
    r.raise_for_status()
    j = r.json()

    quote = ((j.get("data") or {}).get("XRP") or {}).get("quote", {}).get("USD", {}) or {}
    return float(quote.get("volume_24h", 0.0))


def get_xrp_share(api_key, usdkrw):
    """K-XRP Spot Share (24H): (Upbit+Bithumb+Coinone XRP/KRW 24h) / (CMC XRP global 24h)"""
    errors = []

    try:
        upbit_krw = get_upbit_xrp_krw_24h()
    except Exception as e:
        upbit_krw = 0.0
        errors.append(f"upbit:{e}")

    try:
        bithumb_krw = get_bithumb_xrp_krw_24h()
    except Exception as e:
        bithumb_krw = 0.0
        errors.append(f"bithumb:{e}")

    try:
        coinone_krw = get_coinone_xrp_krw_24h()
    except Exception as e:
        coinone_krw = 0.0
        errors.append(f"coinone:{e}")

    korea_krw = upbit_krw + bithumb_krw + coinone_krw
    korea_usd = korea_krw / usdkrw if usdkrw > 0 else 0.0

    try:
        global_usd = get_cmc_global_xrp_usd_24h(api_key)
    except Exception as e:
        global_usd = 0.0
        errors.append(f"cmc:{e}")

    share = (korea_usd / global_usd) * 100 if global_usd > 0 else 0.0

    return {
        "symbol": "XRP",
        "korea": {
            "upbit_krw_24h": round(upbit_krw, 2),
            "bithumb_krw_24h": round(bithumb_krw, 2),
            "coinone_krw_24h": round(coinone_krw, 2),
            "total_krw_24h": round(korea_krw, 2),
            "total_usd_24h": round(korea_usd, 2),
        },
        "global": {
            "cmc_xrp_volume_usd_24h": round(global_usd, 2),
        },
        "k_xrp_share_pct_24h": round(share, 4),
        "errors": errors,
        "notes": [
            "Korea: Upbit+Bithumb+Coinone XRP/KRW spot traded value(24h) converted to USD",
            "Global: CMC XRP volume_24h (USD)",
        ]
    }


def main():
    CMC_API_KEY = os.environ.get('CMC_API_KEY')

    # ✅ 실시간 USDKRW
    usdkrw, fx_source = get_usdkrw_live()

    # 한국 전체(3거래소 합) 24h 거래대금(KRW) - 기존 파일 그대로 사용
    latest_vol_path = Path("out/history/krw_24h_latest.json")
    krw_total_24h = 0
    if latest_vol_path.exists():
        try:
            with open(latest_vol_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                krw_total_24h = data.get("totals", {}).get("combined_24h", 0)
                print(f"[SUCCESS] 거래량 추출 성공: {krw_total_24h}")
        except Exception as e:
            print(f"[DEBUG] File read error: {e}")
    else:
        print("[ERROR] 파일을 찾을 수 없습니다.")

    sentiment = get_fear_and_greed()
    k_market = get_k_share(CMC_API_KEY, krw_total_24h, usdkrw)

    # ✅ XRP 점유율 추가
    xrp_market = get_xrp_share(CMC_API_KEY, usdkrw)

    now_iso = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")

    new_entry = {
        "timestamp": now_iso,
        "sentiment": sentiment,
        "k_market": k_market,
        "xrp_market": xrp_market,
        "usdkrw": usdkrw,
        "fx_source": fx_source
    }

    # 1) 히스토리 누적 저장(기존 유지)
    history_file = Path("data/bm20_history.json")
    history_file.parent.mkdir(parents=True, exist_ok=True)

    history_list = []
    if history_file.exists():
        with open(history_file, 'r', encoding='utf-8') as f:
            try:
                history_list = json.load(f)
                if not isinstance(history_list, list):
                    history_list = []
            except:
                history_list = []

    history_list.append(new_entry)
    with open(history_file, 'w', encoding='utf-8') as f:
        json.dump(history_list, f, indent=4, ensure_ascii=False)

    # 2) ✅ “읽기 편한 위치” 최신 스냅샷도 저장
    out_latest = Path("out/history/k_xrp_share_24h_latest.json")
    out_latest.parent.mkdir(parents=True, exist_ok=True)
    with open(out_latest, 'w', encoding='utf-8') as f:
        json.dump({
            "as_of": now_iso,
            "usdkrw": usdkrw,
            "fx_source": fx_source,
            **xrp_market
        }, f, indent=2, ensure_ascii=False)

    print(f"[FINAL] 업데이트 완료 - K-Share: {k_market['k_share_percent']}% | K-XRP Share(24H): {xrp_market['k_xrp_share_pct_24h']}%")
    print(f"[INFO] USDKRW={usdkrw} ({fx_source})")


if __name__ == "__main__":
    main()
