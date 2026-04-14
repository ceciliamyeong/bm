import requests
import json
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
import xml.etree.ElementTree as ET


# ---- 환율: 실시간 우선 + 실패 시 fallback ----
def get_usdkrw_live():
    # 1) Yahoo Finance API 직접 호출 (bm20_daily.py와 동일 방식)
    try:
        r = requests.get(
            "https://query1.finance.yahoo.com/v8/finance/chart/USDKRW=X",
            headers={"User-Agent": "Mozilla/5.0"},
            params={"interval": "1d", "range": "1d"},
            timeout=10,
        )
        r.raise_for_status()
        rate = float(r.json()["chart"]["result"][0]["meta"]["regularMarketPrice"])
        if 900 <= rate <= 2000:
            print(f"[INFO] USDKRW={rate} (yahoo_api:USDKRW=X)")
            return rate, "yahoo_api:USDKRW=X"
    except Exception as e:
        print(f"[WARN] Yahoo FX failed: {e}")

    # 2) open.er-api.com fallback
    try:
        r = requests.get("https://open.er-api.com/v6/latest/USD", timeout=10)
        r.raise_for_status()
        krw = r.json().get("rates", {}).get("KRW")
        if krw and float(krw) > 0:
            print(f"[INFO] USDKRW={float(krw)} (open.er-api.com)")
            return float(krw), "open.er-api.com"
    except Exception as e:
        print(f"[WARN] open.er-api.com failed: {e}")

    # 3) 최후 fallback
    print(f"[WARN] USDKRW fallback: 1500.0")
    return 1500.0, "fallback-fixed"


def get_fear_and_greed():
    try:
        url = "https://api.alternative.me/fng/?limit=1"
        res = requests.get(url, timeout=10).json()
        return {"value": int(res['data'][0]['value']), "status": res['data'][0]['value_classification']}
    except Exception as e:
        print(f"[DEBUG] Fear & Greed API Error: {e}")
        return {"value": 5, "status": "Extreme Fear"}


def get_k_share(api_key, krw_total_24h, usdkrw):
    my_vol_usd = krw_total_24h / usdkrw if usdkrw > 0 else 0
    if not api_key:
        print("[DEBUG] CMC_API_KEY missing")
        return {"global_vol_usd": 0, "krw_vol_usd": round(my_vol_usd, 2), "k_share_percent": 0, "global_volume_field": None}
    try:
        url = "https://pro-api.coinmarketcap.com/v1/global-metrics/quotes/latest"
        headers = {'X-CMC_PRO_API_KEY': api_key}
        response = requests.get(url, headers=headers, timeout=15)
        res = response.json()
        data_root = res.get('data', {}).get('quote', {})
        usd_data = data_root.get('USD') or data_root.get('usd') or {}
        candidates = [
            ("total_volume_24h_adjusted", usd_data.get("total_volume_24h_adjusted")),
            ("total_volume_24h", usd_data.get("total_volume_24h")),
            ("total_volume_24h_reported", usd_data.get("total_volume_24h_reported")),
        ]
        global_vol_usd = 0
        picked_field = None
        for name, val in candidates:
            if val is not None and float(val) > 0:
                global_vol_usd = float(val)
                picked_field = name
                break
        k_share = (my_vol_usd / global_vol_usd) * 100 if global_vol_usd > 0 else 0
        return {"global_vol_usd": round(global_vol_usd, 2), "krw_vol_usd": round(my_vol_usd, 2), "k_share_percent": round(k_share, 2), "global_volume_field": picked_field}
    except Exception as e:
        print(f"[DEBUG] CMC API parsing error: {e}")
        return {"global_vol_usd": 0, "krw_vol_usd": round(my_vol_usd, 2), "k_share_percent": 0, "global_volume_field": None}


def get_upbit_xrp_krw_24h():
    r = requests.get("https://api.upbit.com/v1/ticker", params={"markets": "KRW-XRP"}, timeout=15)
    r.raise_for_status()
    return float(r.json()[0].get("acc_trade_price_24h", 0.0))


def get_bithumb_xrp_krw_24h():
    r = requests.get("https://api.bithumb.com/public/ticker/ALL_KRW", timeout=15)
    r.raise_for_status()
    xrp = (r.json().get("data") or {}).get("XRP") or {}
    return float(xrp.get("acc_trade_value_24H", 0.0))


def get_coinone_xrp_krw_24h():
    r = requests.get("https://api.coinone.co.kr/public/v2/ticker_new/KRW/XRP", timeout=15)
    r.raise_for_status()
    tickers = r.json().get("tickers") or []
    return float(tickers[0].get("quote_volume", 0.0)) if tickers else 0.0


def get_cmc_global_xrp_usd_24h(api_key):
    if not api_key:
        raise ValueError("CMC_API_KEY missing")
    r = requests.get(
        "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest",
        headers={'X-CMC_PRO_API_KEY': api_key},
        params={"symbol": "XRP", "convert": "USD"},
        timeout=15,
    )
    r.raise_for_status()
    quote = ((r.json().get("data") or {}).get("XRP") or {}).get("quote", {}).get("USD", {}) or {}
    return float(quote.get("volume_24h", 0.0))


def get_xrp_share(api_key, usdkrw):
    errors = []
    try:
        upbit_krw = get_upbit_xrp_krw_24h()
    except Exception as e:
        upbit_krw = 0.0; errors.append(f"upbit:{e}")
    try:
        bithumb_krw = get_bithumb_xrp_krw_24h()
    except Exception as e:
        bithumb_krw = 0.0; errors.append(f"bithumb:{e}")
    try:
        coinone_krw = get_coinone_xrp_krw_24h()
    except Exception as e:
        coinone_krw = 0.0; errors.append(f"coinone:{e}")

    korea_krw = upbit_krw + bithumb_krw + coinone_krw
    korea_usd = korea_krw / usdkrw if usdkrw > 0 else 0.0

    try:
        global_usd = get_cmc_global_xrp_usd_24h(api_key)
    except Exception as e:
        global_usd = 0.0; errors.append(f"cmc:{e}")

    share = (korea_usd / global_usd) * 100 if global_usd > 0 else 0.0
    return {
        "as_of": None,
        "symbol": "XRP",
        "usdkrw": usdkrw,
        "korea": {
            "upbit_krw_24h": round(upbit_krw, 2),
            "bithumb_krw_24h": round(bithumb_krw, 2),
            "coinone_krw_24h": round(coinone_krw, 2),
            "total_krw_24h": round(korea_krw, 2),
            "total_usd_24h": round(korea_usd, 2),
        },
        "global": {"cmc_xrp_volume_usd_24h": round(global_usd, 2)},
        "k_xrp_share_pct_24h": round(share, 4),
        "errors": errors,
        "notes": [
            "Korea: Upbit+Bithumb+Coinone XRP/KRW spot traded value(24h) converted to USD",
            "Global: CMC XRP volume_24h (USD)",
        ]
    }


def _today_kst() -> str:
    kst = timezone(timedelta(hours=9))
    return datetime.now(kst).strftime("%Y-%m-%d")


def append_json_list(path: Path, item: dict, date_key: str = "timestamp"):
    path.parent.mkdir(parents=True, exist_ok=True)
    lst = []
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                lst = json.load(f)
                if not isinstance(lst, list):
                    lst = []
        except Exception:
            lst = []
    today = _today_kst()
    lst = [x for x in lst if str(x.get(date_key, ""))[:10] != today]
    lst.append(item)
    lst = lst[-500:]
    with open(path, "w", encoding="utf-8") as f:
        json.dump(lst, f, indent=2, ensure_ascii=False)
    print(f"[OK] {path.name}: {len(lst)}개 항목 저장 (오늘={today})")


def main():
    CMC_API_KEY = os.environ.get('CMC_API_KEY')

    usdkrw, fx_source = get_usdkrw_live()

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
    xrp_market = get_xrp_share(CMC_API_KEY, usdkrw)

    now_iso = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
    xrp_market["as_of"] = now_iso

    new_entry = {
        "timestamp": now_iso,
        "sentiment": sentiment,
        "k_market": k_market,
        "usdkrw": usdkrw,
        "fx_source": fx_source
    }
    append_json_list(Path("data/bm20_history.json"), new_entry, date_key="timestamp")

    latest_path = Path("out/global/k_xrp_share_24h_latest.json")
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    with open(latest_path, "w", encoding="utf-8") as f:
        json.dump({**xrp_market, "fx_source": fx_source}, f, indent=2, ensure_ascii=False)

    append_json_list(
        Path("out/global/k_xrp_share_24h_history.json"),
        {**xrp_market, "fx_source": fx_source},
        date_key="as_of"
    )

    print(f"[FINAL] BM20 업데이트 완료 - K-Share: {k_market['k_share_percent']}%")
    print(f"[FINAL] XRP 저장 완료(out/global) - K-XRP Share(24H): {xrp_market['k_xrp_share_pct_24h']}%")
    print(f"[INFO] USDKRW={usdkrw} ({fx_source})")


if __name__ == "__main__":
    main()
