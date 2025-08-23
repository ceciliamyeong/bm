# augment_latest.py
# - bm20_latest.json / latest.json 중 존재하는 걸 찾아 김치/펀딩 값을 주입
# - 외부 API 오류 시 조용히 건너뜀(사이트는 계속 동작)

import json, urllib.request, os, time

UA = {"User-Agent": "BM20-Enrich/1.0"}

def get(url, timeout=12):
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception:
        return None

def upbit_btc_krw():
    j = get("https://api.upbit.com/v1/ticker?markets=KRW-BTC")
    try:
        return float(j[0]["trade_price"])
    except Exception:
        return None

def binance_px(sym):
    j = get(f"https://api.binance.com/api/v3/ticker/price?symbol={sym}")
    try:
        return float(j["price"])
    except Exception:
        return None

def compute_kimchi():
    krw = upbit_btc_krw()
    usdt = binance_px("BTCUSDT")
    if krw and usdt:
        try:
            usdkrw = krw / usdt  # 암묵 환율
            return ((krw / usdkrw) / usdt) - 1.0
        except Exception:
            return None
    return None

def fetch_funding():
    out = {}
    j1 = get("https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT")
    if j1 and "lastFundingRate" in j1:
        try:
            out["btc"] = float(j1["lastFundingRate"])
        except Exception:
            pass
    j2 = get("https://fapi.binance.com/fapi/v1/premiumIndex?symbol=ETHUSDT")
    if j2 and "lastFundingRate" in j2:
        try:
            out["eth"] = float(j2["lastFundingRate"])
        except Exception:
            pass
    return out or None

def pick_latest():
    for f in ("bm20_latest.json","latest.json","site/bm20_latest.json","site/latest.json"):
        if os.path.exists(f):
            return f
    return None

def main():
    target = pick_latest()
    if not target:
        print("no latest.json-like file; skip")
        return
    with open(target, "r", encoding="utf-8") as fp:
        data = json.load(fp)

    k = compute_kimchi()
    f = fetch_funding()
    if k is not None:
        data["kimchi"] = k
    if f:
        data["funding"] = f

    tmp = f"{target}.tmp.{int(time.time())}"
    with open(tmp, "w", encoding="utf-8") as fp:
        json.dump(data, fp, ensure_ascii=False, indent=2)
    os.replace(tmp, target)
    print("✅ enriched", target)

if __name__ == "__main__":
    main()
