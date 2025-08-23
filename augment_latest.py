# augment_latest.py
import json, time, urllib.request

def _get(url, timeout=10):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.loads(r.read().decode())

def _pick(d, *ks, default=None):
    for k in ks:
        if d is None or k not in d: return default
        d = d[k]
    return d

def fetch_kimchi():
    """
    kimchi ≈ ( (BTC/KRW ÷ USD/KRW) / BTC/USD ) - 1
    간단/안전 버전: CoinGecko simple/price 사용
    """
    px = _get("https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=krw,usd")
    btc_krw, btc_usd = _pick(px,"bitcoin","krw"), _pick(px,"bitcoin","usd")

    # USD/KRW (가능하면 CG simple/price usd→krw; 실패시 암묵 환율로 대체)
    usd_krw = None
    try:
        usdk = _get("https://api.coingecko.com/api/v3/simple/price?ids=usd&vs_currencies=krw")
        usd_krw = _pick(usdk, "usd", "krw")
    except Exception:
        pass
    if not usd_krw and btc_krw and btc_usd:
        usd_krw = btc_krw / btc_usd

    if btc_krw and btc_usd and usd_krw:
        return ( (btc_krw / usd_krw) / btc_usd ) - 1.0
    return None

def fetch_funding():
    """
    Binance Perp funding (최근값)
    """
    out = {}
    try:
        bi_btc = _get("https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT")
        out["btc"] = float(_pick(bi_btc,"lastFundingRate", default="0"))
    except Exception:
        pass
    try:
        bi_eth = _get("https://fapi.binance.com/fapi/v1/premiumIndex?symbol=ETHUSDT")
        out["eth"] = float(_pick(bi_eth,"lastFundingRate", default="0"))
    except Exception:
        pass
    return out or None

def main():
    with open("latest.json","r",encoding="utf-8") as f:
        latest = json.load(f)

    kimchi = fetch_kimchi()
    funding = fetch_funding()

    if kimchi is not None:
        latest["kimchi"] = kimchi
    if funding:
        latest["funding"] = funding

    tmp = f"latest.tmp.{int(time.time())}.json"
    with open(tmp,"w",encoding="utf-8") as f:
        json.dump(latest, f, ensure_ascii=False, indent=2)

    import os; os.replace(tmp, "latest.json")
    print("✅ latest.json enriched with kimchi/funding")

if __name__ == "__main__":
    main()
