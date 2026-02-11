#!/usr/bin/env python3
# augment_latest.py
# 기존 latest* JSON을 읽어 kimchi/funding을 "주입"하고, api/ 폴더는 쓰지 않음.
# - Kimchi: Upbit BTC/KRW + Yahoo BTC-USD + USDKRW(FX)  (FX 실패 시 1450)
# - Funding: Binance Futures premiumIndex -> Bybit v5 tickers -> cache
# - Output: src 파일을 그대로 원자적으로 덮어쓰기 (tmp -> replace)

import json, urllib.request, os, time
from pathlib import Path

UA = {"User-Agent": "BM20-Enrich/1.1"}

# ---------- HTTP helpers ----------
def get_json(url, timeout=12):
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception:
        return None

def get_float(obj, *keys):
    try:
        if obj is None:
            return None
        cur = obj
        for k in keys:
            if isinstance(cur, dict) and k in cur:
                cur = cur[k]
            else:
                return None
        return float(cur)
    except Exception:
        return None

# ---------- Data sources ----------
def upbit_btc_krw():
    j = get_json("https://api.upbit.com/v1/ticker?markets=KRW-BTC")
    try:
        return float(j[0]["trade_price"])
    except Exception:
        return None

def yahoo_btc_usd_last():
    """
    Yahoo chart endpoint (no yfinance dependency).
    Returns last close/regularMarketPrice-like value for BTC-USD.
    """
    url = "https://query1.finance.yahoo.com/v8/finance/chart/BTC-USD?range=2d&interval=1d"
    j = get_json(url)
    try:
        res = j["chart"]["result"][0]
        meta = res.get("meta", {}) or {}
        # 1) Prefer regularMarketPrice if exists
        if "regularMarketPrice" in meta and meta["regularMarketPrice"] is not None:
            return float(meta["regularMarketPrice"])
        # 2) Fallback to last close in indicators/quote/close
        closes = res["indicators"]["quote"][0]["close"]
        closes = [c for c in closes if c is not None]
        if closes:
            return float(closes[-1])
    except Exception:
        pass
    return None

def usdkrw_rate(default=1450.0):
    """
    USDKRW from exchangerate.host (free). If fails, use default.
    """
    j = get_json("https://api.exchangerate.host/latest?base=USD&symbols=KRW")
    try:
        v = float(j["rates"]["KRW"])
        # sanity band
        if 800 <= v <= 2500:
            return v, "exchangerate.host"
    except Exception:
        pass
    return float(default), f"fallback{int(default)}"

# ---------- Kimchi ----------
def compute_kimchi_pct():
    """
    Kimchi premium (percent) = Upbit(KRW) / (Yahoo(BTC-USD)*USDKRW) - 1
    """
    krw = upbit_btc_krw()
    usd = yahoo_btc_usd_last()
    fx, fx_src = usdkrw_rate(default=1450.0)

    if krw is None or usd is None or fx is None or fx == 0:
        return None, {"upbit_btc_krw": krw, "yahoo_btc_usd": usd, "usdkrw": fx, "fx_src": fx_src}

    try:
        implied_krw = usd * fx
        prem = (krw / implied_krw) - 1.0
        return prem * 100.0, {"upbit_btc_krw": krw, "yahoo_btc_usd": usd, "usdkrw": fx, "fx_src": fx_src}
    except Exception:
        return None, {"upbit_btc_krw": krw, "yahoo_btc_usd": usd, "usdkrw": fx, "fx_src": fx_src}

# ---------- Funding ----------
def binance_futures_funding_pct(symbol="BTCUSDT"):
    # premiumIndex: lastFundingRate
    j = get_json(f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={symbol}")
    if isinstance(j, dict) and j.get("lastFundingRate") is not None:
        try:
            return float(j["lastFundingRate"]) * 100.0
        except Exception:
            return None
    return None

def bybit_funding_pct(symbol="BTCUSDT"):
    # Bybit v5 tickers: fundingRate
    j = get_json(f"https://api.bybit.com/v5/market/tickers?category=linear&symbol={symbol}")
    try:
        lst = j.get("result", {}).get("list", [])
        if lst and lst[0].get("fundingRate") is not None:
            return float(lst[0]["fundingRate"]) * 100.0
    except Exception:
        pass
    return None

# ---------- File handling ----------
def pick_latest():
    # repo에서 실제로 쓰는 후보들 우선순위
    candidates = [
        "bm/bm20_latest.json",
        "bm/latest.json",
        "bm20_latest.json",
        "latest.json",
        "site/bm20_latest.json",
        "site/latest.json",
    ]
    for f in candidates:
        if os.path.exists(f):
            return f
    return None

def atomic_write_json(path, data):
    p = Path(path)
    tmp = p.with_suffix(p.suffix + f".tmp.{int(time.time())}")
    with open(tmp, "w", encoding="utf-8") as fp:
        json.dump(data, fp, ensure_ascii=False, indent=2)
    os.replace(tmp, p)

# ---------- Main ----------
def main():
    src = pick_latest()
    if not src:
        print("no latest.json-like file found; skip")
        return

    with open(src, "r", encoding="utf-8") as fp:
        data = json.load(fp)

    # cache dir
    cache_dir = Path("cache")
    cache_dir.mkdir(parents=True, exist_ok=True)
    kp_cache = cache_dir / "kimchi_last.json"
    fd_cache = cache_dir / "funding_last.json"

    # ---- Kimchi ----
    kimchi_pct, kmeta = compute_kimchi_pct()
    if kimchi_pct is None:
        # cache fallback
        try:
            last = json.loads(kp_cache.read_text(encoding="utf-8"))
            kimchi_pct = float(last.get("kimchi_premium_pct")) if last.get("kimchi_premium_pct") is not None else None
            if kimchi_pct is not None:
                data["kimchi_premium_pct"] = kimchi_pct
                data["kimchi_meta"] = {**(last.get("kimchi_meta") or {}), "is_cache": True}
        except Exception:
            pass
    else:
        data["kimchi_premium_pct"] = kimchi_pct
        data["kimchi_meta"] = {**kmeta, "is_cache": False, "ts": int(time.time())}
        try:
            kp_cache.write_text(json.dumps({"kimchi_premium_pct": kimchi_pct, "kimchi_meta": data["kimchi_meta"]}, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass

    # ---- Funding ----
    btc_bin = binance_futures_funding_pct("BTCUSDT")
    eth_bin = binance_futures_funding_pct("ETHUSDT")

    btc_byb = None
    eth_byb = None
    if btc_bin is None:
        btc_byb = bybit_funding_pct("BTCUSDT")
    if eth_bin is None:
        eth_byb = bybit_funding_pct("ETHUSDT")

    # cache fallback
    cache_used = False
    if btc_bin is None and btc_byb is None:
        try:
            last = json.loads(fd_cache.read_text(encoding="utf-8"))
            btc_bin = last.get("BTC_binance_pct")
            btc_byb = last.get("BTC_bybit_pct")
            cache_used = True
        except Exception:
            pass
    if eth_bin is None and eth_byb is None:
        try:
            last = json.loads(fd_cache.read_text(encoding="utf-8"))
            eth_bin = last.get("ETH_binance_pct")
            eth_byb = last.get("ETH_bybit_pct")
            cache_used = True
        except Exception:
            pass

    # normalize funding structure (HTML이 funding 또는 funding_rates를 볼 수 있게)
    funding_obj = {
        "BTC": {"binance": btc_bin, "bybit": btc_byb},
        "ETH": {"binance": eth_bin, "bybit": eth_byb},
    }
    # 전부 None이면 굳이 넣지 않음
    if any(vv is not None for v in funding_obj.values() for vv in v.values()):
        data["funding"] = funding_obj
        data["funding_meta"] = {"is_cache": bool(cache_used), "ts": int(time.time())}

        try:
            fd_cache.write_text(json.dumps({
                "BTC_binance_pct": btc_bin, "BTC_bybit_pct": btc_byb,
                "ETH_binance_pct": eth_bin, "ETH_bybit_pct": eth_byb,
                "ts": int(time.time())
            }, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass

    # ---- Write back to SAME src file (no api/) ----
    atomic_write_json(src, data)
    print(f"✅ enriched in-place: {src}")
    if "kimchi_premium_pct" in data:
        print(f"   kimchi_premium_pct = {data['kimchi_premium_pct']:.4f}")
    if "funding" in data:
        b = data["funding"].get("BTC", {})
        e = data["funding"].get("ETH", {})
        print(f"   funding BTC(binance/bybit) = {b.get('binance')} / {b.get('bybit')}")
        print(f"   funding ETH(binance/bybit) = {e.get('binance')} / {e.get('bybit')}")

if __name__ == "__main__":
    main()
