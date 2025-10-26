# scripts/upbit_publish_json.py (robust)
import os, time, json, math, requests, pandas as pd
from datetime import datetime, timezone, timedelta

# Endpoints
UPBIT_MARKET_URL = "https://api.upbit.com/v1/market/all"
UPBIT_TICKER_URL = "https://api.upbit.com/v1/ticker"
UPBIT_DAY_CANDLES_URL = "https://api.upbit.com/v1/candles/days"

# Desired default universe (will be filtered by actual listings)
DESIRED_MARKETS = [
    "KRW-BTC","KRW-ETH","KRW-XRP","KRW-SOL","KRW-DOGE","KRW-ADA","KRW-TRX",
    "KRW-TON","KRW-BCH","KRW-LTC","KRW-AVAX","KRW-SUI"
]

LOOKBACK = 30
OUTDIR = "assets/top5"

HDRS = {
    "Accept": "application/json",
    "User-Agent": "BM20Bot/1.0 (+https://ceciliamyeong.github.io/bm)"
}

def http_get(url, params=None, timeout=12):
    try:
        r = requests.get(url, params=params, headers=HDRS, timeout=timeout)
        if r.status_code == 404:
            # Pass the response back to caller to handle gracefully
            return r
        r.raise_for_status()
        return r
    except requests.RequestException as e:
        # Raise to caller; they decide whether to continue
        raise e

def fetch_json(url, params=None):
    r = http_get(url, params=params)
    if r.status_code == 404:
        raise requests.HTTPError("404", response=r)
    return r.json()

def now_kst_iso():
    kst = timezone(timedelta(hours=9))
    return datetime.now(kst).isoformat(timespec="seconds")

def get_available_krw_markets():
    """Return set of KRW-xxx market codes actually listed on Upbit."""
    try:
        js = fetch_json(UPBIT_MARKET_URL, {"isDetails": "false"})
        mk = [d.get("market","") for d in js if isinstance(d, dict)]
        return set([m for m in mk if m.startswith("KRW-")])
    except Exception as e:
        # Fallback: assume common bluechips to avoid total failure
        print("[warn] market/all failed:", e)
        return set(["KRW-BTC","KRW-ETH","KRW-XRP","KRW-SOL","KRW-DOGE","KRW-ADA","KRW-TRX","KRW-TON","KRW-BCH","KRW-LTC"])

def chunked(iterable, n):
    it = list(iterable)
    for i in range(0, len(it), n):
        yield it[i:i+n]

def fetch_tickers_safe(markets):
    """Fetch ticker data in chunks; skip invalids causing 404."""
    rows = []
    for group in chunked(markets, 8):
        qs = ",".join(group)
        try:
            r = http_get(UPBIT_TICKER_URL, {"markets": qs})
            if r.status_code == 404:
                # Try one-by-one to isolate invalid markets
                for m in group:
                    try:
                        r1 = http_get(UPBIT_TICKER_URL, {"markets": m})
                        if r1.status_code == 404:
                            print(f"[warn] ticker 404 skip {m}")
                            continue
                        rows.extend(r1.json())
                    except Exception as e:
                        print(f"[warn] ticker fail {m}:", e)
                        continue
            else:
                rows.extend(r.json())
        except Exception as e:
            print(f"[warn] ticker group fail {group}:", e)
            continue
        time.sleep(0.1)
    return rows

def fetch_day_candles_safe(market, count=30):
    try:
        r = http_get(UPBIT_DAY_CANDLES_URL, {"market": market, "count": count})
        if r.status_code == 404:
            print(f"[warn] day-candles 404 skip {market}")
            return []
        return r.json()
    except Exception as e:
        print(f"[warn] candles fail {market}:", e)
        return []

def write_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)

def main():
    os.makedirs(OUTDIR, exist_ok=True)

    # Resolve universe against available KRW markets
    available = get_available_krw_markets()
    universe = [m for m in DESIRED_MARKETS if m in available]
    if len(universe) < 5:
        # Pad with top available ones to ensure enough breadth
        pad = [m for m in sorted(list(available)) if m not in universe]
        universe.extend(pad[: max(0, 8 - len(universe))])
    print("[info] universe:", universe)

    # 1) Snapshot (Top5 by 24h turnover)
    tickers = fetch_tickers_safe(universe)
    df = pd.DataFrame(tickers)
    if not df.empty and set(["market","acc_trade_price_24h","trade_price"]).issubset(df.columns):
        df = df[["market","acc_trade_price_24h","trade_price"]].copy()
        df = df.sort_values("acc_trade_price_24h", ascending=False)
        top5 = df.head(5).copy()
        if not top5.empty:
            top5["symbol"] = top5["market"].str.split("-").str[1]
            snapshot = {
                "as_of_kst": now_kst_iso(),
                "top5": [
                    {
                        "market": r["market"],
                        "symbol": r["symbol"],
                        "turnover_24h": float(r["acc_trade_price_24h"]),
                        "price": float(r["trade_price"])
                    } for _, r in top5.iterrows()
                ],
                "universe": universe
            }
            write_json(os.path.join(OUTDIR, "top5_snapshot.json"), snapshot)
            print("[ok] wrote top5_snapshot.json")
        else:
            print("[warn] no top5 rows from ticker")
    else:
        print("[warn] ticker df empty or missing columns")

    # 2) 30D Top5 concentration
    frames = []
    for m in universe:
        js = fetch_day_candles_safe(m, count=LOOKBACK)
        if not js:
            continue
        dfi = pd.DataFrame(js)
        if dfi.empty or "candle_date_time_kst" not in dfi or "candle_acc_trade_price" not in dfi:
            continue
        dfi = dfi[["candle_date_time_kst", "candle_acc_trade_price"]].copy()
        dfi["date"] = pd.to_datetime(dfi["candle_date_time_kst"]).dt.date
        dfi["market"] = m
        frames.append(dfi[["date","market","candle_acc_trade_price"]])
        time.sleep(0.1)
    if frames:
        all_df = pd.concat(frames, ignore_index=True)
        pivot = all_df.pivot_table(index="date", columns="market", values="candle_acc_trade_price", aggfunc="sum").sort_index()
        series = []
        for d, row in pivot.iterrows():
            vals = row.dropna().sort_values(ascending=False)
            if vals.empty:
                continue
            total = vals.sum()
            top5_sum = vals.head(5).sum()
            pct = float(top5_sum / total * 100.0) if total > 0 else None
            series.append({"date": d.isoformat(), "top5_share_pct": round(pct, 2) if pct is not None else None})
        if series:
            timeseries = {"as_of_kst": now_kst_iso(), "lookback_days": LOOKBACK, "series": series}
            write_json(os.path.join(OUTDIR, "top5_concentration.json"), timeseries)
            print("[ok] wrote top5_concentration.json")
        else:
            print("[warn] empty concentration series")
    else:
        print("[warn] no candle frames; skip concentration")

if __name__ == "__main__":
    main()
