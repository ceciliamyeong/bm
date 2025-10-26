# scripts/upbit_publish_json_daily_stack.py
import os, time, json, requests, pandas as pd
from datetime import datetime, timezone, timedelta

UPBIT_MARKET_URL = "https://api.upbit.com/v1/market/all"
UPBIT_DAY_CANDLES_URL = "https://api.upbit.com/v1/candles/days"
OUTDIR = "assets/top5"
LOOKBACK_DAYS = 35          # fetch a little more; we'll trim to last 30
OUTPUT_DAYS = 30
TOPN = 5

HDRS = {
    "Accept": "application/json",
    "User-Agent": "BM20Bot/1.2 (+https://ceciliamyeong.github.io/bm)"
}

DESIRED = ["KRW-BTC","KRW-ETH","KRW-XRP","KRW-SOL","KRW-SUI","KRW-DOGE","KRW-ADA","KRW-TON","KRW-BCH","KRW-LTC","KRW-AVAX","KRW-TRX"]

def now_kst_iso():
    kst = timezone(timedelta(hours=9))
    return datetime.now(kst).isoformat(timespec="seconds")

def fetch_json(url, params=None, timeout=12):
    r = requests.get(url, params=params, headers=HDRS, timeout=timeout)
    r.raise_for_status()
    return r.json()

def get_krw_markets():
    try:
        js = fetch_json(UPBIT_MARKET_URL, {"isDetails":"false"})
        mk = [d.get("market","") for d in js if isinstance(d, dict)]
        return set([m for m in mk if m.startswith("KRW-")])
    except Exception as e:
        print("[warn] market/all failed:", e)
        return set(["KRW-BTC","KRW-ETH","KRW-XRP","KRW-SOL","KRW-DOGE","KRW-ADA","KRW-TRX","KRW-TON","KRW-BCH","KRW-LTC"])

def fetch_candles(market, count=LOOKBACK_DAYS):
    try:
        js = fetch_json(UPBIT_DAY_CANDLES_URL, {"market": market, "count": count})
        return js
    except Exception as e:
        print(f"[warn] candles fail {market}:", e)
        return []

def write_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)

def main():
    os.makedirs(OUTDIR, exist_ok=True)
    available = get_krw_markets()
    universe = [m for m in DESIRED if m in available]
    if len(universe) < 6:
        extra = [m for m in sorted(list(available)) if m not in universe]
        universe.extend(extra[: max(0, 8 - len(universe))])
    print("[info] universe:", universe)

    frames = []
    for m in universe:
        js = fetch_candles(m, LOOKBACK_DAYS)
        if not js:
            continue
        import pandas as pd
        df = pd.DataFrame(js)
        if df.empty or "candle_date_time_kst" not in df or "candle_acc_trade_price" not in df:
            continue
        df = df[["candle_date_time_kst","candle_acc_trade_price"]].copy()
        df["date"] = pd.to_datetime(df["candle_date_time_kst"]).dt.date
        df["market"] = m
        frames.append(df[["date","market","candle_acc_trade_price"]])
        time.sleep(0.08)

    if not frames:
        raise SystemExit("no data fetched")

    import pandas as pd
    all_df = pd.concat(frames, ignore_index=True)
    last_dates = sorted(all_df["date"].unique())[-OUTPUT_DAYS:]
    all_df = all_df[all_df["date"].isin(last_dates)]

    pv = all_df.pivot_table(index="date", columns="market",
                            values="candle_acc_trade_price", aggfunc="sum").sort_index()

    days = []
    for d, row in pv.iterrows():
        vals = row.dropna().sort_values(ascending=False)
        total = float(vals.sum()) if len(vals) else 0.0
        if total <= 0:
            continue
        series = []
        top = vals.head(TOPN)
        for mk, v in top.items():
            sym = mk.split("-")[1]
            pct = round(float(v)/total*100.0, 2)
            series.append({"symbol": sym, "value": float(v), "pct": pct})
        others_val = float(vals.iloc[TOPN:].sum()) if len(vals) > TOPN else 0.0
        if others_val > 0:
            series.append({"symbol":"Others", "value": others_val, "pct": round(others_val/total*100.0, 2)})
        days.append({"date": d.isoformat(), "series": series, "total": total})

    out = {"as_of_kst": now_kst_iso(), "topn": TOPN, "days": days, "universe": universe}
    write_json(os.path.join(OUTDIR, "top5_stack_daily.json"), out)
    print("[ok] wrote assets/top5/top5_stack_daily.json")

if __name__ == "__main__":
    main()
