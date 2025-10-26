# scripts/upbit_publish_json.py
# 업비트에서 Top5 거래대금 스냅샷 + 최근 N일 Top5 집중도 시계열을 만들어
# /assets/top5/ 아래 JSON으로 저장합니다.
import os, time, json, requests, pandas as pd
from datetime import datetime, timezone, timedelta

UPBIT_TICKER_URL = "https://api.upbit.com/v1/ticker"
UPBIT_DAY_CANDLES_URL = "https://api.upbit.com/v1/candles/days"
MARKETS = ["KRW-BTC","KRW-ETH","KRW-XRP","KRW-SOL","KRW-SUI","KRW-DOGE","KRW-ADA","KRW-TON","KRW-BCH","KRW-LTC","KRW-AVAX","KRW-TRX"]
LOOKBACK = 30
OUTDIR = "assets/top5"

def fetch(url, params):
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    return r.json()

def now_kst_iso():
    kst = timezone(timedelta(hours=9))
    return datetime.now(kst).isoformat(timespec="seconds")

def main():
    os.makedirs(OUTDIR, exist_ok=True)

    # 1) 오늘의 Top5 스냅샷(JSON)
    ticker = fetch(UPBIT_TICKER_URL, {"markets": ",".join(MARKETS)})
    df = pd.DataFrame(ticker)[["market","acc_trade_price_24h","trade_price"]]
    df = df.sort_values("acc_trade_price_24h", ascending=False)
    top5 = df.head(5).copy()
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
        "universe": [m for m in MARKETS]
    }
    with open(os.path.join(OUTDIR, "top5_snapshot.json"), "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False)

    # 2) 최근 N일 Top5 집중도 시계열(JSON)
    frames = []
    for m in MARKETS:
        try:
            js = fetch(UPBIT_DAY_CANDLES_URL, {"market": m, "count": LOOKBACK})
            time.sleep(0.12)  # 예의상 휴식
            dfi = pd.DataFrame(js)[["candle_date_time_kst","candle_acc_trade_price"]].copy()
            dfi["date"] = pd.to_datetime(dfi["candle_date_time_kst"]).dt.date
            dfi["market"] = m
            frames.append(dfi[["date","market","candle_acc_trade_price"]])
        except Exception:
            continue
    if not frames:
        raise SystemExit("no data")

    all_df = pd.concat(frames, ignore_index=True)
    pivot = all_df.pivot_table(index="date", columns="market", values="candle_acc_trade_price", aggfunc="sum").sort_index()

    series = []
    for d, row in pivot.iterrows():
        vals = row.dropna().sort_values(ascending=False)
        if vals.empty: 
            continue
        top5_share = float(vals.head(5).sum() / vals.sum() * 100.0) if vals.sum() > 0 else None
        series.append({"date": d.isoformat(), "top5_share_pct": round(top5_share, 2)})

    timeseries = {"as_of_kst": now_kst_iso(), "lookback_days": LOOKBACK, "series": series}
    with open(os.path.join(OUTDIR, "top5_concentration.json"), "w", encoding="utf-8") as f:
        json.dump(timeseries, f, ensure_ascii=False)

if __name__ == "__main__":
    main()
