# diagnostics_btc_compare.py
# BM20 vs BTC-USD: 시작=100 정규화, 상관/베타 계산, 최근 180일 시리즈 JSON 출력 (robust)
import os, json, argparse
import pandas as pd
import yfinance as yf

ap = argparse.ArgumentParser()
ap.add_argument("--merged", default="out/bm20_index_merged.csv")
ap.add_argument("--out-json", default="site/diag_btc.json")
ap.add_argument("--start", default="2018-01-01")
ap.add_argument("--tail-days", type=int, default=180)
args = ap.parse_args()

def pack_series(s: pd.Series, tail: int):
    s = s.iloc[-tail:]
    return [[d.strftime("%Y-%m-%d"), float(v)] for d, v in s.items()]

# 1) BM20 병합본
bm = pd.read_csv(args.merged, parse_dates=["date"]).set_index("date")["index"].sort_index()
bm = bm[bm.index >= pd.to_datetime(args.start)]
if bm.empty:
    raise SystemExit("empty bm20 series")

# 2) BTC
btc = yf.download(
    "BTC-USD",
    start=bm.index.min().strftime("%Y-%m-%d"),
    progress=False,
    auto_adjust=True,
)["Close"].dropna()

# 3) 시작=100 정규화
bm_n  = bm  / bm.iloc[0] * 100.0
btc_n = btc / btc.iloc[0] * 100.0

# 4) 상관/베타
ret = pd.concat([bm_n.pct_change(), btc_n.pct_change()], axis=1, join="inner").dropna()
ret.columns = ["bm", "btc"]
corr = float(ret["bm"].corr(ret["btc"])) if not ret.empty else float("nan")
var_btc = float(ret["btc"].var())
beta = float(ret["bm"].cov(ret["btc"]) / var_btc) if var_btc > 0 else float("nan")

# 5) JSON 출력
out = {
    "corr": corr, "beta": beta,
    "bm_start": bm_n.index[0].strftime("%Y-%m-%d"),
    "btc_start": btc_n.index[0].strftime("%Y-%m-%d"),
    "last": bm_n.index[-1].strftime("%Y-%m-%d"),
    "series": {"bm": pack_series(bm_n, args.tail_days),
               "btc": pack_series(btc_n, args.tail_days)},
}
os.makedirs(os.path.dirname(args.out_json) or ".", exist_ok=True)
with open(args.out_json, "w", encoding="utf-8") as f:
    json.dump(out, f, ensure_ascii=False)

print(f"[OK] diag → {args.out_json} corr={corr:.3f} beta={beta:.2f}")
