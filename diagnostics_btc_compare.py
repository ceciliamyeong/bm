# diagnostics_btc_compare.py
# BM20(병합본)과 BTC-USD를 동일 시작=100으로 정규화, 상관/베타 산출, JSON으로 저장
import json, argparse, pandas as pd, yfinance as yf
ap = argparse.ArgumentParser()
ap.add_argument("--merged", default="out/bm20_index_merged.csv")
ap.add_argument("--out-json", default="site/diag_btc.json")
ap.add_argument("--start", default="2018-01-01")
args = ap.parse_args()
bm = pd.read_csv(args.merged, parse_dates=["date"]).set_index("date")["index"].sort_index()
bm = bm[bm.index >= pd.to_datetime(args.start)]
btc = yf.download("BTC-USD", start=bm.index.min().strftime("%Y-%m-%d"), progress=False)["Close"].dropna()
bm_n = bm / bm.iloc[0] * 100.0
btc_n = btc / btc.iloc[0] * 100.0
ret = pd.concat([bm_n.pct_change(), btc_n.pct_change()], axis=1, join="inner").dropna()
ret.columns = ["bm","btc"]
corr = float(ret.corr().loc("bm","btc")) if hasattr(ret.corr(),"loc") else float(ret.corr().loc["bm","btc"])
beta = float(ret["bm"].cov(ret["btc"]) / ret["btc"].var())
out = {
  "corr": corr, "beta": beta,
  "bm_start": str(bm_n.index[0].date()), "btc_start": str(btc_n.index[0].date()),
  "last": str(bm_n.index[-1].date())
}
# 용량 줄이려고 최근 180일만 라인 시리즈 제공
tail = 180
out["series"] = {
  "bm": [[d.strftime("%Y-%m-%d"), float(v)] for d,v in bm_n.iloc[-tail:].items()],
  "btc": [[d.strftime("%Y-%m-%d"), float(v)] for d,v in btc_n.iloc[-tail:].items()]
}
with open(args.out_json, "w", encoding="utf-8") as f: json.dump(out, f, ensure_ascii=False)
print(f"[OK] diag → {args.out_json} corr={corr:.3f} beta={beta:.2f}")
