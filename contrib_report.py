# contrib_report.py
# 최근/MTD/QTD/YTD 구간별 기여 Top10을 site/contrib_top.json으로 출력
import json, argparse, numpy as np, pandas as pd, yfinance as yf

def to_bool(x): return str(x).strip().lower() in {"1","true","t","y","yes"}

STABLE={"USDT","USDC","DAI","FDUSD","TUSD","USDE","USDP","USDL","USDS"}
DERIV={"WBTC","WETH","WBETH","WEETH","STETH","WSTETH","RETH","CBETH","RENBTC","HBTC","TBTC"}
EXC={"LEO","WBT"}

ap = argparse.ArgumentParser()
ap.add_argument("--map", default="bm20_map_btc30.csv")
ap.add_argument("--listed-bonus", type=float, default=1.3)
ap.add_argument("--btc-cap", type=float, default=0.30)
ap.add_argument("--eth-cap", type=float, default=0.20)
ap.add_argument("--start", default="2022-01-01")
ap.add_argument("--out-json", default="site/contrib_top.json")
args = ap.parse_args()

m = pd.read_csv(args.map)
m["symbol"]=m["symbol"].astype(str).str.upper()
inc = m.set_index("symbol")["include"].map(to_bool) if "include" in m else pd.Series(dtype=bool)
syms = [s for s in m["symbol"].unique() if inc.get(s,True) and s not in STABLE|DERIV|EXC]
tick = {s: (m.set_index("symbol")["yf_ticker"].get(s) or f"{s}-USD") for s in syms}

px = yf.download(list(tick.values()), start=args.start, progress=False, auto_adjust=True, group_by="ticker")
cl = {}
for s,t in tick.items():
    try:
        ser = px[t]["Close"].dropna(); ser.index = pd.to_datetime(ser.index).tz_localize(None)
        cl[s]=ser
    except Exception:
        pass

prices = pd.DataFrame(cl).sort_index()
rets = prices.pct_change()

# 기본 가중치: 균등 → KR 보너스 → BTC/ETH cap → 정규화
w = pd.Series(1.0, index=prices.columns, dtype=float)
if "listed_kr_override" in m.columns:
    lk = m.set_index("symbol")["listed_kr_override"].map(to_bool)
    bonus = pd.Series(1.0, index=w.index)
    bonus.loc[bonus.index.map(lambda s: lk.get(s,False))] = args.listed_bonus
    w = w * bonus

caps={"BTC":args.btc_cap,"ETH":args.eth_cap}
w = w / w.sum() if w.sum()>0 else w
for _ in range(12):
    over={s:(w[s]-caps[s]) for s in caps if s in w and w[s]>caps[s]}
    if not over: break
    exc = sum(over.values())
    for s in over: w[s]=caps[s]
    others=[s for s in w.index if s not in caps or w[s]<=caps.get(s,1)+1e-15]
    pool=float(w.loc[others].sum())
    if pool>0:
        w.loc[others] += w.loc[others]/pool * exc
    w = w / w.sum()

def contrib_sum(idx: pd.DatetimeIndex):
    c={}
    for d,row in rets.loc[idx].iterrows():
        avail=row.dropna()
        if avail.empty: continue
        ww = w.loc[avail.index]; ww = ww/ww.sum()
        for s,val in avail.items():
            c[s]=c.get(s,0.0)+float(ww[s])*float(val)
    return dict(sorted(c.items(), key=lambda x:-x[1])[:10])

last=prices.index.max()
if pd.isna(last):
    out={"asof": None, "MTD":{}, "QTD":{}, "YTD":{}}
else:
    mt0 = pd.Timestamp(last.year, last.month, 1)
    qt0 = pd.Timestamp(last.year, ((last.month-1)//3)*3+1, 1)
    yt0 = pd.Timestamp(last.year, 1, 1)
    out={
        "asof": str(last.date()),
        "MTD": contrib_sum(prices.index[prices.index>=mt0]),
        "QTD": contrib_sum(prices.index[prices.index>=qt0]),
        "YTD": contrib_sum(prices.index[prices.index>=yt0]),
    }

import os
os.makedirs(os.path.dirname(args.out_json) or ".", exist_ok=True)
with open(args.out_json, "w", encoding="utf-8") as f:
    json.dump(out, f, ensure_ascii=False)
print(f"[OK] contrib → {args.out_json}")

# --- 기존 코드 마지막 줄 바로 아래 추가 ---
# 1D 퍼포먼스 Top/Down
perf_up, perf_down = [], []
try:
    # 마지막 두 날 종가 기준으로 1일 수익률 계산
    last2 = prices.tail(2)
    if len(last2) == 2:
        ret1d = (last2.iloc[-1] / last2.iloc[-2] - 1.0) * 100.0
        ret1d = ret1d.dropna().astype(float).sort_values(ascending=False)
        def _mk(items):
            return [{"symbol": sym, "ret_24h_pct": round(float(v), 4)} for sym, v in items]
        perf_up   = _mk(ret1d.head(10).items())
        perf_down = _mk(ret1d.tail(10).items())
except Exception as e:
    print("[WARN] daily perf build skipped:", e)

# perf_up.json / perf_down.json 같이 저장
out_dir = os.path.dirname(args.out_json) or "."
with open(os.path.join(out_dir, "perf_up.json"), "w", encoding="utf-8") as f:
    json.dump({"date": out.get("asof"), "top": perf_up}, f, ensure_ascii=False, indent=2)
with open(os.path.join(out_dir, "perf_down.json"), "w", encoding="utf-8") as f:
    json.dump({"date": out.get("asof"), "bottom": perf_down}, f, ensure_ascii=False, indent=2)
print(f"[OK] perf_up/perf_down → {out_dir}")
# --- 추가 끝 ---

