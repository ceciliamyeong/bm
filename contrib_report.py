# contrib_report.py
# 최근/MTD/QTD/YTD 구간별 기여 Top10을 bm/api/contrib_top.json으로 출력
# 그리고 1D 성과를 bm/api/perf_up.json / bm/api/perf_down.json 으로 분리 저장

import os, json, argparse, numpy as np, pandas as pd, yfinance as yf
from pathlib import Path

def to_bool(x): return str(x).strip().lower() in {"1","true","t","y","yes"}

STABLE = {"USDT","USDC","DAI","FDUSD","TUSD","USDE","USDP","USDL","USDS"}
DERIV  = {"WBTC","WETH","WBETH","WEETH","STETH","WSTETH","RETH","CBETH","RENBTC","HBTC","TBTC"}
EXC    = {"LEO","WBT"}

ap = argparse.ArgumentParser()
ap.add_argument("--map", default="bm20_map_btc30.csv")
ap.add_argument("--listed-bonus", type=float, default=1.3)
ap.add_argument("--btc-cap", type=float, default=0.30)
ap.add_argument("--eth-cap", type=float, default=0.20)
ap.add_argument("--start", default="2022-01-01")
ap.add_argument("--out-dir", default="bm/api")                     # 표준
ap.add_argument("--out-json", default=None, help="(deprecated)")   # 호환
args = ap.parse_args()

# --out-json(파일 경로)을 받으면 그 디렉터리를 out-dir로 사용 (사용자가 out-dir을 따로 안 준 경우에만)
if args.out_json and args.out_dir == "bm/api":
    compat_dir = os.path.dirname(args.out_json) or "."
    args.out_dir = compat_dir

out_dir = Path(args.out_dir)
out_dir.mkdir(parents=True, exist_ok=True)

# ---------- 데이터 준비 ----------
m = pd.read_csv(args.map)
m["symbol"] = m["symbol"].astype(str).str.upper()
inc = m.set_index("symbol")["include"].map(to_bool) if "include" in m else pd.Series(dtype=bool)
syms = [s for s in m["symbol"].unique() if inc.get(s, True) and s not in (STABLE | DERIV | EXC)]

tick_map = m.set_index("symbol")["yf_ticker"] if "yf_ticker" in m else pd.Series(dtype=str)
tick = {s: (tick_map.get(s) or f"{s}-USD") for s in syms}

px = yf.download(list(tick.values()), start=args.start,
                 progress=False, auto_adjust=True, group_by="ticker")

cl = {}
for s, t in tick.items():
    try:
        ser = px[t]["Close"].dropna()
        ser.index = pd.to_datetime(ser.index).tz_localize(None)
        if not ser.empty:
            cl[s] = ser
    except Exception:
        # 개별 티커 실패는 스킵
        pass

prices = pd.DataFrame(cl).sort_index()

# ---------- 가중치 ----------
w = pd.Series(1.0, index=prices.columns, dtype=float)
if "listed_kr_override" in m.columns:
    lk = m.set_index("symbol")["listed_kr_override"].map(to_bool)
    bonus = pd.Series(1.0, index=w.index)
    bonus.loc[bonus.index.map(lambda s: lk.get(s, False))] = args.listed_bonus
    w = w * bonus

# 실제 존재 심볼로 제한 + 정규화
w = w.loc[prices.columns] if not prices.empty else w
w = w / w.sum() if w.sum() > 0 else w

# BTC/ETH cap
caps = {"BTC": args.btc_cap, "ETH": args.eth_cap}
for _ in range(12):
    over = {s: (w[s] - caps[s]) for s in caps if s in w and w[s] > caps[s]}
    if not over:
        break
    exc = sum(over.values())
    for s in over:
        w[s] = caps[s]
    others = [s for s in w.index if s not in caps or w[s] <= caps.get(s, 1) + 1e-15]
    pool = float(w.loc[others].sum()) if len(others) else 0.0
    if pool > 0:
        w.loc[others] += w.loc[others] / pool * exc
    w = w / w.sum() if w.sum() > 0 else w

# ---------- 기여도 계산 ----------
def contrib_sum(idx: pd.DatetimeIndex) -> dict:
    if prices.empty or len(idx) == 0:
        return {}
    # 구간 내 일일 수익률
    sub = prices.loc[idx]
    rets = sub.pct_change(fill_method=None)
    c = {}
    for _, row in rets.iterrows():
        avail = row.dropna()
        if avail.empty:
            continue
        ww = w.loc[avail.index]
        if ww.sum() <= 0:
            continue
        ww = ww / ww.sum()
        for s, val in avail.items():
            c[s] = c.get(s, 0.0) + float(ww[s]) * float(val)
    # 상위 10개 (기여도가 음수일 수 있음)
    return dict(sorted(c.items(), key=lambda x: -x[1])[:10])

last = prices.index.max() if not prices.empty else pd.NaT
if pd.isna(last):
    out = {"asof": None, "MTD": {}, "QTD": {}, "YTD": {}}
else:
    mt0 = pd.Timestamp(last.year, last.month, 1)
    qt0 = pd.Timestamp(last.year, ((last.month - 1)//3)*3 + 1, 1)
    yt0 = pd.Timestamp(last.year, 1, 1)
    out = {
        "asof": str(last.date()),
        "MTD": contrib_sum(prices.index[prices.index >= mt0]),
        "QTD": contrib_sum(prices.index[prices.index >= qt0]),
        "YTD": contrib_sum(prices.index[prices.index >= yt0]),
    }

# ---------- 저장: contrib_top ----------
(out_dir / "contrib_top.json").write_text(
    json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8"
)
print(f"[OK] contrib → {out_dir/'contrib_top.json'}")

# ---------- 저장: perf_up / perf_down (1D) ----------
perf_up, perf_down = [], []
try:
    if not prices.empty and len(prices.index) >= 2:
        last2 = prices.tail(2)
        ret1d = (last2.iloc[-1] / last2.iloc[-2] - 1.0) * 100.0
        ret1d = ret1d.dropna().astype(float).sort_values(ascending=False)

        def mk(items):
            return [{"symbol": sym, "ret_24h_pct": round(float(v), 4)} for sym, v in items]

        perf_up   = mk(ret1d.head(10).items())
        perf_down = mk(ret1d.tail(10).items())
except Exception as e:
    print("[WARN] daily perf build skipped:", e)

(out_dir / "perf_up.json").write_text(
    json.dumps({"date": out.get("asof"), "top": perf_up}, ensure_ascii=False, indent=2),
    encoding="utf-8"
)
(out_dir / "perf_down.json").write_text(
    json.dumps({"date": out.get("asof"), "bottom": perf_down}, ensure_ascii=False, indent=2),
    encoding="utf-8"
)
print(f"[OK] perf_up/perf_down → {out_dir}")

