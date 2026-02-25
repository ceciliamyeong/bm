#!/usr/bin/env python3
import os, sys, glob, re, argparse
import pandas as pd
import numpy as np

# ✅ 스테이블 제외 목록 (USDT는 제외하지 않음, 강제 포함 대상)
STABLE = {"USDC","DAI","FDUSD","TUSD","USDE","USDP","USDL","USDS"}
EXCLUDE_DERIV = {"WBTC","WETH","WBETH","WEETH","STETH","WSTETH","RETH","CBETH","RENBTC","HBTC","TBTC"}

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--archive", default="archive")
    ap.add_argument("--out", default="out")
    ap.add_argument("--base-date", dest="base_date", default=None)
    ap.add_argument("--base-value", dest="base_value", type=float, default=100.0)
    ap.add_argument("--rebalance", choices=["daily","monthly","quarterly"], default="quarterly")
    ap.add_argument("--weights-source", choices=["csv","rules"], default="rules")
    ap.add_argument("--listed-bonus", type=float, default=1.3)
    ap.add_argument("--cap", action="append", default=[], help="e.g., --cap BTC:0.30 --cap ETH:0.20")
    ap.add_argument("--map", dest="map_path", default=None)
    ap.add_argument("--no-upbit", action="store_true")
    ap.add_argument("--dump-constituents", type=int, default=1)
    ap.add_argument("--plot", action="store_true")
    ap.add_argument("--plot-log", action="store_true")
    ap.add_argument("--ret-cap", type=float, default=None, help="winsorize daily returns at +/-RET_CAP (e.g., 0.35)")
    return ap.parse_args()

def load_daily_csvs(archive_dir):
    files = sorted(glob.glob(os.path.join(archive_dir, "*", "bm20_daily_data_*.csv")))
    out = []
    for f in files:
        m = re.search(r"(\d{4}-\d{2}-\d{2})", f)
        if not m: continue
        day = pd.to_datetime(m.group(1)).date()
        try:
            df = pd.read_csv(f)
        except Exception:
            try: df = pd.read_csv(f, encoding="utf-8-sig")
            except Exception: continue
        df["_date"] = pd.to_datetime(day)
        out.append(df)
    if not out: return pd.DataFrame()
    all_df = pd.concat(out, ignore_index=True)
    if "symbol" in all_df.columns:
        all_df["symbol"] = all_df["symbol"].astype(str).str.upper()
    return all_df

def parse_caps(cap_list):
    cap_map = {}
    for c in cap_list:
        if ":" in c:
            k,v = c.split(":",1)
            try: cap_map[k.upper()] = float(v)
            except: pass
    return cap_map

def read_map(path):
    if not path or not os.path.exists(path):
        return pd.DataFrame(columns=["symbol","yf_ticker","listed_kr_override","include","cap_override"])
    df = pd.read_csv(path)
    for k in ["symbol","yf_ticker","listed_kr_override","include","cap_override"]:
        if k not in df.columns: df[k] = np.nan
    df["symbol"] = df["symbol"].astype(str).str.upper()
    df["listed_kr_override"] = df["listed_kr_override"].astype(str).str.lower().isin(["1","true","y","yes"])
    df["include"] = df["include"].astype(str).str.lower().isin(["1","true","y","yes",""])
    with np.errstate(all="ignore"):
        df["cap_override"] = pd.to_numeric(df["cap_override"], errors="coerce")
    return df[["symbol","yf_ticker","listed_kr_override","include","cap_override"]]

def fetch_upbit_symbols():
    import requests
    url = "https://api.upbit.com/v1/market/all"
    try:
        r = requests.get(url, timeout=15); r.raise_for_status()
        syms = {m["market"].split("-")[1].upper() for m in r.json() if m.get("market","").startswith("KRW-")}
        return syms
    except Exception:
        return set()

# ✅ 상한 + 나머지 균등
def apply_caps_equalize_rest(w: pd.Series, caps: dict) -> pd.Series:
    if w.sum() <= 0 or not len(w): return w
    w = (w / w.sum()).copy()
    caps = {k.upper(): float(v) for k,v in (caps or {}).items()}
    capped_idx = [s for s in w.index if s in caps]
    uncapped_idx = [s for s in w.index if s not in caps]
    for s in capped_idx:
        w[s] = min(float(w.get(s,0.0)), caps[s])
    remaining = 1.0 - float(w.loc[capped_idx].sum()) if capped_idx else 1.0
    if remaining < 0: remaining = 0.0
    if len(uncapped_idx) > 0:
        equal = remaining / len(uncapped_idx)
        w.loc[uncapped_idx] = equal
    else:
        w = w / w.sum() if w.sum() > 0 else w
    return w / w.sum() if w.sum() > 0 else w

# ✅ Top20 (+DOGE, USDT 포함) → 상한 적용
def bm_weights_rules(day_df, listed_bonus=1.3, cap_map=None, mapping=None,
                     use_upbit=True, top_n=20, force_include=("DOGE","USDT")):
    cap_map = cap_map or {}
    df = day_df.copy()
    df["symbol"] = df["symbol"].astype(str).str.upper()
    df = df[~df["symbol"].isin(STABLE)].copy()
    df = df[~df["symbol"].isin(EXCLUDE_DERIV)].copy()
    if mapping is not None and "include" in mapping.columns:
        inc = mapping.set_index("symbol")["include"]
        df = df[df["symbol"].map(inc).fillna(True)]
    mc = pd.to_numeric(df.get("market_cap", np.nan), errors="coerce").fillna(0.0)
    df = df.loc[mc > 0].copy()
    if df.empty: return pd.Series(dtype="float64")
    listed = pd.Series(False, index=df.index, dtype="bool")
    if mapping is not None and "listed_kr_override" in mapping.columns:
        listed = listed | df["symbol"].map(mapping.set_index("symbol")["listed_kr_override"]).fillna(False).astype(bool)
    if use_upbit:
        try:
            upbit = fetch_upbit_symbols()
            listed = listed | df["symbol"].isin(upbit)
        except Exception:
            pass
    bonus = listed.map(lambda x: listed_bonus if x else 1.0).astype(float).reindex(df.index).fillna(1.0)
    df["effcap_raw"] = mc.values * bonus.values
    df = df.sort_values("effcap_raw", ascending=False)
    force_include = tuple(s.upper() for s in (force_include or ()))
    top = df.head(top_n)
    must = df[df["symbol"].isin(force_include)]
    if len(must) > 0 and not all(must["symbol"].isin(top["symbol"])):
        union = pd.concat([top, must]).drop_duplicates(subset=["symbol"])
        union = union.sort_values("effcap_raw", ascending=False).head(top_n)
        df = union
    else:
        df = top
    w0 = df.set_index("symbol")["effcap_raw"]
    w0 = w0 / w0.sum() if w0.sum() > 0 else w0
    caps = dict(cap_map or {})
    if mapping is not None and "cap_override" in mapping.columns:
        for sym, v in mapping[["symbol","cap_override"]].dropna().values:
            caps[str(sym).upper()] = float(v)
    w = apply_caps_equalize_rest(w0, caps)
    return w

def compute_returns(day_df, date, mapping=None, use_yahoo=True, ret_cap=None):
    r = None
    if "current_price" in day_df.columns and "previous_price" in day_df.columns:
        cur = pd.to_numeric(day_df["current_price"], errors="coerce")
        prev = pd.to_numeric(day_df["previous_price"], errors="coerce")
        with np.errstate(all="ignore"):
            r = (cur/prev) - 1.0
        r = r.replace([np.inf,-np.inf], np.nan)
    if r is None or r.isna().all():
        r = pd.to_numeric(day_df.get("price_change_pct", np.nan), errors="coerce")
        if r.abs().median(skipna=True) > 1.0: r = r/100.0
    if use_yahoo and r.isna().any():
        try:
            import yfinance as yf
            need = day_df.loc[r.isna(), "symbol"].astype(str).str.upper().tolist()
            if need:
                if mapping is not None and "yf_ticker" in mapping.columns:
                    m = mapping.set_index("symbol")["yf_ticker"]
                    tickers = {s: (m.get(s) or f"{s}-USD") for s in need}
                else:
                    tickers = {s: f"{s}-USD" for s in need}
                start = pd.to_datetime(date) - pd.Timedelta(days=5)
                end   = pd.to_datetime(date) + pd.Timedelta(days=1)
                data = yf.download(list(tickers.values()), start=str(start.date()), end=str(end.date()),
                                   interval="1d", auto_adjust=True, progress=False, group_by="ticker")
                for s, tkr in tickers.items():
                    try:
                        ser = data[tkr]["Close"].dropna().sort_index()
                        if len(ser) >= 2:
                            r.loc[day_df["symbol"].str.upper()==s] = float(ser.iloc[-1]/ser.iloc[-2] - 1.0)
                    except Exception: continue
        except Exception: pass
    r = r.fillna(0.0)
    if ret_cap is not None:
        r = r.clip(lower=-abs(ret_cap), upper=abs(ret_cap))
    return r

def first_trading_day(dates, freq="QE-DEC"):
    s = pd.Series(pd.to_datetime(sorted(dates)))
    periods = s.dt.to_period("Q-DEC") if freq.startswith("Q") else s.dt.to_period("M")
    firsts = {}
    for p in periods.unique():
        firsts[p] = s.loc[periods==p].iloc[0].date()
    return {d: firsts[p] for d, p in zip(s.dt.date, periods)}

def compute_index_series(all_df, base_date=None, base_value=100.0, rebalance="quarterly",
                         weights_source="rules", listed_bonus=1.3, cap_map=None, mapping=None,
                         use_upbit=True, dump_const=True, out_dir="out", ret_cap=None):
    import os
    os.makedirs(out_dir, exist_ok=True)
    all_df["_date"] = pd.to_datetime(all_df["_date"]).dt.date
    dates = sorted(all_df["_date"].unique())
    if not dates: raise RuntimeError("No daily CSVs found")
    base_date = dates[0] if base_date is None else pd.to_datetime(base_date).date()
    if rebalance == "quarterly":
        anchor = first_trading_day(dates, "QE-DEC")
    elif rebalance == "monthly":
        anchor = first_trading_day(dates, "M")
    else:
        anchor = {d:d for d in dates}
    index_level = base_value
    rows, stored = [], set()
    for d in dates:
        day_df = all_df[all_df["_date"] == d].copy()
        day_df["symbol"] = day_df["symbol"].astype(str).str.upper()
        a = anchor[d]
        base_df = all_df[all_df["_date"] == a]
        if weights_source == "csv" and "weight_ratio" in base_df.columns:
            w = pd.to_numeric(base_df["weight_ratio"], errors="coerce").fillna(0.0)
            w = w / w.sum() if w.sum()>0 else w
            w = pd.Series(w.values, index=base_df["symbol"].astype(str).str.upper())
        else:
            w = bm_weights_rules(base_df, listed_bonus=listed_bonus, cap_map=cap_map,
                                 mapping=mapping, use_upbit=use_upbit)
        r = compute_returns(day_df, d, mapping=mapping, use_yahoo=True, ret_cap=ret_cap)
        r.index = day_df["symbol"].astype(str).str.upper()
        r = r.reindex(w.index).fillna(0.0)
        daily_ret = float((w * r).sum())
        index_level = base_value if d == base_date else index_level * (1.0 + daily_ret)
        rows.append({"date":str(d), "index":round(index_level,6), "ret":round(daily_ret,8),
                     "n_constituents":int((w>0).sum())})
        if dump_const and a not in stored:
            snap = pd.DataFrame({"symbol": w.index, "weight_base": (w/w.sum()).round(12)})
            ym = pd.to_datetime(a).to_period("Q-DEC").strftime("%YQ%q") if rebalance=="quarterly" else pd.to_datetime(a).to_period("M").strftime("%Y-%m")
            snap.to_csv(os.path.join(out_dir, f"bm20_constituents_{ym}.csv"), index=False, encoding="utf-8")
            stored.add(a)
    df_out = pd.DataFrame(rows)
    df_out["index_log"] = np.log(df_out["index"].clip(lower=1e-12))
    df_out["index_log10"] = np.log10(df_out["index"].clip(lower=1e-12))
    out_csv = os.path.join(out_dir, "bm20_index_from_csv.csv")
    df_out.to_csv(out_csv, index=False, encoding="utf-8")
    return out_csv, rows[0]["date"], rows[-1]["date"], len(rows)

# === 아래부터 파일 하단 교체 ===
import os
import pandas as pd
import matplotlib.pyplot as plt

def main():
    # 1) 인자 파싱
    args = parse_args()

    # 2) 데이터 로드
    all_df = load_daily_csvs(args.archive)
    if all_df.empty:
        raise RuntimeError(f"No daily CSVs found under: {args.archive}")

    # 3) 옵션/맵 읽기
    cap_map = parse_caps(args.cap)               # --cap BTC:0.30 ...
    mapping = read_map(args.map_path)            # 심볼 매핑/오버라이드
    use_upbit = not args.no_upbit                # --no-upbit가 없으면 True

    # 4) 지수 시리즈 계산 → CSV 저장 (경로/기간/일수 반환)
    out_csv, d0, dN, n = compute_index_series(
        all_df=all_df,
        base_date=args.base_date,
        base_value=args.base_value,
        rebalance=args.rebalance,                # "daily" | "monthly" | "quarterly"
        weights_source=args.weights_source,      # "csv" | "rules"
        listed_bonus=args.listed_bonus,          # 예: 1.3
        cap_map=cap_map,
        mapping=mapping,
        use_upbit=use_upbit,
        dump_const=bool(args.dump_constituents), # 분기/월별 구성 스냅샷 CSV 저장
        out_dir=args.out,                        # 출력 폴더
        ret_cap=args.ret_cap                     # 수익률 winsorize 한계
    )

    # 5) 결과 로그
    print(f"[OK] Index series → {out_csv} ({d0} → {dN}, {n} days, "
          f"rebalance={args.rebalance}, weights={args.weights_source})")

    # 6) CSV → JSON (site/series.json)
    d = pd.read_csv(out_csv, dtype={"date": str})
    series_json = os.path.join("site", "series.json")
    os.makedirs("site", exist_ok=True)
    d.to_json(series_json, orient="records", force_ascii=False, indent=2)
    print(f"[OK] JSON saved -> {series_json}")

    # 7) (옵션) 플롯 저장
    if args.plot:
        d_plot = pd.read_csv(out_csv, parse_dates=["date"])
        fig, ax = plt.subplots(figsize=(9, 4))
        ax.plot(d_plot["date"], d_plot["index"], lw=1.2)
        if args.plot_log:
            ax.set_yscale("log")
        ax.set_title("BM20 Index" + (" (Log Scale)" if args.plot_log else ""))
        ax.grid(True, which="both", alpha=0.3)
        png_path = os.path.join(
            args.out,
            "bm20_index.png" if not args.plot_log else "bm20_index_log.png"
        )
        fig.tight_layout()
        fig.savefig(png_path, dpi=150)
        print(f"[OK] Chart saved -> {png_path}")

if __name__ == "__main__":
    main()
# === 교체 끝 ===
