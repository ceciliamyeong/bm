#!/usr/bin/env python3
import os, sys, glob, re, argparse
import pandas as pd
import numpy as np

STABLE = {"USDT","USDC","DAI","FDUSD","TUSD","USDE","USDP","USDL","USDS"}
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

def apply_caps(weights: pd.Series, caps: dict):
    w = weights.copy()
    w = w / w.sum() if w.sum()>0 else w
    if not caps: return w
    for _ in range(10):
        over = {s: float(w.get(s,0) - c) for s,c in caps.items() if s in w.index and w[s] > c}
        if not over: break
        excess = sum(over.values())
        for s in over: w[s] = caps[s]
        others = [s for s in w.index if s not in caps or w[s] < caps.get(s,1.0)+1e-15]
        pool = float(pd.Series(w.loc[others]).sum())
        if pool <= 0: break
        w.loc[others] = w.loc[others] + (w.loc[others] / pool) * excess
    return w / w.sum() if w.sum()>0 else w

def bm_weights_rules(day_df, listed_bonus=1.3, cap_map=None, mapping=None, use_upbit=True):
    cap_map = cap_map or {}
    df = day_df.copy()
    df["symbol"] = df["symbol"].astype(str).str.upper()

    # exclude stables + derivatives
    df = df[~df["symbol"].isin(STABLE)].copy()
    df = df[~df["symbol"].isin(EXCLUDE_DERIV)].copy()
    if mapping is not None and "include" in mapping.columns:
        inc = mapping.set_index("symbol")["include"]
        df = df[df["symbol"].map(inc).fillna(True)]

    mc = pd.to_numeric(df.get("market_cap", np.nan), errors="coerce").fillna(0.0)
    if mc.sum() > 0:
        df = df.loc[mc>0].copy()

    if df.empty:
        return pd.Series(dtype="float64")

    # base weights: market cap preferred, fallback to weight_ratio if present
    if "market_cap" in df.columns and df["market_cap"].notna().any():
        base_w = pd.to_numeric(df["market_cap"], errors="coerce").fillna(0.0)
        base_w = base_w / base_w.sum() if base_w.sum()>0 else base_w
    elif "weight_ratio" in df.columns:
        base_w = pd.to_numeric(df["weight_ratio"], errors="coerce").fillna(0.0)
        base_w = base_w / base_w.sum() if base_w.sum()>0 else base_w
    else:
        base_w = pd.Series(1.0, index=df.index); base_w = base_w / base_w.sum()

    # KR listed overrides + Upbit auto
    listed = pd.Series(False, index=df.index, dtype="bool")
    if mapping is not None and "listed_kr_override" in mapping.columns:
        listed = listed | df["symbol"].map(mapping.set_index("symbol")["listed_kr_override"]).fillna(False).astype(bool)
    if use_upbit:
        try:
            upbit = fetch_upbit_symbols()
            listed = listed | df["symbol"].isin(upbit)
        except Exception:
            pass

    w_eff = base_w * listed.map(lambda x: listed_bonus if x else 1.0)
    w_eff = w_eff / w_eff.sum() if w_eff.sum()>0 else w_eff

    # caps
    caps = dict(cap_map or {})
    if mapping is not None and "cap_override" in mapping.columns:
        for sym, v in mapping[["symbol","cap_override"]].dropna().values:
            caps[str(sym).upper()] = float(v)

    w = pd.Series(w_eff.values, index=df["symbol"])
    w = apply_caps(w, caps)
    return w

def compute_returns(day_df, date, mapping=None, use_yahoo=True):
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
                    except Exception:
                        continue
        except Exception:
            pass
    return r.fillna(0.0)

def first_trading_day(dates, freq="QE-DEC"):
    s = pd.Series(pd.to_datetime(sorted(dates)))
    periods = s.dt.to_period("Q-DEC") if freq.startswith("Q") else s.dt.to_period("M")
    firsts = {}
    for p in periods.unique():
        firsts[p] = s.loc[periods==p].iloc[0].date()
    return {d: firsts[p] for d, p in zip(s.dt.date, periods)}

def compute_index_series(all_df, base_date=None, base_value=100.0, rebalance="quarterly",
                         weights_source="rules", listed_bonus=1.3, cap_map=None, mapping=None,
                         use_upbit=True, dump_const=True, out_dir="out"):
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
    rows = []
    stored = set()
    for d in dates:
        day_df = all_df[all_df["_date"] == d].copy()
        day_df["symbol"] = day_df["symbol"].astype(str).str.upper()

        if weights_source == "csv" and "weight_ratio" in day_df.columns:
            w = pd.to_numeric(day_df["weight_ratio"], errors="coerce").fillna(0.0)
            w = w / w.sum() if w.sum()>0 else w
            w = pd.Series(w.values, index=day_df["symbol"])
            w.loc[w.index.isin(STABLE | EXCLUDE_DERIV)] = 0.0
            if mapping is not None and "include" in mapping.columns:
                inc = mapping.set_index("symbol")["include"]
                mask = day_df["symbol"].map(inc).fillna(True).to_numpy()
                w = pd.Series(np.where(mask, w, 0.0), index=w.index)
            w = w / w.sum() if w.sum()>0 else w
        else:
            w = bm_weights_rules(day_df, listed_bonus=listed_bonus, cap_map=cap_map, mapping=mapping, use_upbit=use_upbit)

        a = anchor[d]
        if d != a:
            base_df = all_df[all_df["_date"] == a]
            if weights_source == "csv" and "weight_ratio" in base_df.columns:
                w = pd.to_numeric(base_df["weight_ratio"], errors="coerce").fillna(0.0)
                w = w / w.sum() if w.sum()>0 else w
                w = pd.Series(w.values, index=base_df["symbol"].astype(str).str.upper())
                w.loc[w.index.isin(STABLE | EXCLUDE_DERIV)] = 0.0
                if mapping is not None and "include" in mapping.columns:
                    inc = mapping.set_index("symbol")["include"]
                    mask = base_df["symbol"].map(inc).fillna(True).to_numpy()
                    w = pd.Series(np.where(mask, w, 0.0), index=w.index)
                w = w / w.sum() if w.sum()>0 else w
            else:
                w = bm_weights_rules(base_df, listed_bonus=listed_bonus, cap_map=cap_map, mapping=mapping, use_upbit=use_upbit)

        r = compute_returns(day_df, d, mapping=mapping, use_yahoo=True)
        r.index = day_df["symbol"].astype(str).str.upper()
        r = r.reindex(w.index).fillna(0.0)

        daily_ret = float((w * r).sum())
        index_level = base_value if d == base_date else index_level * (1.0 + daily_ret)
        rows.append({"date":str(d), "index":round(index_level,6), "ret":round(daily_ret,8), "n_constituents":int((w>0).sum())})

        if dump_const:
            key = a
            if key not in stored:
                snap = pd.DataFrame({"symbol": w.index, "weight_base": (w/w.sum()).round(12)})
                ym = pd.to_datetime(a).to_period("Q-DEC").strftime("%YQ%q") if rebalance=="quarterly" else pd.to_datetime(a).to_period("M").strftime("%Y-%m")
                snap_path = os.path.join(out_dir, f"bm20_constituents_{ym}.csv")
                snap.to_csv(snap_path, index=False, encoding="utf-8")
                stored.add(key)

    out_csv = os.path.join(out_dir, "bm20_index_from_csv.csv")
    pd.DataFrame(rows).to_csv(out_csv, index=False, encoding="utf-8")
    return out_csv, rows[0]["date"], rows[-1]["date"], len(rows)

def main():
    args = parse_args()
    all_df = load_daily_csvs(args.archive)
    if all_df.empty:
        print("[ERR] No CSV found in", args.archive); sys.exit(1)
    cap_map = parse_caps(args.cap)
    mapping = read_map(args.map_path) if args.map_path else read_map(None)
    out_csv, d0, dN, n = compute_index_series(
        all_df,
        base_date=args.base_date,
        base_value=args.base_value,
        rebalance=args.rebalance,
        weights_source=args.weights_source,
        listed_bonus=args.listed_bonus,
        cap_map=cap_map,
        mapping=mapping,
        use_upbit=not args.no_upbit,
        dump_const=bool(args.dump_constituents),
        out_dir=args.out
    )
    print(f"[OK] Index series → {out_csv} ({d0} → {dN}, {n} days, rebalance={args.rebalance}, weights={args.weights_source})")

if __name__ == "__main__":
    main()
