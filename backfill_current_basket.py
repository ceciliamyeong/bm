#!/usr/bin/env python3
import os, argparse, pandas as pd, numpy as np

STABLE = {"USDT","USDC","DAI","FDUSD","TUSD","USDE","USDP","USDL","USDS"}
DERIV  = {"WBTC","WETH","WBETH","WEETH","STETH","WSTETH","RETH","CBETH","RENBTC","HBTC","TBTC"}

def read_latest_archive(archive_dir: str) -> pd.DataFrame:
    import glob, os
    files = sorted(glob.glob(os.path.join(archive_dir, "*", "bm20_daily_data_*.csv")))
    if not files:
        raise FileNotFoundError(f"No daily CSV found under {archive_dir}")
    df = pd.read_csv(files[-1])
    if "symbol" not in df.columns:
        raise ValueError("archive CSV must contain a 'symbol' column")
    df["symbol"] = df["symbol"].astype(str).str.upper()
    return df

def read_map(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame(columns=["symbol","yf_ticker","listed_kr_override","include","cap_override"])
    m = pd.read_csv(path)
    for k in ["symbol","yf_ticker","listed_kr_override","include","cap_override"]:
        if k not in m.columns: m[k] = np.nan
    m["symbol"] = m["symbol"].astype(str).str.upper()
    return m

def build_weights(df: pd.DataFrame, mapping: pd.DataFrame, btc_cap=0.30, eth_cap=0.20, listed_bonus=1.3):
    # base weights
    if "weight_ratio" in df.columns and df["weight_ratio"].notna().any():
        w = pd.to_numeric(df["weight_ratio"], errors="coerce").fillna(0.0)
    elif "market_cap" in df.columns and df["market_cap"].notna().any():
        w = pd.to_numeric(df["market_cap"], errors="coerce").fillna(0.0)
    else:
        w = pd.Series(1.0, index=df.index)
    w = pd.Series(w.values, index=df["symbol"])
    w = w / w.sum() if w.sum() > 0 else w

    # include override
    if "include" in mapping.columns:
        inc = mapping.set_index("symbol")["include"].astype(str).str.lower().isin(["1","true","y","yes"])
        w = w[w.index.map(lambda s: inc.get(s, True))]

    # listed bonus (override only; keep it simple for backfill)
    if "listed_kr_override" in mapping.columns:
        listed = mapping.set_index("symbol")["listed_kr_override"].astype(str).str.lower().isin(["1","true","y","yes"])
        bonus = pd.Series(1.0, index=w.index)
        bonus.loc[bonus.index.map(lambda s: listed.get(s, False))] = listed_bonus
        w = w * bonus
        w = w / w.sum()

    # caps
    caps = {"BTC": btc_cap, "ETH": eth_cap}
    if "cap_override" in mapping.columns:
        for sym, v in mapping[["symbol","cap_override"]].dropna().values:
            try:
                caps[str(sym).upper()] = float(v)
            except:
                pass

    # exclude stable/derivative
    w.loc[w.index.isin(STABLE | DERIV)] = 0.0

    # iterative cap reassign
    for _ in range(10):
        over = {s: float(w[s] - caps[s]) for s in caps if s in w.index and w[s] > caps[s]}
        if not over: break
        excess = sum(over.values())
        for s in over: w[s] = caps[s]
        others = [s for s in w.index if s not in caps or w[s] <= caps[s] + 1e-15]
        pool = float(w.loc[others].sum())
        if pool <= 0: break
        w.loc[others] = w.loc[others] + (w.loc[others] / pool) * excess
    w = w / w.sum()
    return w

def download_prices(tickers: dict, start: str) -> pd.DataFrame:
    import yfinance as yf
    data = yf.download(list(tickers.values()), start=start, interval="1d",
                       auto_adjust=True, progress=False, group_by="ticker")
    closes = {}
    for sym, tkr in tickers.items():
        try:
            s = data[tkr]["Close"].dropna().sort_index()
            if not s.empty:
                closes[sym] = s
        except Exception:
            continue
    if not closes:
        raise RuntimeError("No prices downloaded")
    return pd.DataFrame(closes)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--archive", default="archive")
    ap.add_argument("--map", default="bm20_map_btc30.csv")
    ap.add_argument("--start", default="2018-01-01")
    ap.add_argument("--out", default="out/backfill_current_basket.csv")
    ap.add_argument("--base-value", type=float, default=100.0)
    ap.add_argument("--btc-cap", type=float, default=0.30)
    ap.add_argument("--eth-cap", type=float, default=0.20)
    ap.add_argument("--listed-bonus", type=float, default=1.3)
    args = ap.parse_args()

    latest = read_latest_archive(args.archive)
    mapping = read_map(args.map)

    weights = build_weights(latest, mapping, args.btc_cap, args.eth_cap, args.listed_bonus)
    # map to yfinance tickers
    m = mapping.set_index("symbol")["yf_ticker"].to_dict() if "yf_ticker" in mapping.columns else {}
    tickers = {s: (m.get(s) or f"{s}-USD") for s in weights.index}

    prices = download_prices(tickers, args.start).sort_index()
    rets = prices.pct_change().fillna(0.0)

    # fixed basket backfill
    w = weights.reindex(rets.columns).fillna(0.0)
    w = w / w.sum() if w.sum() > 0 else w
    port = (rets * w).sum(axis=1)

    idx = pd.Series(index=port.index, dtype="float64")
    level = args.base_value
    for i, (d, r) in enumerate(port.items()):
        if i == 0:
            idx.loc[d] = args.base_value
        else:
            level *= (1.0 + float(r))
            idx.loc[d] = level

    out = pd.DataFrame({"date": idx.index.date, "index": idx.values, "ret": port.values})
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    out.to_csv(args.out, index=False, encoding="utf-8")
    print(f"[OK] backfill written → {args.out} rows={len(out)} symbols={len(w)}")

if __name__ == "__main__":
    main()
