from __future__ import annotations

import os, json
from datetime import datetime, timezone
import pandas as pd

IN_CSV = "out/history/coin_prices_usd.csv"
OUT_DIR = "assets/data"

WINDOWS = [
    ("7d", 7,  25),
    ("30d", 30, 70),
    ("1y", 365, 420),
    ("2y", 730, 760),
]

def load_panel() -> pd.DataFrame:
    df = pd.read_csv(IN_CSV)
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date").sort_index()
    for c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.ffill()

def cumret_pct(ret: pd.Series) -> pd.Series:
    ret = ret.fillna(0.0)
    return ((1.0 + ret).cumprod() - 1.0) * 100.0

def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    df = load_panel()

    if "BTC" not in df.columns or df["BTC"].dropna().empty:
        raise RuntimeError("BTC series missing")

    alt_cols = [c for c in df.columns if c not in ("BTC", "USDT")]
    alt_cols = [c for c in alt_cols if df[c].notna().any()]
    if not alt_cols:
        raise RuntimeError("No ALT constituents available (non-BTC, non-USDT).")

    failures = [c for c in df.columns if c not in ("date",) and df[c].dropna().empty]

    for tag, lookback, pad in WINDOWS:
        dfw = df.tail(pad).copy()

        btc_ret = dfw["BTC"].pct_change()
        alt_ret = dfw[alt_cols].pct_change().mean(axis=1, skipna=True)

        common = btc_ret.index.intersection(alt_ret.index)
        btc_ret = btc_ret.loc[common].dropna()
        alt_ret = alt_ret.loc[common].dropna()

        btc_last = btc_ret.tail(lookback)
        alt_last = alt_ret.tail(lookback)

        btc_c = cumret_pct(btc_last)
        alt_c = cumret_pct(alt_last)

        out = []
        for d in btc_c.index:
            out.append({
                "date": d.strftime("%Y-%m-%d"),
                "btc": float(btc_c.loc[d]),
                "alt": float(alt_c.loc[d]),
            })

        with open(os.path.join(OUT_DIR, f"btc_alt_{tag}.json"), "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False)

    meta = {
        "updated_utc": datetime.now(timezone.utc).isoformat(),
        "alt_coins": len(alt_cols),
        "alt_universe": alt_cols,
        "empty_series": failures,
        "source": "Yahoo Finance (yfinance) daily close panel -> BTC vs ALT equal-weight returns",
    }
    with open(os.path.join(OUT_DIR, "meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print("[OK] wrote assets/data/*.json")

if __name__ == "__main__":
    main()
