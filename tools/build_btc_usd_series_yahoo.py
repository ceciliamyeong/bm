import os
import json
from pathlib import Path

import pandas as pd
import yfinance as yf

OUT = Path("btc_usd_series.json")

def _extract_close_series(df: pd.DataFrame) -> pd.Series:
    """Return a 1D Series of close prices from yfinance download output.

    yfinance can return:
      - columns: Open/High/Low/Close/... (single ticker)
      - MultiIndex columns: (PriceField, Ticker) or (Ticker, PriceField) depending on options/version
    """
    if df is None or df.empty:
        return pd.Series(dtype=float)

    # If MultiIndex, try to slice out 'Close' regardless of where it is.
    if isinstance(df.columns, pd.MultiIndex):
        cols0 = set(df.columns.get_level_values(0))
        cols1 = set(df.columns.get_level_values(1))
        if "Close" in cols0:
            s = df.xs("Close", axis=1, level=0)
            # s may be a DataFrame with one column (ticker)
            if isinstance(s, pd.DataFrame):
                return s.iloc[:, 0]
            return s
        if "Close" in cols1:
            s = df.xs("Close", axis=1, level=1)
            if isinstance(s, pd.DataFrame):
                return s.iloc[:, 0]
            return s

        # Fallback: flatten and search
        flat = df.copy()
        flat.columns = ["_".join([str(x) for x in c if x is not None]).strip("_") for c in flat.columns.to_list()]
        for k in ("Close", "close", "CLOSE", "Adj Close", "Adj_Close", "AdjClose"):
            if k in flat.columns:
                return flat[k]
        for c in flat.columns:
            if c.lower().endswith("close") or "close" in c.lower():
                return flat[c]

        return pd.Series(dtype=float)

    # Non-multiindex
    for k in ("Close", "close", "CLOSE", "Adj Close", "Adj_Close", "AdjClose"):
        if k in df.columns:
            return df[k]

    # Sometimes yfinance returns a Series-like DF; try first numeric column
    num_cols = df.select_dtypes(include="number").columns.tolist()
    if num_cols:
        return df[num_cols[0]]

    return pd.Series(dtype=float)

def main():
    start = os.getenv("START_DATE", "2018-01-01").strip() or "2018-01-01"

    # Download daily BTC-USD OHLCV from Yahoo Finance via yfinance
    # NOTE: yfinance output column structure can vary; handle robustly.
    df = yf.download(
        "BTC-USD",
        start=start,
        interval="1d",
        auto_adjust=False,
        progress=False,
        group_by="column",  # tends to keep single-level columns for 1 ticker
        threads=False,
    )

    close = _extract_close_series(df).dropna()
    if close.empty:
        # Print diagnostics for debugging in Actions logs
        print("[btc] No Close series extracted.")
        try:
            print("[btc] df.columns:", df.columns)
            print("[btc] df.head():\n", df.head())
        except Exception:
            pass
        raise SystemExit("No data returned (or unexpected columns) for BTC-USD")

    series = [{"date": idx.strftime("%Y-%m-%d"), "price": float(v)} for idx, v in close.items()]
    series.sort(key=lambda x: x["date"])

    OUT.write_text(json.dumps(series, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[btc] wrote {OUT} ({len(series)} points) from {series[0]['date']} to {series[-1]['date']}")

if __name__ == "__main__":
    main()
