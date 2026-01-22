# scripts/make_bm20_vs_alt_chart.py
# - 같은 기간 BTC vs ALT 누적수익률(%) 비교
# - ALT = 동일가중, BTC/USDT 제외
# - output: assets/bm20_vs_alt.png

from __future__ import annotations

import os
import pandas as pd
import matplotlib.pyplot as plt

IN_CSV = "out/history/coin_prices_usd.csv"
OUT_PNG = "assets/bm20_vs_alt.png"

def main():
    if not os.path.exists(IN_CSV):
        raise FileNotFoundError(f"Missing input CSV: {IN_CSV}")

    df = pd.read_csv(IN_CSV)
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date").sort_index()

    # numeric + ffill
    for c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.ffill()

    # ---------- BTC 누적수익률 ----------
    if "BTC" not in df.columns or df["BTC"].dropna().empty:
        raise RuntimeError("BTC series missing")

    btc_ret = df["BTC"].pct_change().fillna(0.0)
    btc_cum = (1 + btc_ret).cumprod() - 1
    btc_cum_pct = btc_cum * 100
    btc_cum_pct.name = "BTC"

    # ---------- ALT 누적수익률 ----------
    alt_cols = [c for c in df.columns if c not in ("BTC", "USDT")]
    alt_cols = [c for c in alt_cols if df[c].notna().any()]

    if not alt_cols:
        raise RuntimeError("No ALT constituents available")

    alt_ret = df[alt_cols].pct_change().mean(axis=1, skipna=True).fillna(0.0)
    alt_cum = (1 + alt_ret).cumprod() - 1
    alt_cum_pct = alt_cum * 100
    alt_cum_pct.name = "ALT"

    # ---------- Merge ----------
    out = pd.concat([btc_cum_pct, alt_cum_pct], axis=1).dropna()
    if len(out) < 30:
        raise RuntimeError("Not enough data to plot")

    # ---------- Plot ----------
    os.makedirs(os.path.dirname(OUT_PNG), exist_ok=True)

    plt.figure(figsize=(10.5, 5.5))
    plt.plot(out.index, out["BTC"], label="BTC cumulative return (%)")
    plt.plot(out.index, out["ALT"], label="ALT cumulative return (%)")
    plt.axhline(0, linewidth=0.8, alpha=0.4)

    plt.title("BTC vs ALT cumulative returns (same period)")
    plt.ylabel("Cumulative return (%)")
    plt.grid(True, alpha=0.25)
    plt.legend()
    plt.tight_layout()
    plt.savefig(OUT_PNG, dpi=200)
    plt.close()

    print(f"[OK] Saved chart: {OUT_PNG}")
    print(f"[INFO] ALT constituents: {len(alt_cols)} coins (equal-weight)")

if __name__ == "__main__":
    main()

