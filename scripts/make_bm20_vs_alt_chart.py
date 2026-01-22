# scripts/make_bm20_vs_alt_chart.py
# - out/history/coin_prices_usd.csv 로부터
#   (1) BM20 지수 (고정 가중치, base 100)
#   (2) ALT/BTC 상대강도 (ALT 동일가중 지수 / BTC 가격, base 100)
#   를 계산해서 assets/bm20_vs_alt.png 저장

from __future__ import annotations

import os
import pandas as pd
import matplotlib.pyplot as plt

IN_CSV = "out/history/coin_prices_usd.csv"
OUT_PNG = "assets/bm20_vs_alt.png"

BM20_WEIGHTS = {
    "BTC": 0.30,
    "ETH": 0.20,
    "XRP": 0.05,
    "USDT": 0.05,
    "BNB": 0.05,
}

BM20_OTHERS = [
    "SOL","TON","SUI","DOGE","DOT",
    "LINK","AVAX","NEAR","ICP","ATOM",
    "LTC","OP","ARB","MATIC","ADA",
]

def rebased_from_prices(px: pd.Series, base: float = 100.0) -> pd.Series:
    px = px.dropna()
    if px.empty:
        return px
    return (px / px.iloc[0]) * base

def rebased_from_returns(ret: pd.Series, base: float = 100.0) -> pd.Series:
    ret = ret.fillna(0.0)
    return (1.0 + ret).cumprod() * base

def main():
    if not os.path.exists(IN_CSV):
        raise FileNotFoundError(f"Missing input CSV: {IN_CSV}")

    df = pd.read_csv(IN_CSV)
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date").sort_index()

    # numeric + ffill (야후 결측 완화)
    for c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.ffill()

    # BTC 필수
    if "BTC" not in df.columns or df["BTC"].dropna().empty:
        raise RuntimeError("BTC price series missing. Cannot compute ALT/BTC relative strength.")

    # ---- BM20 지수 (수익률 가중합 → base 100) ----
    others_w = 0.35 / len(BM20_OTHERS)
    w = dict(BM20_WEIGHTS)
    for t in BM20_OTHERS:
        w[t] = others_w

    available = [t for t in w.keys() if t in df.columns and df[t].notna().any()]
    if not available:
        raise RuntimeError("No BM20 constituents available in coin price panel.")

    w_avail = {t: w[t] for t in available}
    w_sum = sum(w_avail.values())
    w_avail = {t: v / w_sum for t, v in w_avail.items()}

    bm20_ret = None
    for t, wt in w_avail.items():
        r = df[t].pct_change()
        bm20_ret = (wt * r) if bm20_ret is None else (bm20_ret + wt * r)

    bm20_idx = rebased_from_returns(bm20_ret, base=100.0)
    bm20_idx.name = "BM20"

    # ---- ALT 지수 (동일가중, BTC/USDT 제외) ----
    alt_universe = [c for c in df.columns if c not in ("BTC", "USDT")]
    alt_universe = [c for c in alt_universe if df[c].notna().any()]

    if not alt_universe:
        raise RuntimeError("No ALT constituents available (non-BTC, non-USDT).")

    alt_ret = df[alt_universe].pct_change().mean(axis=1, skipna=True)
    alt_idx = rebased_from_returns(alt_ret, base=100.0)
    alt_idx.name = "ALT"

    # ---- ALT/BTC 상대강도: (ALT index / BTC price) 리베이스 100 ----
    rs_raw = (alt_idx / df["BTC"])
    alt_over_btc = rebased_from_prices(rs_raw, base=100.0)
    alt_over_btc.name = "ALT/BTC (RS)"

    # 공통 구간만
    out = pd.concat([bm20_idx, alt_over_btc], axis=1).dropna()
    if out.empty or len(out) < 30:
        raise RuntimeError("Not enough overlapping data to plot.")

    # ---- Plot ----
    os.makedirs(os.path.dirname(OUT_PNG), exist_ok=True)

    plt.figure(figsize=(10.5, 5.5))
    plt.plot(out.index, out["BM20"], label="BM20 (Base=100)")
    plt.plot(out.index, out["ALT/BTC (RS)"], label="ALT/BTC Relative Strength (Base=100)")
    plt.title("BM20 vs ALT/BTC Relative Strength")
    plt.ylabel("Index (Base=100)")
    plt.grid(True, alpha=0.25)
    plt.legend()
    plt.tight_layout()
    plt.savefig(OUT_PNG, dpi=200)
    plt.close()

    print(f"[OK] Saved chart: {OUT_PNG}")
    print(f"[INFO] BM20 constituents used: {list(w_avail.keys())}")
    print(f"[INFO] ALT constituents used: {len(alt_universe)} coins (ex BTC/USDT)")

if __name__ == "__main__":
    main()
