# scripts/make_bm20_vs_alt_chart.py
# - out/history/coin_prices_usd.csv (Yahoo 종가 패널)로부터
#   BM20(고정 가중치) vs ALT(동일가중, BTC/USDT 제외) 지수(리베이스 100) 생성 후
#   assets/bm20_vs_alt.png 저장

from __future__ import annotations

import os
import pandas as pd
import matplotlib.pyplot as plt

IN_CSV = "out/history/coin_prices_usd.csv"
OUT_PNG = "assets/bm20_vs_alt.png"

# BM20 Methodology 고정 가중치 (네가 저장해둔 룰 그대로)
# BTC 30%, ETH 20%, XRP/BNB/USDT 5%씩, 나머지 15개 균등 35%
BM20_WEIGHTS = {
    "BTC": 0.30,
    "ETH": 0.20,
    "XRP": 0.05,
    "USDT": 0.05,
    "BNB": 0.05,
    # 나머지 15개는 아래에서 자동 계산해서 채움
}

BM20_OTHERS = [
    "SOL","TON","SUI","DOGE","DOT",
    "LINK","AVAX","NEAR","ICP","ATOM",
    "LTC","OP","ARB","MATIC","ADA",
]

def rebased_index_from_returns(ret: pd.Series, base: float = 100.0) -> pd.Series:
    ret = ret.fillna(0.0)
    idx = (1.0 + ret).cumprod() * base
    return idx

def main():
    if not os.path.exists(IN_CSV):
        raise FileNotFoundError(f"Missing input CSV: {IN_CSV}")

    df = pd.read_csv(IN_CSV)
    if "date" not in df.columns:
        raise ValueError("coin_prices_usd.csv must have 'date' column")

    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date").sort_index()

    # 숫자 변환
    for c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # 결측치 약간 보정(야후는 종종 중간 NA가 있음)
    df = df.ffill()

    # ---------- BM20 구성 ----------
    # 나머지 15개 균등
    others_w = 0.35 / len(BM20_OTHERS)
    w = dict(BM20_WEIGHTS)
    for t in BM20_OTHERS:
        w[t] = others_w

    # 가격이 없는 종목은 제외(가중치 재정규화)
    available = [t for t in w.keys() if t in df.columns and df[t].notna().any()]
    if not available:
        raise RuntimeError("No BM20 constituents available in coin price panel.")

    w_avail = {t: w[t] for t in available}
    w_sum = sum(w_avail.values())
    w_avail = {t: v / w_sum for t, v in w_avail.items()}  # 재정규화

    bm20_ret = None
    for t, wt in w_avail.items():
        r = df[t].pct_change()
        bm20_ret = (wt * r) if bm20_ret is None else (bm20_ret + wt * r)

    bm20_idx = rebased_index_from_returns(bm20_ret, base=100.0)
    bm20_idx.name = "BM20"

    # ---------- ALT 지수 ----------
    # ALT는 "BTC 제외" + "USDT 제외(스테이블 영향 제거)" + 가능한 것만
    alt_universe = [c for c in df.columns if c not in ("BTC", "USDT")]
    alt_universe = [c for c in alt_universe if df[c].notna().any()]

    if not alt_universe:
        raise RuntimeError("No ALT constituents available (non-BTC, non-USDT).")

    # 동일가중
    alt_rets = df[alt_universe].pct_change()
    alt_ret = alt_rets.mean(axis=1, skipna=True)
    alt_idx = rebased_index_from_returns(alt_ret, base=100.0)
    alt_idx.name = "ALT"

    # 두 지수 공통 구간만
    out = pd.concat([bm20_idx, alt_idx], axis=1).dropna()

    if out.empty or len(out) < 30:
        raise RuntimeError("Not enough overlapping data to plot BM20 vs ALT.")

    # ---------- Plot ----------
    os.makedirs(os.path.dirname(OUT_PNG), exist_ok=True)

    plt.figure(figsize=(10.5, 5.5))
    plt.plot(out.index, out["BM20"], label="BM20")
    plt.plot(out.index, out["ALT"], label="ALT")
    plt.title("BM20 vs Alt (Rebased to 100)")
    plt.xlabel("")
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
