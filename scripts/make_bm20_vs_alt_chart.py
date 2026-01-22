# scripts/make_bm20_vs_alt_chart.py
# - out/history/bm20_index_history.csv + out/history/coin_prices_usd.csv → 듀얼축 차트 저장
import os
import pandas as pd
import matplotlib.pyplot as plt

BM20_CSV = "out/history/bm20_index_history.csv"    # date, level
PRICES_CSV = "out/history/coin_prices_usd.csv"     # date, BTC, ETH, ...
OUT_PNG = "assets/bm20_vs_alt.png"                 # Pages에서 고정 참조하기 좋음

def main():
    bm20 = pd.read_csv(BM20_CSV)
    bm20["date"] = pd.to_datetime(bm20["date"])
    bm20 = bm20.sort_values("date").set_index("date").rename(columns={"level": "BM20"})

    px = pd.read_csv(PRICES_CSV)
    px["date"] = pd.to_datetime(px["date"])
    px = px.sort_values("date").set_index("date")

    btc = px["BTC"]

    # Alt-RS: BTC 제외, ETH 포함, 스테이블 제외(USDT/USDC/DAI)
    stable = [c for c in px.columns if c.upper() in ["USDT", "USDC", "DAI"]]
    alts = [c for c in px.columns if c != "BTC" and c not in stable]

    rel = px[alts].div(btc, axis=0)
    base = rel.dropna().iloc[0]
    alt_rs = rel.div(base).mean(axis=1) * 100
    alt_rs.name = "ALT_RS"

    df = pd.concat([bm20["BM20"], alt_rs], axis=1).dropna()

    os.makedirs(os.path.dirname(OUT_PNG), exist_ok=True)

    fig = plt.figure(figsize=(11, 5.5))
    ax1 = plt.gca()
    ax2 = ax1.twinx()

    ax1.plot(df.index, df["BM20"], linewidth=2, label="BM20")
    ax2.plot(df.index, df["ALT_RS"], linewidth=2, label="ALT-RS (BTC-relative)")

    ax1.set_title("BM20 vs Alt Relative Strength (ALT-RS)")
    ax1.set_xlabel("Date")
    ax1.set_ylabel("BM20 (Absolute Index)")
    ax2.set_ylabel("ALT-RS (Rebased to 100)")

    l1, lab1 = ax1.get_legend_handles_labels()
    l2, lab2 = ax2.get_legend_handles_labels()
    ax1.legend(l1 + l2, lab1 + lab2, loc="upper left", frameon=False)

    ax1.grid(True, linewidth=0.6, alpha=0.5)
    plt.tight_layout()
    plt.savefig(OUT_PNG, dpi=200)
    print(f"Saved: {OUT_PNG}")

if __name__ == "__main__":
    main()
