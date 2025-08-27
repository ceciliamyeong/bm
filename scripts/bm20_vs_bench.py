# scripts/bm20_vs_bench.py
import pandas as pd, matplotlib.pyplot as plt
from pathlib import Path
from datetime import date
from scripts.util_price import load_bm20_index_history, load_btc_close, load_eth_close

BASE_DATE = "2024-01-01"
BASE_VALUE = 100.0
OUTD = Path(f"out/{date.today().isoformat()}"); OUTL = Path("out/latest")
OUTD.mkdir(parents=True, exist_ok=True); OUTL.mkdir(parents=True, exist_ok=True)

# --- robust base picker & normalizer (한 번만 정의) ---
def _pick_base_value(s: pd.Series, base_date: str) -> float:
    ts = pd.to_datetime(base_date)
    s = s.dropna().copy()
    s.index = pd.to_datetime(s.index)
    exact = s.loc[s.index == ts]
    if not exact.empty:
        return float(exact.iloc[0])
    aft = s[s.index > ts]
    if not aft.empty:
        return float(aft.iloc[0])
    bef = s[s.index < ts]
    if not bef.empty:
        return float(bef.iloc[-1])
    raise ValueError("Series has no data to pick a base value")

def norm_to_base(s: pd.Series, base_date: str, base_value: float = 100.0) -> pd.Series:
    s = s.copy()
    s.index = pd.to_datetime(s.index)
    base_val = _pick_base_value(s, base_date)
    return (s / base_val) * base_value

def main():
    bm20 = load_bm20_index_history()               # ['date','bm20']
    btc  = load_btc_close(start_date="2017-01-01") # ['date','close']
    eth  = load_eth_close(start_date="2017-01-01")

    # 머지
    df = (bm20.rename(columns={"bm20":"BM20"})
              .merge(btc.rename(columns={"close":"BTC"}), on="date", how="inner")
              .merge(eth.rename(columns={"close":"ETH"}), on="date", how="inner"))
    df["date"] = pd.to_datetime(df["date"])
    df = df[df["date"] >= pd.to_datetime(BASE_DATE)].copy()
    df.set_index("date", inplace=True)

    # ✅ 정규화 (반드시 먼저)
    df["BM20_rel"] = norm_to_base(df["BM20"], BASE_DATE, BASE_VALUE)
    df["BTC_rel"]  = norm_to_base(df["BTC"],  BASE_DATE, BASE_VALUE)
    df["ETH_rel"]  = norm_to_base(df["ETH"],  BASE_DATE, BASE_VALUE)

    # 상대성과(비율) 지수 (100=동일 성과)
    df["BM20_over_BTC"] = (df["BM20_rel"] / df["BTC_rel"]) * 100.0
    df["BM20_over_ETH"] = (df["BM20_rel"] / df["ETH_rel"]) * 100.0

    # 저장
    out_csv_d = OUTD / f"bm20_vs_bench_{date.today().isoformat()}.csv"
    out_csv_l = OUTL / "bm20_vs_bench.csv"
    df[["BM20_rel","BTC_rel","ETH_rel","BM20_over_BTC","BM20_over_ETH"]].to_csv(out_csv_d)
    df[["BM20_rel","BTC_rel","ETH_rel","BM20_over_BTC","BM20_over_ETH"]].to_csv(out_csv_l)

    # 차트 1: BM20 vs BTC vs ETH
    plt.figure()
    df[["BM20_rel","BTC_rel","ETH_rel"]].plot()
    plt.axhline(100, linestyle="--", linewidth=1)
    plt.title("BM20 vs BTC vs ETH (Base=2024-01-01 → 100)")
    plt.ylabel("Index (Base=100)"); plt.xlabel("")
    plt.tight_layout()
    plt.savefig(OUTD / f"bm20_btc_eth_line_{date.today().isoformat()}.png", dpi=180)
    plt.savefig(OUTL / "bm20_btc_eth_line.png", dpi=180)
    plt.close()

    # 차트 2: BM20/BTC
    plt.figure()
    df["BM20_over_BTC"].plot()
    plt.axhline(100, linestyle="--", linewidth=1)
    plt.title("Relative Performance: BM20 / BTC (100 = equal)")
    plt.ylabel("Index"); plt.xlabel("")
    plt.tight_layout()
    plt.savefig(OUTD / f"bm20_over_btc_{date.today().isoformat()}.png", dpi=180)
    plt.savefig(OUTL / "bm20_over_btc.png", dpi=180)
    plt.close()

    # 차트 3: BM20/ETH
    plt.figure()
    df["BM20_over_ETH"].plot()
    plt.axhline(100, linestyle="--", linewidth=1)
    plt.title("Relative Performance: BM20 / ETH (100 = equal)")
    plt.ylabel("Index"); plt.xlabel("")
    plt.tight_layout()
    plt.savefig(OUTD / f"bm20_over_eth_{date.today().isoformat()}.png", dpi=180)
    plt.savefig(OUTL / "bm20_over_eth.png", dpi=180)
    plt.close()

if __name__ == "__main__":
    main()
