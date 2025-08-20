#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
import yfinance as yf

JSON_URL = "https://ceciliamyeong.github.io/bm/series.json"

def main():
    # BM20 불러오기
    bm20 = pd.read_json(JSON_URL)
    bm20["date"] = pd.to_datetime(bm20["date"])
    bm20.set_index("date", inplace=True)

    # BTC 불러오기
    btc = yf.download("BTC-USD", start="2018-01-01")["Close"]
    btc = btc / btc.iloc[0] * 100
    btc = btc.reindex(bm20.index).ffill()

    # 그래프
    plt.figure(figsize=(12,6))
    plt.plot(bm20.index, bm20["index"], label="BM20 Index", linewidth=2)
    plt.plot(btc.index, btc, label="BTC (normalized)", linewidth=2, linestyle="--")

    plt.yscale("log")
    plt.title("BM20 Index vs Bitcoin (2018-01-01 = 100)", fontsize=14)
    plt.xlabel("Date")
    plt.ylabel("Index (Log Scale)")
    plt.legend()
    plt.grid(True, which="both", linestyle="--", alpha=0.7)

    plt.tight_layout()
    plt.savefig("bm20_vs_btc.png")  # 결과 파일 저장

if __name__ == "__main__":
    main()
