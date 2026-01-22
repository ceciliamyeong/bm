# scripts/update_coin_prices_usd.py
# - Yahoo Finance (yfinance)로 BM20 코인 USD 일봉 종가 패널 생성/증분 업데이트
# - 최근 2년(730일)만 유지
# - Yahoo에서 일부 코인이 이상하게 내려오거나(스칼라/빈 DF) 데이터가 짧아도 파이프라인이 죽지 않게 설계
from __future__ import annotations

import os
import json
from datetime import datetime, timedelta, timezone
from typing import Dict, List

import pandas as pd
import yfinance as yf


OUT_DIR = "out/history"
OUT_CSV = os.path.join(OUT_DIR, "coin_prices_usd.csv")
OUT_META = os.path.join(OUT_DIR, "coin_prices_usd.meta.json")

MAX_LOOKBACK_DAYS = 730
LOOKBACK_DAYS = 3

TICKERS: List[str] = [
    "BTC","ETH","BNB","XRP","USDT",
    "SOL","TON","ADA","DOGE","DOT",
    "LINK","AVAX","NEAR","ICP","ATOM",
    "LTC","OP","ARB","MATIC","SUI",
]

# 기본 심볼: XXX-USD (안 잡히는 건 나중에 여기만 수정)
YF_TICKER: Dict[str, str] = {t: f"{t}-USD" for t in TICKERS}


def yf_download_close(symbol: str, start: str, end: str) -> pd.Series:
    """
    yfinance로 일봉 Close 수집 -> 반드시 1차원 pd.Series(date index)로 반환
    """
    df = yf.download(
        symbol,
        start=start,
        end=end,
        interval="1d",
        progress=False,
        auto_adjust=False,
        threads=False,
    )

    if df is None or df.empty:
        raise ValueError(f"Empty yfinance data for {symbol}")

    # Close 추출 (Series/DataFrame/ndarray 어떤 형태로 와도 1D로 평탄화)
    if "Close" not in df.columns:
        raise ValueError(f"No Close column for {symbol}. cols={list(df.columns)}")

    close = df[["Close"]]  # 항상 DataFrame으로 잡고
    close = close.squeeze("columns")  # ✅ (n,1) -> (n,) Series로 변환

    # 혹시 여전히 Series가 아니면 강제 변환
    if not isinstance(close, pd.Series):
        close = pd.Series(close)

    close = close.dropna()
    if close.empty:
        raise ValueError(f"No valid Close data for {symbol}")

    close.index = pd.to_datetime(close.index).date
    return close



def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    now_utc = datetime.now(timezone.utc)
    min_date = (now_utc - timedelta(days=MAX_LOOKBACK_DAYS)).date()

    # 기존 CSV 있으면 마지막 날짜 기준 증분(lookback 포함), 없으면 최근 2년부터
    if os.path.exists(OUT_CSV):
        old = pd.read_csv(OUT_CSV)
        old["date"] = pd.to_datetime(old["date"]).dt.date
        old = old.sort_values("date")
        last_date = old["date"].max()

        start_date = (pd.to_datetime(last_date) - pd.Timedelta(days=LOOKBACK_DAYS)).date()
        if start_date < min_date:
            start_date = min_date

        df_old = old.set_index("date")
    else:
        start_date = min_date
        df_old = None

    # yfinance는 end가 배타적으로 동작하는 경우가 있어 하루 여유
    start_str = str(start_date)
    end_str = str((now_utc + timedelta(days=1)).date())

    series_map: Dict[str, pd.Series] = {}
    failures: Dict[str, str] = {}

    for t in TICKERS:
        symbol = YF_TICKER.get(t, f"{t}-USD")
        try:
            s = yf_download_close(symbol, start=start_str, end=end_str)

            # ✅ 혹시라도 스칼라가 섞이면 여기서 차단
            if not isinstance(s, pd.Series):
                raise TypeError(f"Expected Series, got {type(s)}")

            s.name = t
            series_map[t] = s
            print(f"[OK] {t} ({symbol}) rows={len(s)}")

        except Exception as e:
            failures[t] = f"{type(e).__name__}: {e}"
            print(f"[FAIL] {t} ({symbol}) -> {type(e).__name__}: {e}")

    # ✅ 여기가 핵심: DataFrame(series_map) 대신 concat으로 안정화
    if series_map:
        df_new = pd.concat(series_map.values(), axis=1)
        df_new.columns = list(series_map.keys())
        df_new.index.name = "date"
        df_new = df_new.sort_index()
    else:
        # 전부 실패해도 파일 형식은 유지 (단, 차트 단계에서 의미 없으니 여기서 명확히 터뜨려도 됨)
        raise RuntimeError("All tickers failed on Yahoo Finance. Cannot build price panel.")

    # 기존 데이터와 결합: new 우선(덮어쓰기)
    if df_old is not None:
        df_old2 = df_old.copy()
        df_old2.index = pd.to_datetime(df_old2.index).date

        df_new2 = df_new.copy()
        df_new2.index = pd.to_datetime(df_new2.index).date

        combined = pd.concat([df_old2, df_new2], axis=0)
        combined = combined[~combined.index.duplicated(keep="last")]
        out = combined.sort_index()
    else:
        out = df_new

    # 최근 2년만 유지
    out = out[out.index >= min_date]

    # 실패한 티커도 칼럼은 유지
    for t in TICKERS:
        if t not in out.columns:
            out[t] = pd.NA

    out = out[TICKERS]

    # save csv
    out2 = out.reset_index()
    out2["date"] = out2["date"].astype(str)
    out2.to_csv(OUT_CSV, index=False)

    meta = {
        "source": "Yahoo Finance (yfinance)",
        "generated_utc": now_utc.isoformat(),
        "max_lookback_days": MAX_LOOKBACK_DAYS,
        "lookback_days": LOOKBACK_DAYS,
        "start": str(out.index.min()) if len(out.index) else None,
        "end": str(out.index.max()) if len(out.index) else None,
        "tickers": TICKERS,
        "yahoo_symbols": YF_TICKER,
        "failures": failures,
    }
    with open(OUT_META, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print(f"\nSaved: {OUT_CSV}")
    print(f"Saved: {OUT_META}")
    if failures:
        print("\nFailed tickers (kept as NaN columns):")
        for k, v in failures.items():
            print(f"- {k}: {v}")


if __name__ == "__main__":
    main()
