# scripts/update_coin_prices_usd.py
# - Yahoo Finance (yfinance)로 BM20 코인 USD 일봉 종가 패널 생성/증분 업데이트
# - 최근 2년(730일)만 유지
# - 일부 코인이 Yahoo에서 안 잡히면: 해당 코인만 NaN으로 남기고 전체 파이프라인은 성공 처리
from __future__ import annotations

import os
import json
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import pandas as pd

# yfinance is required (install in GitHub Actions)
import yfinance as yf

OUT_DIR = "out/history"
OUT_CSV = os.path.join(OUT_DIR, "coin_prices_usd.csv")
OUT_META = os.path.join(OUT_DIR, "coin_prices_usd.meta.json")

# 최근 2년만 유지
MAX_LOOKBACK_DAYS = 730
# 증분 업데이트 시 마지막 n일은 다시 덮어쓰기 (Yahoo 수정/누락 대비)
LOOKBACK_DAYS = 3

# BM20 UNIVERSE (20)
TICKERS: List[str] = [
    "BTC","ETH","BNB","XRP","USDT",
    "SOL","TON","ADA","DOGE","DOT",
    "LINK","AVAX","NEAR","ICP","ATOM",
    "LTC","OP","ARB","MATIC","SUI",
]

# Yahoo 심볼 매핑
# 기본은 {TICKER}-USD. (안 잡히는 코인은 나중에 여기만 고치면 됨)
YF_TICKER: Dict[str, str] = {t: f"{t}-USD" for t in TICKERS}

# 예: USDT는 종종 데이터가 이상할 수 있어서, 필요하면 나중에 별도 처리
# YF_TICKER["USDT"] = "USDT-USD"

def yf_download_close(symbol: str, start: str, end: str) -> pd.Series:
    """
    yfinance로 일봉 Close 수집 -> 어떤 형태로 와도 1D Series로 강제 변환
    (MultiIndex columns 포함)
    """
    df = yf.download(
        symbol,
        start=start,
        end=end,
        interval="1d",
        progress=False,
        auto_adjust=False,
        threads=False,
        group_by="column",   # ✅ 컬럼 기준으로 묶도록 명시
    )

    if df is None or df.empty:
        raise ValueError(f"Empty yfinance data for {symbol}")

    # ✅ Close 추출 (MultiIndex 방어)
    if isinstance(df.columns, pd.MultiIndex):
        # 예: level0 = ['Open','High','Low','Close',...], level1 = ['BTC-USD']
        if "Close" not in df.columns.get_level_values(0):
            raise ValueError(f"No Close in MultiIndex columns for {symbol}")
        close_df = df.xs("Close", axis=1, level=0)   # -> DataFrame (cols: ticker)
        close = close_df.iloc[:, 0]                  # -> Series (1D)
    else:
        if "Close" not in df.columns:
            raise ValueError(f"No Close column for {symbol}. cols={list(df.columns)}")
        close = df["Close"]
        # 혹시 Close가 DataFrame으로 오면 1열만
        if isinstance(close, pd.DataFrame):
            close = close.iloc[:, 0]

    # ✅ 1D 강제 + 정리
    close = pd.to_numeric(close, errors="coerce").dropna()
    if close.empty:
        raise ValueError(f"No valid Close data for {symbol}")

    close.index = pd.to_datetime(close.index).date
    return close


def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    now_utc = datetime.now(timezone.utc)
    min_date = (now_utc - timedelta(days=MAX_LOOKBACK_DAYS)).date()

    # 기존 CSV가 있으면 마지막 날짜 기준으로 증분(lookback 포함), 없으면 최근 2년부터
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

    # yfinance download는 end가 "exclusive" 처럼 동작하는 경우가 있어 하루 여유
    start_str = str(start_date)
    end_str = str((now_utc + timedelta(days=1)).date())

    series_map: Dict[str, pd.Series] = {}
    failures: Dict[str, str] = {}

    for t in TICKERS:
        symbol = YF_TICKER.get(t)
        if not symbol:
            failures[t] = "No Yahoo symbol mapping"
            continue

        try:
            s = yf_download_close(symbol, start=start_str, end=end_str)
            series_map[t] = s
            print(f"[OK] {t} ({symbol}) rows={len(s)}")
        except Exception as e:
            # ✅ 여기서 죽지 않고 계속 진행
            failures[t] = f"{type(e).__name__}: {e}"
            print(f"[FAIL] {t} ({symbol}) -> {type(e).__name__}: {e}")

    # wide dataframe
    df_new = pd.DataFrame(series_map)
    df_new.index.name = "date"
    df_new = df_new.sort_index()

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

    # ✅ 최근 2년만 유지
    out = out[out.index >= min_date]

    # ✅ 실패한 티커도 칼럼은 유지(차트/후속 계산에서 컬럼 존재가 더 편함)
    # 컬럼이 아예 없으면 만들기
    for t in TICKERS:
        if t not in out.columns:
            out[t] = pd.NA

    # 컬럼 순서 정렬
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
        "failures": failures,  # 어떤 코인이 안 잡혔는지 기록
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
