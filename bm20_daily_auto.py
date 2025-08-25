#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BM20 daily auto:
- weights 탭에서 Top20 심볼 로드
- bm20_map의 yf_ticker 매핑 우선, 없으면 SYMBOL-USD
- yfinance로 최근 2영업일 종가 → (오늘/어제 -1) = ret_pct
- summary_perf_latest 시트: symbol, ret_pct (덮어쓰기)
- summary_perf 시트: date, symbol, ret_pct (날짜별 누적 append)
"""

import os, json, time, datetime as dt
from typing import List, Dict
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import yfinance as yf

TZ = "Asia/Seoul"

SPREADSHEET_ID = os.getenv("BM20_SHEET_ID")
if not SPREADSHEET_ID:
    raise RuntimeError("Missing env BM20_SHEET_ID")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

def authorize_gspread() -> gspread.Client:
    raw = os.getenv("GSPREAD_SA_JSON")
    if not raw:
        raise RuntimeError("Missing env GSPREAD_SA_JSON")
    creds = Credentials.from_service_account_info(json.loads(raw), scopes=SCOPES)
    return gspread.authorize(creds)

gc = authorize_gspread()
ss = gc.open_by_key(SPREADSHEET_ID)
print("OK: Spreadsheet opened ->", SPREADSHEET_ID)

def ensure_ws(title: str, rows: int=1000, cols: int=26):
    try:
        return ss.worksheet(title)
    except gspread.WorksheetNotFound:
        return ss.add_worksheet(title=title, rows=str(rows), cols=str(cols))

def write_df(ws, df: pd.DataFrame, clear=True):
    if clear: ws.clear()
    values = [df.columns.tolist()] + df.astype(object).where(pd.notnull(df), "").values.tolist()
    ws.update(values=values, range_name="A1")

def append_rows(ws, rows: List[List[object]]):
    # 빠르고 간단한 append
    ws.append_rows(rows, value_input_option="USER_ENTERED")

def _today_date_str():
    return pd.Timestamp.now(tz=TZ).strftime("%Y-%m-%d")

def _load_weights_symbols() -> List[str]:
    w = ss.worksheet("weights")
    vals = w.get_all_values()
    if len(vals) < 2:
        raise RuntimeError("weights sheet empty")
    df = pd.DataFrame(vals[1:], columns=[c.strip() for c in vals[0]])
    if "symbol" not in df.columns:
        raise RuntimeError("weights missing 'symbol'")
    # weight가 있으면 상위 20 정렬, 없으면 등장 순서대로 상위 20
    if "weight" in df.columns:
        df["weight"] = pd.to_numeric(df["weight"], errors="coerce")
        df = df.sort_values("weight", ascending=False)
    syms = df["symbol"].astype(str).str.upper().str.strip().tolist()
    return syms[:20]

def _load_map_df() -> pd.DataFrame:
    try:
        ws = ss.worksheet("bm20_map")
        vals = ws.get_all_values()
        if len(vals) < 2: return pd.DataFrame(columns=["symbol","yf_ticker"])
        df = pd.DataFrame(vals[1:], columns=[c.strip() for c in vals[0]])
        if "symbol" in df.columns:
            df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
        if "yf_ticker" in df.columns:
            df["yf_ticker"] = df["yf_ticker"].astype(str).str.strip()
        return df
    except Exception:
        return pd.DataFrame(columns=["symbol","yf_ticker"])

def _map_yf_tickers(symbols: List[str], map_df: pd.DataFrame) -> Dict[str, str]:
    m = {}
    if not map_df.empty and {"symbol","yf_ticker"}.issubset(set(map_df.columns)):
        md = map_df.dropna(subset=["symbol"]).copy()
        md["symbol"] = md["symbol"].astype(str).str.upper().str.strip()
        for _, r in md.iterrows():
            sy = str(r["symbol"]).upper()
            yt = str(r.get("yf_ticker","")).strip()
            if yt: m[sy] = yt
    for s in symbols:
        if s not in m:
            m[s] = f"{s}-USD"
    return m

def _download_2d_closes(yf_tickers: List[str]) -> pd.DataFrame:
    now = pd.Timestamp.now(tz=TZ).tz_localize(None)
    start = now - pd.Timedelta(days=10)
    end   = now + pd.Timedelta(days=1)
    data = yf.download(
        tickers=" ".join(yf_tickers),
        start=start.strftime("%Y-%m-%d"),
        end=end.strftime("%Y-%m-%d"),
        interval="1d",
        group_by="ticker",
        auto_adjust=False,
        threads=False,
        progress=False,
    )
    rows = []
    if isinstance(data.columns, pd.MultiIndex):
        for tkr in yf_tickers:
            try:
                closes = data[(tkr, "Close")].dropna().sort_index()
                rows.append(pd.DataFrame({"yf_ticker": tkr, "date": closes.index, "close": closes.values}))
            except Exception:
                continue
    else:
        # 단일 티커 케이스
        closes = data["Close"].dropna().sort_index()
        rows.append(pd.DataFrame({"yf_ticker": yf_tickers[0], "date": closes.index, "close": closes.values}))
    if not rows:
        return pd.DataFrame(columns=["yf_ticker","date","close"])
    return pd.concat(rows, ignore_index=True)

def compute_daily_returns(symbols20: List[str], map_df: pd.DataFrame) -> pd.DataFrame:
    tmap = _map_yf_tickers(symbols20, map_df)
    uniq_tickers = list(dict.fromkeys(tmap.values()))  # unique, order-preserving
    raw = _download_2d_closes(uniq_tickers)
    if raw.empty:
        return pd.DataFrame(columns=["symbol","ret_pct"])
    raw = raw.sort_values(["yf_ticker","date"])
    last2 = raw.groupby("yf_ticker").tail(2).copy()
    last2["rn"] = last2.groupby("yf_ticker").cumcount()
    piv = last2.pivot(index="yf_ticker", columns="rn", values="close").rename(columns={0:"prev",1:"cur"})
    piv = piv.dropna(subset=["prev","cur"]).reset_index()
    piv["ret_pct"] = piv["cur"] / piv["prev"] - 1.0

    # yf_ticker -> symbol 역매핑
    inv = {v:k for k,v in tmap.items()}
    piv["symbol"] = piv["yf_ticker"].map(inv)
    out = piv[["symbol","ret_pct"]].dropna()
    out["symbol"] = out["symbol"].astype(str).str.upper().str.strip()
    # Top20만 남기기(혹시 추가 포함됐을 경우)
    out = out[out["symbol"].isin(symbols20)].copy()
    return out

def main():
    today = _today_date_str()
    symbols20 = _load_weights_symbols()
    map_df = _load_map_df()
    perf = compute_daily_returns(symbols20, map_df)

    # 1) 최신 시트: summary_perf_latest (덮어쓰기)
    ws_latest = ensure_ws("summary_perf_latest")
    if perf.empty:
        write_df(ws_latest, pd.DataFrame(columns=["symbol","ret_pct"]))
    else:
        perf_latest = perf[["symbol","ret_pct"]].copy()
        perf_latest["ret_pct"] = pd.to_numeric(perf_latest["ret_pct"], errors="coerce").round(6)
        write_df(ws_latest, perf_latest)

    # 2) 누적 시트: summary_perf (append, date 포함)
    ws_hist = ensure_ws("summary_perf")
    # 헤더 없으면 생성
    vals = ws_hist.get_all_values()
    if not vals:
        ws_hist.update(values=[["date","symbol","ret_pct"]], range_name="A1")
        vals = [["date","symbol","ret_pct"]]

    if not perf.empty:
        rows = [[today, s, float(r)] for s, r in perf[["symbol","ret_pct"]].values]
        append_rows(ws_hist, rows)

    print(f"OK: {today} summary_perf_latest & summary_perf updated. rows={len(perf)}")

if __name__ == "__main__":
    main()
