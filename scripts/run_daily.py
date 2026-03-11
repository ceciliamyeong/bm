#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BM20 Index Builder
- 구성 시트(bm20_constituents)에서 coin_id/symbol/weight/국내보정 플래그를 읽음
- yfinance로 가격을 수집해 BM20 지수를 산출
- 결과를 bm20_index 시트에 append
- YF 티커는 시트에서 "동적 생성"하고, 필요한 예외만 오버라이드

Env:
  GOOGLE_SERVICE_ACCOUNT_JSON  : 서비스 계정 JSON 문자열 (전체)
  SHEET_ID                     : 구글시트 ID (https://docs.google.com/spreadsheets/d/<이 값>/...)
Optional:
  BM20_KR_BONUS                : 한국 상장 코인 보정 배수 (기본 1.3)
  BM20_BASE_VALUE              : 기준지수 (기본 100.0)
  DRY_RUN                      : "1"이면 시트에 쓰지 않고 콘솔만 출력
"""

import os, json, datetime as dt
import numpy as np
import pandas as pd
import gspread
import yfinance as yf
from typing import Dict, List
from google.oauth2.service_account import Credentials

# -------- 설정값 --------
SCOPES   = ["https://www.googleapis.com/auth/spreadsheets"]
TZ       = "Asia/Seoul"            # updated_at 표기
NY_TZ    = "America/New_York"      # 가격/시장 기준
BASE_VALUE = float(os.getenv("BM20_BASE_VALUE", "100.0"))
KR_BONUS   = float(os.getenv("BM20_KR_BONUS", "1.3"))
DRY_RUN    = os.getenv("DRY_RUN", "0") == "1"

# coin_id별 YF 예외 오버라이드 (없으면 SYMBOL-USD 규칙 사용)
YF_OVERRIDES: Dict[str, str] = {
    "binancecoin": "BNB-USD",
    "avalanche-2": "AVAX-USD",
    "internet-computer": "ICP-USD",
    "cosmos": "ATOM-USD",
    "uniswap": "UNI-USD",
    "ethereum-classic": "ETC-USD",
    "toncoin": "TON-USD",
    "tether": "USDT-USD",
    "sui": "SUI-USD",
    # 필요 시 추가
}

# -------- 유틸 --------
def nyt_today() -> dt.date:
    """뉴욕 현지 날짜(미국장 종가 기준)."""
    return pd.Timestamp.now(tz=NY_TZ).date()

def authorize():
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]), scopes=SCOPES
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(os.environ["SHEET_ID"])
    return sh

def ensure_ws(sh, title: str, header: List[str]):
    try:
        ws = sh.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=5000, cols=max(len(header), 8))
        ws.append_row(header, value_input_option="USER_ENTERED")
    return ws

# -------- 시트 로딩/가공 --------
def load_constituents(ws_cons) -> pd.DataFrame:
    """
    기대 컬럼: month, coin_id, symbol, weight [, listed_in_kr3, kr_bonus_applied, is_stable, notes]
    - month: YYYY-MM-01 등 → 월(period)로 변환
    - KR 보정: listed_in_kr3 또는 kr_bonus_applied가 참이면 KR_BONUS 배수 적용
    - 월내 정규화: 보정 후 합 1.0
    반환 컬럼: month (Period[M]), coin_id, symbol, weight, norm_weight, kr_bonus_applied(bool)
    """
    vals = ws_cons.get_all_values()
    if len(vals) < 2:
        raise RuntimeError("bm20_constituents 비어 있음")

    df = pd.DataFrame(vals[1:], columns=[c.strip().lower() for c in vals[0]])
    # 필수 체크
    need = ["month", "coin_id", "symbol", "weight"]
    for c in need:
        if c not in df.columns:
            raise RuntimeError(f"컬럼 '{c}' 없음 (bm20_constituents)")

    df["month"]  = pd.to_datetime(df["month"], errors="coerce").dt.to_period("M")
    df["weight"] = pd.to_numeric(df["weight"], errors="coerce").fillna(0.0)

    # KR 보정 플래그 (둘 중 하나라도 참이면 보정)
    listed = df.get("listed_in_kr3", False)
    bonus  = df.get("kr_bonus_applied", False)
    df["kr_bonus_applied"] = (
        pd.Series(listed).astype(str).str.lower().isin(["1","true","y","yes"]) |
        pd.Series(bonus ).astype(str).str.lower().isin(["1","true","y","yes"])
    )

    df = df.dropna(subset=["month", "coin_id", "symbol"])
    # KR 보정 후 월내 합 1.0로 정규화
    df["_eff"] = df["weight"] * df["kr_bonus_applied"].map({True: KR_BONUS, False: 1.0})
    df["_sum"] = df.groupby("month")["_eff"].transform("sum")
    # 합이 0이면 원시 weight의 개수로 나눠 균등
    zero_mask = df["_sum"].le(0)
    df.loc[zero_mask, "_sum"] = df.groupby("month")["_eff"].transform(lambda s: len(s))
    df["norm_weight"] = df["_eff"] / df["_sum"]
    df = df.drop(columns=["_eff", "_sum"])
    return df

def latest_sheet_snapshot(ws_index):
    vals = ws_index.get_all_values()
    if len(vals) < 2:
        return None, None
    df = pd.DataFrame(vals[1:], columns=[c.strip().lower() for c in vals[0]])
    if "date" not in df.columns or "index" not in df.columns:
        return None, None
    df["date"]  = pd.to_datetime(df["date"], errors="coerce").dt.date
    df["index"] = pd.to_numeric(df["index"], errors="coerce")
    df = df.dropna(subset=["date","index"]).sort_values("date")
    if df.empty:
        return None, None
    return df["date"].iloc[-1], float(df["index"].iloc[-1])

def expand_carry_forward(cons_df: pd.DataFrame, start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    """
    월별 구성(cons_df)을 [start_date, end_date] 범위로 '월단위 유지' 전개.
    - 해당 월 데이터가 없으면 직전 월의 구성을 carry.
    - 리밸런싱 주기가 월단위라고 가정(1/4/7/10월 규칙은 upstream에서 가중치만 바꾸면 자동 반영).
    """
    if cons_df is None or cons_df.empty:
        return pd.DataFrame(columns=getattr(cons_df, "columns", []))

    months = pd.period_range(pd.Period(start_date, 'M'), pd.Period(end_date, 'M'), freq='M')
    out = []
    for m in months:
        g = cons_df[cons_df["month"] == m]
        if g.empty:
            prev_all = cons_df[cons_df["month"] < m]
            if prev_all.empty:
                # 첫 달인데 구성 없음 → 스킵
                continue
            last_m = prev_all["month"].max()
            g = prev_all[prev_all["month"] == last_m].copy()
            g["month"] = m
        out.append(g)
    return pd.concat(out, ignore_index=True)

# -------- YF 매핑 --------
def build_yf_map_from_sheet(cons_df: pd.DataFrame) -> Dict[str, str]:
    """
    시트의 coin_id/symbol에서 YF 티커 동적 생성.
    기본: SYMBOL-USD, 예외: YF_OVERRIDES 적용.
    """
    base = (cons_df
            .dropna(subset=["coin_id","symbol"])
            .drop_duplicates(subset=["coin_id"], keep="last")
            .set_index("coin_id")["symbol"]
            .to_dict())
    yf_map = {cid: f"{sym.upper()}-USD" for cid, sym in base.items() if sym}
    yf_map.update({cid: t for cid, t in YF_OVERRIDES.items() if cid in base})
    missing = [cid for cid in base.keys() if cid not in yf_map]
    if missing:
        print(f"[WARN] YF ticker not mapped for coin_id: {missing}")
    return yf_map

# -------- 가격 수집 --------
def fetch_close(tickers: List[str], start: dt.date, end: dt.date) -> pd.DataFrame | None:
    """[start, end] 범위(일) 종가 수집. yfinance end는 배타적이라 내부에서 +1일."""
    raw = yf.download(
        tickers=tickers,
        start=str(start),
        end=str(end + dt.timedelta(days=1)),
        interval="1d",
        auto_adjust=True,
        progress=False,
        group_by="ticker",
    )
    close = None
    if isinstance(raw, pd.DataFrame) and not raw.empty:
        if isinstance(raw.columns, pd.MultiIndex):
            lvl1 = set(raw.columns.get_level_values(1))
            use  = "Close" if "Close" in lvl1 else ("Adj Close" if "Adj Close" in lvl1 else None)
            if use:
                close = raw.xs(use, axis=1, level=1)
        else:
            if   "Close" in raw.columns: close = raw[["Close"]]
            elif "Adj Close" in raw.columns: close = raw[["Adj Close"]]
            if close is not None:
                close.columns = tickers
    # per-ticker 폴백
    if close is None or close.shape[1] == 0:
        cols = {}
        for t in tickers:
            h = yf.download(
                tickers=t,
                start=str(start),
                end=str(end + dt.timedelta(days=1)),
                interval="1d",
                auto_adjust=True,
                progress=False,
            )
            if not h.empty:
                if "Close" in h.columns: cols[t] = h["Close"]
                elif "Adj Close" in h.columns: cols[t] = h["Adj Close"]
        if cols:
            close = pd.DataFrame(cols)
    if close is not None:
        close = close.dropna(how="all", axis=1)
    return close

# -------- 메인 --------
def main():
    sh = authorize()
    ws_cons  = ensure_ws(sh, "bm20_constituents",
                         ["month","coin_id","symbol","weight","kr_bonus_applied","listed_in_kr3","is_stable","notes"])
    ws_index = ensure_ws(sh, "bm20_index", ["date","index","flag","updated_at"])

    cons_df = load_constituents(ws_cons)
    last_date, last_index = latest_sheet_snapshot(ws_index)

    # 시작점 결정
    if last_date is None:
        firstM = cons_df["month"].min()
        start_date  = pd.to_datetime(f"{firstM.year}-{firstM.month:02d}-01").date()
        start_index = BASE_VALUE
    else:
        start_date  = last_date + dt.timedelta(days=1)
        start_index = last_index

    today_ny = nyt_today()
    if start_date > today_ny:
        print(f"[INFO] Already up-to-date (last={last_date}).")
        return

    # 월단위 가중치 전개
    cons_exp = expand_carry_forward(cons_df, start_date, today_ny)

    # 시트 기반 YF 매핑
    YF_MAP = build_yf_map_from_sheet(cons_exp)

    # 대상 코인과 티커
    coins   = sorted(cons_exp["coin_id"].unique())
    mapped  = [c for c in coins if c in YF_MAP]
    if not mapped:
        raise RuntimeError("YF_MAP 매핑된 코인이 없습니다. (coin_id/symbol/OVERRIDES 확인)")
    tickers = [YF_MAP[c] for c in mapped]

    print("[INFO] coins:", coins)
    print("[INFO] mapped:", {c: YF_MAP[c] for c in mapped})

    # 가격 수집 (전일 포함)
    price_start = start_date - dt.timedelta(days=1)
    close = fetch_close(tickers, price_start, today_ny)
    if close is None or close.shape[1] == 0:
        raise RuntimeError("가격 데이터가 비었습니다. (레이트리밋/티커 확인)")

    # 열명을 coin_id로 통일
    rev = {YF_MAP[c]: c for c in mapped}
    close = (
        close.rename(columns=rev)
             .loc[:, [c for c in mapped if c in close.columns]]
             .astype(float)
             .sort_index()
    )

    # 일자 인덱스 표준화 및 보간 → 일간 수익률
    di = pd.date_range(close.index.min(), close.index.max(), freq="D")
    close = close.reindex(di).ffill()
    rets  = close.pct_change().fillna(0.0)

    # (날짜 인덱스/가중치 인덱스 모두 naive 날짜로 정규화)
    rets.index = pd.to_datetime(rets.index).tz_localize(None).normalize()

    # 일자별 가중치 테이블 만들기 (월단위 고정 가중치)
    weights = pd.DataFrame(0.0, index=rets.index, columns=rets.columns)
    months_idx = weights.index.to_period("M")
    for m, g in cons_exp.groupby("month"):
        w = (g.set_index("coin_id")["norm_weight"]
               .reindex(weights.columns).fillna(0.0).values)
        mask = (months_idx == m)
        if mask.any():
            weights.loc[mask, :] = w

    # 월별 합 0 방지 (방어적 정규화)
    rs = weights.sum(axis=1)
    nz = rs != 0
    weights.loc[nz] = weights.loc[nz].div(rs[nz], axis=0)

    # 계산 대상 날짜(뉴욕 기준)
    today_d  = pd.Timestamp.now(tz=NY_TZ).date()
    start_d  = pd.to_datetime(start_date).date()
    target_idx = pd.date_range(start_d, min(today_d, rets.index.max().date()), freq="D")

    available = rets.index.intersection(weights.index)
    calc_idx = target_idx.intersection(available)
    if len(calc_idx) == 0:
        last_common = min(rets.index.max(), weights.index.max())
        calc_idx = pd.DatetimeIndex([last_common])
        print(f"⚠️ No overlap on target days. Using latest available: {last_common.date()}")

    # 포트폴리오 수익률 & 지수 누적
    port_ret     = (rets.loc[calc_idx] * weights.loc[calc_idx]).sum(axis=1)
    index_series = (1.0 + port_ret).cumprod() * start_index

    # 결과 쓰기
    now_iso = pd.Timestamp.now(tz=TZ).isoformat()
    market_date_note = lambda d: f"Market date (NYT): {d.date()}"
    rows = [[d.strftime("%Y-%m-%d"), float(v), market_date_note(d), now_iso] for d, v in index_series.items()]
    if not rows:
        print("[INFO] No new rows to append.")
        return

    if DRY_RUN:
        print(f"[DRY_RUN] Would append {len(rows)} rows: {rows[0][0]} → {rows[-1][0]}")
        return

    ws_index.append_rows(rows, value_input_option="USER_ENTERED")
    print(f"[OK] Appended {len(rows)} rows: {rows[0][0]} → {rows[-1][0]}")

if __name__ == "__main__":
    main()
