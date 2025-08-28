#!/usr/bin/env python3
import os, json, datetime as dt
import numpy as np
import pandas as pd
import gspread, yfinance as yf
from google.oauth2.service_account import Credentials

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
TZ = "Asia/Seoul"                # 표기/updated_at용
NY_TZ = "America/New_York"       # 지수 산출(야후 데이터 정합)용
BASE_VALUE = 100.0

# ---- 20종 YF_MAP ----
YF_MAP = {
    "bitcoin":             "BTC-USD",
    "ethereum":            "ETH-USD",
    "ripple":              "XRP-USD",
    "binancecoin":         "BNB-USD",
    "solana":              "SOL-USD",
    "cardano":             "ADA-USD",
    "dogecoin":            "DOGE-USD",
    "tron":                "TRX-USD",
    "avalanche-2":         "AVAX-USD",
    "polkadot":            "DOT-USD",
    "chainlink":           "LINK-USD",
    "bitcoin-cash":        "BCH-USD",
    "litecoin":            "LTC-USD",
    "stellar":             "XLM-USD",
    "internet-computer":   "ICP-USD",
    "near":                "NEAR-USD",
    "aptos":               "APT-USD",
    "cosmos":              "ATOM-USD",
    "uniswap":             "UNI-USD",
    "ethereum-classic":    "ETC-USD",
}

def nyt_today():
    """뉴욕 현지 날짜(미국장 종가 기준)."""
    return pd.Timestamp.now(tz=NY_TZ).date()

def authorize():
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]),
        scopes=SCOPES
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(os.environ["SHEET_ID"])
    return sh

def ensure_ws(sh, title, header):
    try:
        ws = sh.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=5000, cols=len(header))
        ws.append_row(header, value_input_option="USER_ENTERED")
    return ws

def load_constituents(ws_cons):
    vals = ws_cons.get_all_values()
    if len(vals) < 2:
        raise RuntimeError("bm20_constituents 비어 있음")
    df = pd.DataFrame(vals[1:], columns=[c.strip() for c in vals[0]])
    need = ["month","coin_id","symbol","weight"]
    for c in need:
        if c not in df.columns:
            raise RuntimeError(f"컬럼 '{c}' 없음")
    df["month"]  = pd.to_datetime(df["month"], errors="coerce").dt.to_period("M")
    df["weight"] = pd.to_numeric(df["weight"], errors="coerce").fillna(0.0)
    if "kr_bonus_applied" not in df.columns:
        df["kr_bonus_applied"] = False
    df["kr_bonus_applied"] = df["kr_bonus_applied"].astype(str).str.lower().isin(["1","true","y","yes"])
    df = df.dropna(subset=["month","coin_id"])
    # KR 1.3x 보너스 후 월내 정규화
    df["w_eff"] = df["weight"] * df["kr_bonus_applied"].map({True:1.3, False:1.0})
    sum_by_m = df.groupby("month")["w_eff"].transform(lambda s: s.sum() if s.sum() > 0 else len(s))
    df["norm_weight"] = df["w_eff"] / sum_by_m
    df = df.drop(columns=["w_eff"])
    return df

def latest_sheet_snapshot(ws_index):
    vals = ws_index.get_all_values()
    if len(vals) < 2:
        return None, None
    df = pd.DataFrame(vals[1:], columns=[c.strip().lower() for c in vals[0]])
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df["index"] = pd.to_numeric(df["index"], errors="coerce")
    df = df.dropna(subset=["date","index"]).sort_values("date")
    if df.empty:
        return None, None
    return df["date"].iloc[-1], float(df["index"].iloc[-1])

def expand_carry_forward(cons_df: pd.DataFrame, start_date, end_date) -> pd.DataFrame:
    # 1) 입력 가드
    if cons_df is None or cons_df.empty:
        print("[WARN] cons_df is empty in expand_carry_forward; returning empty df")
        return pd.DataFrame(columns=getattr(cons_df, "columns", []))

    out = []

    # ... 기존 로직 삽입 위치(사용 중인 버전 유지) ...

    # 2) 출력 가드
    if not out:
        print("[WARN] No rows generated in expand_carry_forward; returning input unchanged")
        return cons_df.copy()

    return pd.concat(out, ignore_index=True)

def fetch_close(tickers, start, end):
    """[start, end] 범위(일 단위) 종가 수집. yfinance end는 배타적이라 내부에서 +1일."""
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
            if close is not None: close.columns = tickers
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

def main():
    sh = authorize()
    ws_cons  = ensure_ws(sh, "bm20_constituents", ["month","coin_id","symbol","weight","kr_bonus_applied","listed_in_kr3","is_stable","notes"])
    ws_index = ensure_ws(sh, "bm20_index",       ["date","index","flag","updated_at"])

    cons_df = load_constituents(ws_cons)
    last_date, last_index = latest_sheet_snapshot(ws_index)

    if last_date is None:
        firstM = cons_df["month"].min()
        start_date = pd.to_datetime(f"{firstM.year}-{firstM.month:02d}-01").date()
        start_index = BASE_VALUE
    else:
        start_date  = last_date + dt.timedelta(days=1)
        start_index = last_index

    # 지수 산출 기준일: 뉴욕 현지 날짜(야후와 정합)
    today_ny = nyt_today()
    if start_date > today_ny:
        print(f"[INFO] Already up-to-date (last={last_date}).")
        return

    # 구성 종목 가중치 전개 (start_date ~ today_ny)
    cons_exp = expand_carry_forward(cons_df, start_date, today_ny)

    # 티커 매핑
    coins   = sorted(cons_exp["coin_id"].unique())
    mapped  = [c for c in coins if c in YF_MAP]
    if not mapped:
        raise RuntimeError("YF_MAP 매핑된 코인이 없습니다. (YF_MAP 업데이트 필요)")
    tickers = [YF_MAP[c] for c in mapped]

    # 가격 수집
    price_start = start_date - dt.timedelta(days=1)
    close = fetch_close(tickers, price_start, today_ny)
    if close is None or close.shape[1] == 0:
        raise RuntimeError("가격 데이터가 비었습니다. (레이트리밋/티커 확인)")

    # 컬럼명을 coin_id로 통일
    rev = {YF_MAP[c]: c for c in mapped}
    close = (
        close.rename(columns=rev)
             .loc[:, [c for c in mapped if c in close.columns]]
             .astype(float)
             .sort_index()
    )

    # 일자 리샘플/전일 보간 → 일간 수익률
    di = pd.date_range(close.index.min(), close.index.max(), freq="D")
    close = close.reindex(di).ffill()
    rets  = close.pct_change().fillna(0.0)

    # 일자별 가중치 매핑
    weights = pd.DataFrame(0.0, index=rets.index, columns=rets.columns)
    months_idx = weights.index.to_period("M")
    for m, g in cons_exp.groupby("month"):
        w = g.set_index("coin_id")["norm_weight"].reindex(weights.columns).fillna(0.0).values
        mask = (months_idx == m)
        if mask.any():
            weights.loc[mask, :] = w

    # 혹시 월별 합이 0인 구간 정규화
    rs = weights.sum(axis=1)
    bad = rs != 0
    weights.loc[bad] = weights.loc[bad].div(rs[bad], axis=0)

    # --- 날짜 준비 (tz 충돌 방지: naive date로 통일, NY 기준) ---
    today_d  = pd.Timestamp.now(tz=NY_TZ).date()
    start_d  = pd.to_datetime(start_date).date()

    # 인덱스 정규화(타임존/시간 제거 → 날짜 기준)
    rets = rets.copy(); weights = weights.copy()
    rets.index = pd.to_datetime(rets.index).tz_localize(None).normalize()
    weights.index = pd.to_datetime(weights.index).tz_localize(None).normalize()

    # 계산 대상 달력 날짜(뉴욕 기준)
    target_idx = pd.date_range(start_d, min(today_d, rets.index.max().date()), freq="D")

    # 실제 공통으로 존재하는 날짜만 선택
    available = rets.index.intersection(weights.index)
    calc_idx = target_idx.intersection(available)

    # 공통 구간이 비면(예: 오늘 데이터 미반영) → 최신 공통일로 대체
    if len(calc_idx) == 0:
        last_common = min(rets.index.max(), weights.index.max())
        calc_idx = pd.DatetimeIndex([last_common])
        print(f"⚠️ No overlap on target days. Using latest available: {last_common.date()}")

    # --- 포트폴리오 수익률 및 지수 누적 ---
    port_ret     = (rets.loc[calc_idx] * weights.loc[calc_idx]).sum(axis=1)
    index_series = (1.0 + port_ret).cumprod() * start_index

    # --- 시트 Append ---
    now_iso = pd.Timestamp.now(tz=TZ).isoformat()
    market_date_note = f"Market date (NYT): {calc_idx[-1].date() if len(calc_idx)>0 else today_d}"
    rows = [[d.strftime("%Y-%m-%d"), float(v), market_date_note, now_iso] for d, v in index_series.items()]
    if not rows:
        print("[INFO] No new rows to append.")
        return

    ws_index.append_rows(rows, value_input_option="USER_ENTERED")
    print(f"[OK] Appended {len(rows)} rows: {rows[0][0]} → {rows[-1][0]}")

if __name__ == "__main__":
    main()

