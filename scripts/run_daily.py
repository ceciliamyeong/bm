#!/usr/bin/env python3
import os, json, datetime as dt
import numpy as np
import pandas as pd
import gspread, yfinance as yf
from google.oauth2.service_account import Credentials

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
TZ = "Asia/Seoul"
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

def kst_today():
    return pd.Timestamp.now(tz=TZ).date()

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
        if c not in df.columns: raise RuntimeError(f"컬럼 '{c}' 없음")
    df["month"]  = pd.to_datetime(df["month"], errors="coerce").dt.to_period("M")
    df["weight"] = pd.to_numeric(df["weight"], errors="coerce").fillna(0.0)
    if "kr_bonus_applied" not in df.columns: df["kr_bonus_applied"] = False
    df["kr_bonus_applied"] = df["kr_bonus_applied"].astype(str)\
                              .str.lower().isin(["1","true","y","yes"])
    df = df.dropna(subset=["month","coin_id"])
    # KR 1.3x 보너스 후 월내 정규화(Deprecation 없는 버전)
    df["w_eff"] = df["weight"] * df["kr_bonus_applied"].map({True:1.3, False:1.0})
    sum_by_m = df.groupby("month")["w_eff"].transform(lambda s: s.sum() if s.sum() > 0 else len(s))
    df["norm_weight"] = df["w_eff"] / sum_by_m
    df = df.drop(columns=["w_eff"])
    return df

def latest_sheet_snapshot(ws_index):
    vals = ws_index.get_all_values()
    if len(vals) < 2: return None, None
    df = pd.DataFrame(vals[1:], columns=[c.strip().lower() for c in vals[0]])
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df["index"] = pd.to_numeric(df["index"], errors="coerce")
    df = df.dropna(subset=["date","index"]).sort_values("date")
    if df.empty: return None, None
    return df["date"].iloc[-1], float(df["index"].iloc[-1])

def expand_carry_forward(cons_df: pd.DataFrame, start_date, end_date) -> pd.DataFrame:
    # 1) 입력 가드
    if cons_df is None or cons_df.empty:
        print("[WARN] cons_df is empty in expand_carry_forward; returning empty df")
        return pd.DataFrame(columns=getattr(cons_df, "columns", []))

    out = []

    # ... 여기에 기존 날짜 루프/로직 있음 ...

    # 2) 출력 가드
    if not out:
        print("[WARN] No rows generated in expand_carry_forward; returning input unchanged")
        return cons_df.copy()

    return pd.concat(out, ignore_index=True)


def fetch_close(tickers, start, end):
    raw = yf.download(tickers=tickers, start=str(start), end=str(end + dt.timedelta(days=1)),
                      interval="1d", auto_adjust=True, progress=False, group_by="ticker")
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
            h = yf.download(tickers=t, start=str(start), end=str(end + dt.timedelta(days=1)),
                            interval="1d", auto_adjust=True, progress=False)
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

    today = kst_today()
    if start_date > today:
        print(f"[INFO] Already up-to-date (last={last_date}).")
        return

    cons_exp = expand_carry_forward(cons_df, start_date, today)

    coins   = sorted(cons_exp["coin_id"].unique())
    mapped  = [c for c in coins if c in YF_MAP]
    if not mapped:
        raise RuntimeError("YF_MAP 매핑된 코인이 없습니다. (YF_MAP 업데이트 필요)")
    tickers = [YF_MAP[c] for c in mapped]

    price_start = start_date - dt.timedelta(days=1)
    close = fetch_close(tickers, price_start, today)
    if close is None or close.shape[1] == 0:
        raise RuntimeError("가격 데이터가 비었습니다. (레이트리밋/티커 확인)")

    rev = {YF_MAP[c]: c for c in mapped}
    close = close.rename(columns=rev).loc[:, [c for c in mapped if c in close.columns]].astype(float).sort_index()

    di = pd.date_range(close.index.min(), close.index.max(), freq="D")
    close = close.reindex(di).ffill()
    rets  = close.pct_change().fillna(0.0)

    weights = pd.DataFrame(0.0, index=rets.index, columns=rets.columns)
    months_idx = weights.index.to_period("M")
    for m, g in cons_exp.groupby("month"):
        w = g.set_index("coin_id")["norm_weight"].reindex(weights.columns).fillna(0.0).values
        mask = (months_idx == m)
        if mask.any(): weights.loc[mask, :] = w

    rs = weights.sum(axis=1)
    bad = rs != 0
    weights.loc[bad] = weights.loc[bad].div(rs[bad], axis=0)

    calc_idx = pd.date_range(start_date, today, freq="D")
    port_ret = (rets.loc[calc_idx] * weights.loc[calc_idx]).sum(axis=1)
    index_series = (1.0 + port_ret).cumprod() * start_index

    now_iso = pd.Timestamp.now(tz=TZ).isoformat()
    rows = [[d.strftime("%Y-%m-%d"), float(v), "live", now_iso] for d, v in index_series.items()]
    if not rows:
        print("[INFO] No new rows to append."); return

    ws_index.append_rows(rows, value_input_option="USER_ENTERED")
    print(f"[OK] Appended {len(rows)} rows: {rows[0][0]} → {rows[-1][0]}")

if __name__ == "__main__":
    main()
