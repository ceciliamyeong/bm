#!/usr/bin/env python3
"""
scripts/run_daily.py

- Google Sheets의 `bm20_index` 탭에 오늘자 라이브 값 한 줄을 추가합니다.
- 계산 로직:
    1) `bm20_constituents`에서 '현재 월' 구성/가중치 로드
    2) KR 1.3배 보정 → 월내 정규화(norm_weight)
    3) yfinance 일별 종가로 어제→오늘 수익률 산출
    4) 전일 지수 × (1 + 가중 평균 수익률) = 오늘 지수
- 전제 환경변수(레포 Secrets로 등록):
    * SHEET_ID
    * GOOGLE_SERVICE_ACCOUNT_JSON  (서비스계정 JSON 전문)
- 의존성: pandas numpy yfinance gspread google-auth google-auth-oauthlib google-auth-httplib2
"""
import os, json, math, datetime as dt, time
import pandas as pd, numpy as np
import yfinance as yf
import gspread
from google.oauth2.service_account import Credentials

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
HEADER = ["date", "index", "flag", "updated_at"]

# CoinGecko coin_id -> Yahoo Finance ticker 매핑 (필요시 추가)
YF_MAP = {
    "bitcoin": "BTC-USD",
    "ethereum": "ETH-USD",
    "ripple": "XRP-USD",
    "litecoin": "LTC-USD",
    "bitcoin-cash": "BCH-USD",
    "binancecoin": "BNB-USD",
    "cardano": "ADA-USD",
    "solana": "SOL-USD",
    "polkadot": "DOT-USD",
    "chainlink": "LINK-USD",
    "stellar": "XLM-USD",
    "tron": "TRX-USD",
    "dogecoin": "DOGE-USD",
}

def kst_now():
    return dt.datetime.now(dt.timezone(dt.timedelta(hours=9)))

def kst_today_date():
    return kst_now().date()

def authorize():
    sheet_id = os.environ["SHEET_ID"]
    sa_json = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
    creds = Credentials.from_service_account_info(json.loads(sa_json), scopes=SCOPES)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)
    return sh

def ensure_ws(sh):
    try:
        ws = sh.worksheet("bm20_index")
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title="bm20_index", rows=2000, cols=len(HEADER))
        ws.append_row(HEADER, value_input_option="USER_ENTERED")
    return ws

def get_last_row(ws):
    vals = ws.get_all_values()
    return vals[-1] if len(vals) >= 2 else None

def get_last_index_value(ws) -> float | None:
    vals = ws.get_values("B:B")
    try:
        return float(vals[-1][0]) if vals and vals[-1] and vals[-1][0] else None
    except Exception:
        return None

def read_constituents(sh) -> pd.DataFrame | None:
    try:
        ws = sh.worksheet("bm20_constituents")
    except gspread.exceptions.WorksheetNotFound:
        return None
    vals = ws.get_all_values()
    if len(vals) < 2:
        return None
    df = pd.DataFrame(vals[1:], columns=vals[0])
    df["month"] = pd.to_datetime(df["month"], format="%Y-%m", errors="coerce").dt.to_period("M")
    df["weight"] = pd.to_numeric(df["weight"], errors="coerce").fillna(0.0)
    if "kr_bonus_applied" in df.columns:
        df["kr_bonus_applied"] = df["kr_bonus_applied"].astype(str).str.lower().isin(["1","true","y","yes"])
    else:
        df["kr_bonus_applied"] = False

    def norm_group(g: pd.DataFrame):
        bonus = np.where(g["kr_bonus_applied"].values, 1.3, 1.0)
        w = g["weight"].values * bonus
        s = w.sum()
        if not math.isfinite(s) or s <= 0:
            n = len(g)
            w = np.array([1.0/n]*n, dtype=float)
            s = w.sum()
        return g.assign(norm_weight=(w / s))

    return df.groupby("month", group_keys=False).apply(norm_group)

def fetch_daily_return_yf(ticker: str) -> float | None:
    try:
        dfp = yf.download(ticker, period="5d", interval="1d", auto_adjust=True, progress=False)
        if dfp is None or dfp.empty or "Close" not in dfp.columns or len(dfp) < 2:
            return None
        close = dfp["Close"]
        r = float(close.iloc[-1] / close.iloc[-2] - 1.0)
        return r if math.isfinite(r) else None
    except Exception:
        return None

def calc_today_index_value(sh, ws_index) -> float | None:
    prev_idx = get_last_index_value(ws_index)
    if prev_idx is None:
        return None

    cons = read_constituents(sh)
    if cons is None or cons.empty:
        return None

    cur_month = pd.Period(kst_today_date(), freq="M")
    g = cons[cons["month"] == cur_month]
    if g.empty:
        return None

    coins = g["coin_id"].tolist()
    w = g.set_index("coin_id")["norm_weight"]

    contribs = []
    for cid in coins:
        tkr = YF_MAP.get(cid)
        if not tkr:
            print(f"[WARN] YF_MAP missing for coin_id='{cid}' → contribution=0")
            continue
        r = fetch_daily_return_yf(tkr)
        if r is None:
            print(f"[WARN] could not get daily return for {tkr} → contribution=0")
            continue
        wi = float(w.get(cid, 0.0) or 0.0)
        contribs.append(wi * r)
        time.sleep(0.15)  # rate-limit 배려

    if not contribs:
        return None

    daily_ret = float(np.sum(contribs))
    if not math.isfinite(daily_ret):
        return None

    return float(prev_idx * (1.0 + daily_ret))

def append_today(ws_index, value: float):
    today = kst_today_date().isoformat()
    updated_at = kst_now().isoformat()
    ws_index.append_row([today, float(value), "live", updated_at], value_input_option="USER_ENTERED")
    print(f"[OK] Appended live row: {today} -> {value}")

def main():
    sh = authorize()
    ws_index = ensure_ws(sh)

    today = kst_today_date().isoformat()
    last = get_last_row(ws_index)
    if last and last[0] == today:
        print(f"[INFO] Already updated for {today}. Skipping.")
        return

    val = calc_today_index_value(sh, ws_index)
    if val is None or not math.isfinite(val):
        prev = get_last_index_value(ws_index)
        if prev is None:
            raise RuntimeError("No previous index value and daily calc unavailable.")
        print("[WARN] Daily calc unavailable; carrying forward previous value.")
        val = prev

    append_today(ws_index, val)

if __name__ == "__main__":
    for k in ("SHEET_ID", "GOOGLE_SERVICE_ACCOUNT_JSON"):
        if not os.environ.get(k):
            raise SystemExit(f"Missing required environment variable: {k}")
    main()
