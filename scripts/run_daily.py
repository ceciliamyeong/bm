#!/usr/bin/env python3
import os, json, datetime as dt
import gspread
from google.oauth2.service_account import Credentials

HEADER = ["date","index","flag","updated_at"]
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def kst_now_iso():
    KST = dt.timezone(dt.timedelta(hours=9))
    return dt.datetime.now(tz=KST).isoformat()

def authorize_ws():
    sheet_id = os.environ["SHEET_ID"]
    sa_json = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
    creds = Credentials.from_service_account_info(json.loads(sa_json), scopes=SCOPES)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)
    try:
        ws = sh.worksheet("bm20_index")
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title="bm20_index", rows=2000, cols=len(HEADER))
        ws.append_row(HEADER, value_input_option="USER_ENTERED")
    return ws

def calc_today_index_value():
    # TODO: plug in your real daily calc later (return float).
    # Returning None will carry forward yesterday's value.
    return None

def last_row(ws):
    vals = ws.get_all_values()
    return vals[-1] if len(vals) >= 2 else None

def last_index_value(ws):
    vals = ws.get_values("B:B")
    try:
        return float(vals[-1][0]) if vals and vals[-1] and vals[-1][0] else None
    except Exception:
        return None

def main():
    ws = authorize_ws()
    today = dt.date.today().isoformat()

    lr = last_row(ws)
    if lr and lr[0] == today:
        print(f"[INFO] Already updated for {today}. Skipping.")
        return

    idx = calc_today_index_value()
    if idx is None:
        prev = last_index_value(ws)
        if prev is None:
            raise RuntimeError("No previous index value available and daily calc not implemented.")
        idx = prev

    ws.append_row([today, float(idx), "live", kst_now_iso()], value_input_option="USER_ENTERED")
    print(f"[OK] Appended live row: {today} -> {idx}")

if __name__ == "__main__":
    main()
