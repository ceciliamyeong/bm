#!/usr/bin/env python3
import os, json, pandas as pd, gspread
from datetime import datetime, timezone, timedelta
from google.oauth2.service_account import Credentials

OUT_DIR = os.getenv("OUT_DIR", "out")
CSV = os.path.join(OUT_DIR, "history", "bm20_index_history.csv")
SHEET_ID = os.environ["SHEET_ID"]
SA_JSON  = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
DRY_RUN  = os.getenv("DRY_RUN","0") == "1"

def now_kst(): return datetime.now(timezone(timedelta(hours=9))).isoformat()

hist = pd.read_csv(CSV, dtype={"date":str, "index":float}).sort_values("date")
last_date  = str(hist.iloc[-1]["date"])
last_index = float(hist.iloc[-1]["index"])

creds = Credentials.from_service_account_info(json.loads(SA_JSON),
          scopes=["https://www.googleapis.com/auth/spreadsheets"])
gc = gspread.authorize(creds); sh = gc.open_by_key(SHEET_ID)
try: ws = sh.worksheet("bm20_index")
except gspread.exceptions.WorksheetNotFound:
    ws = sh.add_worksheet(title="bm20_index", rows=50000, cols=8)
    ws.append_row(["date","index","flag","updated_at"], value_input_option="USER_ENTERED")

rows = ws.get_all_values()
last_sheet_date = rows[-1][0] if len(rows) >= 2 else None
row = [last_date, f"{last_index:.6f}", f"Market date (NYT): {last_date}", now_kst()]

if DRY_RUN:
    print("[DRY_RUN]", row)
elif last_sheet_date == last_date:
    print(f"[INFO] {last_date} already on sheet, skip.")
else:
    ws.append_row(row, value_input_option="USER_ENTERED")
    print(f"[OK] appended", row)
