#!/usr/bin/env python3

Pull bm20_map from Google Sheets and save to local CSV (bm20_map_btc30.csv).

Usage (env vars required):
  GSPREAD_SA_JSON  = service account JSON (full text)
  BM20_SHEET_ID    = Google Sheets spreadsheet ID

Columns expected on the sheet:
  symbol | yf_ticker | listed_kr_override | include | cap_override
Missing columns will be created empty.

This is safe to run in GitHub Actions. Grant the service account Editor access to the sheet.



import os, sys, json
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials

SCOPES = ['https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive']

def main():
    sa = os.getenv("GSPREAD_SA_JSON")
    sid = os.getenv("BM20_SHEET_ID")
    if not sa or not sid:
        print("[ERR] require env GSPREAD_SA_JSON and BM20_SHEET_ID", file=sys.stderr)
        sys.exit(1)
    creds = Credentials.from_service_account_info(json.loads(sa), scopes=SCOPES)
    gc = gspread.authorize(creds)
    ss = gc.open_by_key(sid)

    try:
        ws = ss.worksheet("bm20_map")
    except gspread.WorksheetNotFound:
        print("[ERR] worksheet 'bm20_map' not found", file=sys.stderr)
        sys.exit(1)
    vals = ws.get_all_values()
    if not vals or len(vals) < 2:
        print("[ERR] bm20_map empty", file=sys.stderr)
        sys.exit(1)

    cols = [c.strip() for c in vals[0]]
    df = pd.DataFrame(vals[1:], columns=cols)
    for c in ["symbol","yf_ticker","listed_kr_override","include","cap_override"]:
        if c not in df.columns:
            df[c] = np.nan

    df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
    df["yf_ticker"] = df["yf_ticker"].astype(str).str.strip().replace({"": np.nan})

    # normalise booleans
    def to_bool(s):
        s = str(s).strip().lower()
        return s in ("1","true","y","yes")
    df["listed_kr_override"] = df["listed_kr_override"].map(to_bool)
    df["include"] = df["include"].map(to_bool) | df["include"].isna()  # default True
    df["cap_override"] = pd.to_numeric(df["cap_override"], errors="coerce")

    out = df[["symbol","yf_ticker","listed_kr_override","include","cap_override"]].copy()
    out.to_csv("bm20_map_btc30.csv", index=False, encoding="utf-8")
    print(f"[OK] wrote bm20_map_btc30.csv rows={len(out)}")

if __name__ == "__main__":
    main()
