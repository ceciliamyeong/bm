#!/usr/bin/env python3
import os, json, argparse
from datetime import datetime, timezone, timedelta
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

def now_kst_iso():
    return datetime.now(timezone(timedelta(hours=9))).isoformat()

def load_history(out_dir: str, days: int | None):
    csv = os.path.join(out_dir, "history", "bm20_index_history.csv")
    if not os.path.exists(csv):
        raise FileNotFoundError(f"Not found: {csv} (먼저 bm20_daily.py를 실행해 history를 생성하세요)")
    df = pd.read_csv(csv, dtype={"date": str, "index": float}).sort_values("date")
    if days and days > 0:
        df = df.tail(days)
    if df.empty:
        raise RuntimeError("History CSV is empty after filtering.")
    return df

def ensure_ws(sh):
    try:
        ws = sh.worksheet("bm20_index")
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title="bm20_index", rows=50000, cols=8)
        ws.append_row(["date","index","flag","updated_at"], value_input_option="USER_ENTERED")
    return ws

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=None, help="최근 N일만 업로드 (기본: 전체)")
    ap.add_argument("--chunk", type=int, default=400, help="append_rows 배치 크기")
    ap.add_argument("--dry-run", action="store_true", help="시트에 쓰지 않고 출력만")
    args = ap.parse_args()

    sheet_id = os.environ["SHEET_ID"]
    sa_json  = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
    out_dir  = os.getenv("OUT_DIR", "out")

    df = load_history(out_dir, args.days)

    creds = Credentials.from_service_account_info(json.loads(sa_json),
              scopes=["https://www.googleapis.com/auth/spreadsheets"])
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)
    ws = ensure_ws(sh)

    have = set()
    rows_all = ws.get_all_values()
    if len(rows_all) >= 2:
        for r in rows_all[1:]:
            if r and r[0]:
                have.add(r[0])

    # flag는 간단히 동일 날짜로 표기(원하면 NYT로 변환해도 됨)
    rows = []
    for _, r in df.iterrows():
        d = str(r["date"])
        if d in have:
            continue
        rows.append([d, f'{float(r["index"]):.6f}', f"Market date (NYT): {d}", now_kst_iso()])

    if not rows:
        print("[INFO] nothing to append (all dates already on sheet).")
        return

    if args.dry_run:
        print(f"[DRY_RUN] would append {len(rows)} rows: {rows[0][0]} → {rows[-1][0]}")
        return

    # batch append
    for i in range(0, len(rows), args.chunk):
        ws.append_rows(rows[i:i+args.chunk], value_input_option="USER_ENTERED")
    print(f"[OK] appended {len(rows)} rows: {rows[0][0]} → {rows[-1][0]}")

if __name__ == "__main__":
    main()
