# scripts/bm20_vs_btc.py
import os
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, date
from scripts.util_price import load_btc_close, load_bm20_index_history

BASE_DATE = "2024-01-01"
BASE_VALUE = 100.0
OUT_DIR_DATE = Path(f"out/{date.today().isoformat()}")  # 워크플로에서 실행 시 해당 일자 디렉토리로 수정됨
OUT_DIR_LATEST = Path("out/latest")
for d in (OUT_DIR_DATE, OUT_DIR_LATEST):
    d.mkdir(parents=True, exist_ok=True)

def compute_relative_series():
    bm20 = load_bm20_index_history()               # date, bm20
    btc  = load_btc_close(start_date="2017-01-01") # date, close (USD)

    # 기준일 정렬
    bm20_base = bm20[bm20["date"] >= pd.to_datetime(BASE_DATE).date()].copy()
    btc_base  = btc[ btc["date"]  >= pd.to_datetime(BASE_DATE).date()].copy()

    # 기준일 값
    bm20_0 = float(b
