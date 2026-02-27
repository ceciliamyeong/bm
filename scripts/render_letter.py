#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent

TEMPLATE = ROOT / "letter_newsletter_template.html"
BM20_JSON = ROOT / "bm20_latest.json"
DAILY_CSV = ROOT / "bm20_daily_data_latest.csv"
KRW_JSON = ROOT / "out/history/krw_24h_latest.json"
BTC_JSON = ROOT / "out/history/btc_usd_series.json"
OUT = ROOT / "letter.html"

GREEN = "#16a34a"
RED = "#dc2626"
INK = "#0f172a"

def colored_change_html(v):
    v = float(v)
    if v > 0:
        return f'<span style="color:{GREEN};font-weight:900;">▲ {v:+.2f}%</span>'
    if v < 0:
        return f'<span style="color:{RED};font-weight:900;">▼ {v:+.2f}%</span>'
    return f'<span style="color:{INK};font-weight:900;">{v:+.2f}%</span>'

def build():
    bm20 = json.loads(BM20_JSON.read_text(encoding="utf-8"))
    krw = json.loads(KRW_JSON.read_text(encoding="utf-8"))
    df = pd.read_csv(DAILY_CSV)
    series = json.loads(BTC_JSON.read_text(encoding="utf-8"))

    btc_last = float(series[-1]["price"])
    btc_prev = float(series[-2]["price"])
    btc_1d = (btc_last / btc_prev - 1) * 100

    r1d = bm20["returns"]["1D"] * 100
    level = bm20["bm20Level"]

    html = TEMPLATE.read_text(encoding="utf-8")
    html = html.replace("{{BTC_USD}}", f"{btc_last:,.0f}")
    html = html.replace("{{BTC_1D}}", colored_change_html(btc_1d))
    html = html.replace("{{BM20_LEVEL}}", f"{level:,.2f}")
    html = html.replace("{{BM20_1D}}", colored_change_html(r1d))

    OUT.write_text(html, encoding="utf-8")
    print("Letter rendered:", OUT)

if __name__ == "__main__":
    build()
