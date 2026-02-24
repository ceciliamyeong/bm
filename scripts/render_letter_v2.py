#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Render newsletter HTML (email-safe) by injecting latest metrics into placeholders.

Inputs (repo root default):
- letter_newsletter_template.html  (template with placeholders)
- bm20_latest.json                 (BM20 level/returns/kimchi)
- bm20_daily_data_latest.csv       (for Best/Worst/Breadth)
- out/history/krw_24h_latest.json  (KRW 24h volumes + timestamp)

Output:
- letter_rendered.html
"""

from __future__ import annotations

import json
from pathlib import Path
import pandas as pd

ROOT = Path(".")
TEMPLATE = ROOT / "letter_newsletter_template.html"
BM20_JSON = ROOT / "bm20_latest.json"
DAILY_CSV = ROOT / "bm20_daily_data_latest.csv"
KRW_JSON = ROOT / "out/history/krw_24h_latest.json"
OUT = ROOT / "letter_rendered.html"

# ---------- formatting ----------
def fmt_level(x: float) -> str:
    return f"{float(x):,.2f}"

def fmt_pct_from_ratio_or_pct(x: float, digits: int = 2) -> str:
    """If x looks like a ratio (<=1.5), convert to pct."""
    x = float(x)
    if abs(x) <= 1.5:
        x *= 100.0
    return f"{x:+.{digits}f}%"

def fmt_share_pct(x: float) -> str:
    """x may be 0-1 or 0-100"""
    x = float(x)
    if x <= 1.5:
        x *= 100.0
    return f"{x:.1f}%"

def fmt_krw_big(x: float) -> str:
    """KRW to compact '조원/억원' string."""
    x = float(x)
    jo = 1_0000_0000_0000  # 1조
    eok = 1_0000_0000      # 1억
    if x >= jo:
        return f"{x/jo:.2f}조원"
    if x >= eok:
        return f"{x/eok:.1f}억원"
    return f"{x:,.0f}원"

def fmt_num(x: float, digits: int = 2) -> str:
    return f"{float(x):,.{digits}f}"

# ---------- loads ----------
def load_json(p: Path) -> dict:
    if not p.exists():
        raise FileNotFoundError(f"Missing {p} (expected at this path).")
    return json.loads(p.read_text(encoding="utf-8"))

def load_daily_df() -> pd.DataFrame:
    if not DAILY_CSV.exists():
        raise FileNotFoundError(f"Missing {DAILY_CSV}.")
    df = pd.read_csv(DAILY_CSV)
    if "symbol" not in df.columns:
        for c in ["ticker", "asset"]:
            if c in df.columns:
                df = df.rename(columns={c:"symbol"})
                break
    if "price_change_pct" not in df.columns:
        for c in ["change_pct", "pct_change", "return_1d_pct"]:
            if c in df.columns:
                df = df.rename(columns={c:"price_change_pct"})
                break
    df["price_change_pct"] = pd.to_numeric(df["price_change_pct"], errors="coerce")
    return df

def compute_best_worst_breadth(df: pd.DataFrame, n=3):
    best = df.sort_values("price_change_pct", ascending=False).head(n)
    worst = df.sort_values("price_change_pct", ascending=True).head(n)
    best_txt = "<br/>".join([f"{r.symbol} {r.price_change_pct:+.2f}%" for r in best.itertuples()])
    worst_txt = "<br/>".join([f"{r.symbol} {r.price_change_pct:+.2f}%" for r in worst.itertuples()])
    up = int((df["price_change_pct"] > 0).sum())
    down = int((df["price_change_pct"] < 0).sum())
    breadth = f"상승 {up} · 하락 {down}"
    return best_txt, worst_txt, breadth, up, down

def build_placeholders() -> dict[str,str]:
    bm20 = load_json(BM20_JSON)
    krw = load_json(KRW_JSON)
    df = load_daily_df()

    BTC_JSON = ROOT / "out/history/btc_usd_series.json"
    btc_series = json.loads(BTC_JSON.read_text(encoding="utf-8"))

    last = btc_series[-1]["price"]
    prev = btc_series[-2]["price"]

    btc_usd = float(last)
    btc_1d = (btc_usd / float(prev) - 1) * 100

    btc_price_txt = f"{btc_usd:,.0f}"
    btc_1d_txt = f"{btc_1d:+.2f}%"
    
    best3, worst3, breadth, up, down = compute_best_worst_breadth(df, n=3)

    # BM20
    asof = bm20.get("asof","")
    level = bm20.get("bm20Level", None)
    r1d = (bm20.get("returns",{}) or {}).get("1D", None)

    bm20_1d = "—"
    direction = "보합"
    if r1d is not None:
        bm20_1d = fmt_pct_from_ratio_or_pct(r1d)
        # direction based on pct value
        rtmp = float(r1d)
        if abs(rtmp) <= 1.5: rtmp *= 100.0
        direction = "반등" if rtmp > 0 else ("약세" if rtmp < 0 else "보합")

    comment = f"한 줄 코멘트: BM20 {direction}, 상승 {up} · 하락 {down}"

    # Kimchi (from bm20_latest.json)
    kimchi_p = bm20.get("kimchi_premium_pct", None)
    usdkrw = (bm20.get("kimchi_meta", {}) or {}).get("usdkrw", None)
    kimchi_txt = "—"
    usdkrw_txt = "—"
    if kimchi_p is not None:
        # kimchi_premium_pct seems already percent value (e.g., 1.7015)
        kimchi_txt = f"{float(kimchi_p):+.2f}%"
    if usdkrw is not None:
        usdkrw_txt = fmt_num(usdkrw, 2)

    # KRW 24h totals + exchange shares
    ts_label = krw.get("timestamp_label","")  # already KST label
    totals = (krw.get("totals", {}) or {})
    combined = totals.get("combined_24h", None)
    upbit_v = totals.get("upbit_24h", None)
    bith_v = totals.get("bithumb_24h", None)
    coin_v = totals.get("coinone_24h", None)

    # shares computed
    upbit_share = bith_share = coin_share = None
    if combined and upbit_v is not None:
        upbit_share = (float(upbit_v) / float(combined)) * 100.0
    if combined and bith_v is not None:
        bith_share = (float(bith_v) / float(combined)) * 100.0
    if combined and coin_v is not None:
        coin_share = (float(coin_v) / float(combined)) * 100.0

    ph = {
        "{{BM20_LEVEL}}": fmt_level(level) if level is not None else "—",
        "{{BTC_USD}}": btc_price_txt,
        "{{BTC_1D}}": btc_1d_txt,
        "{{BM20_ASOF}}": str(asof),
        "{{BM20_1D}}": bm20_1d,
        "{{BM20_BREADTH}}": breadth,
        "{{BM20_BEST3}}": best3,
        "{{BM20_WORST3}}": worst3,
        "{{BM20_COMMENT}}": comment,

        "{{KIMCHI_PREM}}": kimchi_txt,
        "{{USDKRW}}": usdkrw_txt,

        "{{KRW_TOTAL_24H}}": fmt_krw_big(combined) if combined is not None else "—",
        "{{KRW_ASOF_KST}}": ts_label if ts_label else str(asof),
        "{{UPBIT_SHARE_24H}}": fmt_share_pct(upbit_share) if upbit_share is not None else "—",
        "{{BITHUMB_SHARE_24H}}": fmt_share_pct(bith_share) if bith_share is not None else "—",
        "{{COINONE_SHARE_24H}}": fmt_share_pct(coin_share) if coin_share is not None else "—",
    }
    return ph

def render():
    if not TEMPLATE.exists():
        raise FileNotFoundError(f"Missing {TEMPLATE}.")
    html = TEMPLATE.read_text(encoding="utf-8")
    ph = build_placeholders()
    for k, v in ph.items():
        html = html.replace(k, v)
    OUT.write_text(html, encoding="utf-8")
    print(f"OK: wrote {OUT}")

if __name__ == "__main__":
    render()
