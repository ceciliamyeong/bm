#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Render newsletter HTML (email-safe) by injecting latest metrics into placeholders.

Inputs (repo root default):
- letter_newsletter_template.html  (template with placeholders)
- bm20_latest.json                 (BM20 level/returns/kimchi)
- bm20_daily_data_latest.csv       (for Best/Worst/Breadth)
- out/history/krw_24h_latest.json  (KRW 24h volumes + timestamp)
- out/history/btc_usd_series.json  (BTC USD series; last two points used for 1D)

Output (repo root):
- letter.html   (rendered)
"""

from __future__ import annotations

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

# ------------------ formatting helpers ------------------

GREEN = "#16a34a"
RED = "#dc2626"
INK = "#0f172a"

def fmt_level(x: float) -> str:
    return f"{float(x):,.2f}"

def fmt_num(x: float, digits: int = 2) -> str:
    return f"{float(x):,.{digits}f}"

def fmt_share_pct(x: float) -> str:
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

def pct_to_display(x: float, digits: int = 2) -> float:
    """Accept ratio(<=1.5) or pct; return pct number."""
    x = float(x)
    if abs(x) <= 1.5:
        x *= 100.0
    return x

def colored_change_html(pct_value: float, digits: int = 2, wrap_parens: bool = False) -> str:
    """
    Returns HTML span like: ▲ +1.23% (green) or ▼ -1.23% (red) or 0.00% (ink)
    """
    v = float(pct_value)
    if v > 0:
        arrow, color = "▲", GREEN
    elif v < 0:
        arrow, color = "▼", RED
    else:
        arrow, color = "", INK

    s = f"{v:+.{digits}f}%"
    text = f"{arrow} {s}".strip()
    if wrap_parens:
        text = f"({text})"
    return f'<span style="color:{color};font-weight:900;">{text}</span>'

def tone_bg(pct_value: float) -> str:
    """
    Very subtle background tone (email-safe). You can choose to use it in template via placeholders.
    """
    v = float(pct_value)
    if v > 0:
        return "#f0fdf4"  # green-50
    if v < 0:
        return "#fef2f2"  # red-50
    return "#ffffff"

# ------------------ load helpers ------------------

def load_json(p: Path) -> dict:
    if not p.exists():
        raise FileNotFoundError(f"Missing {p}")
    return json.loads(p.read_text(encoding="utf-8"))

def load_daily_df() -> pd.DataFrame:
    if not DAILY_CSV.exists():
        raise FileNotFoundError(f"Missing {DAILY_CSV}")
    df = pd.read_csv(DAILY_CSV)

    if "symbol" not in df.columns:
        for c in ["ticker", "asset"]:
            if c in df.columns:
                df = df.rename(columns={c: "symbol"})
                break

    if "price_change_pct" not in df.columns:
        for c in ["change_pct", "pct_change", "return_1d_pct"]:
            if c in df.columns:
                df = df.rename(columns={c: "price_change_pct"})
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

# ------------------ placeholders ------------------

def build_placeholders() -> dict[str, str]:
    bm20 = load_json(BM20_JSON)
    krw = load_json(KRW_JSON)
    df = load_daily_df()

    # BTC
    if not BTC_JSON.exists():
        raise FileNotFoundError(f"Missing {BTC_JSON}")
    btc_series = json.loads(BTC_JSON.read_text(encoding="utf-8"))
    if len(btc_series) < 2:
        raise ValueError(f"{BTC_JSON} must have at least 2 points.")
    btc_last = float(btc_series[-1]["price"])
    btc_prev = float(btc_series[-2]["price"])
    btc_1d = (btc_last / btc_prev - 1) * 100.0

    btc_usd_txt = f"{btc_last:,.0f}"
    btc_1d_html = colored_change_html(btc_1d, digits=2, wrap_parens=True)
    btc_bg = tone_bg(btc_1d)

    # BM20
    asof = bm20.get("asof", "")
    level = bm20.get("bm20Level", None)
    r1d_raw = (bm20.get("returns", {}) or {}).get("1D", None)

    bm20_1d_pct = None
    bm20_1d_html = "—"
    bm20_bg = "#ffffff"
    direction = "보합"
    if r1d_raw is not None:
        bm20_1d_pct = pct_to_display(r1d_raw)
        bm20_1d_html = colored_change_html(bm20_1d_pct, digits=2, wrap_parens=False)
        bm20_bg = tone_bg(bm20_1d_pct)
        if bm20_1d_pct > 0:
            direction = "반등"
        elif bm20_1d_pct < 0:
            direction = "약세"

    best3, worst3, breadth, up, down = compute_best_worst_breadth(df, n=3)

    # Comment with synced color chip
    if bm20_1d_pct is None:
        comment = f"한 줄 코멘트: BM20 보합, 상승 {up} · 하락 {down}"
        comment_chip = f'<span style="font-weight:900;color:{INK};">보합</span>'
    else:
        chip_color = GREEN if bm20_1d_pct > 0 else (RED if bm20_1d_pct < 0 else INK)
        comment_chip = f'<span style="font-weight:900;color:{chip_color};">{direction}</span>'
        comment = f"한 줄 코멘트: BM20 {direction}, 상승 {up} · 하락 {down}"

    # Kimchi (from bm20_latest.json)
    kimchi_p = bm20.get("kimchi_premium_pct", None)
    usdkrw = (bm20.get("kimchi_meta", {}) or {}).get("usdkrw", None)

    kimchi_html = "—"
    kimchi_bg = "#ffffff"
    if kimchi_p is not None:
        kimchi_pct = float(kimchi_p)  # already percent
        kimchi_html = colored_change_html(kimchi_pct, digits=2, wrap_parens=False)
        kimchi_bg = tone_bg(kimchi_pct)

    usdkrw_txt = fmt_num(usdkrw, 2) if usdkrw is not None else "—"

    # KRW 24h totals + exchange shares
    ts_label = krw.get("timestamp_label", "")  # already KST label
    totals = (krw.get("totals", {}) or {})
    combined = totals.get("combined_24h", None)
    upbit_v = totals.get("upbit_24h", None)
    bith_v = totals.get("bithumb_24h", None)
    coin_v = totals.get("coinone_24h", None)

    upbit_share = (float(upbit_v) / float(combined) * 100.0) if (combined and upbit_v is not None) else None
    bith_share = (float(bith_v) / float(combined) * 100.0) if (combined and bith_v is not None) else None
    coin_share = (float(coin_v) / float(combined) * 100.0) if (combined and coin_v is not None) else None

    return {
        # BM20
        "{{BM20_LEVEL}}": fmt_level(level) if level is not None else "—",
        "{{BM20_ASOF}}": str(asof),
        "{{BM20_1D}}": bm20_1d_html,
        "{{BM20_BREADTH}}": breadth,
        "{{BM20_BEST3}}": best3,
        "{{BM20_WORST3}}": worst3,
        "{{BM20_COMMENT}}": comment,
        "{{BM20_CHIP}}": comment_chip,
        "{{BM20_BG}}": bm20_bg,

        # BTC
        "{{BTC_USD}}": btc_usd_txt,
        "{{BTC_1D}}": btc_1d_html,
        "{{BTC_BG}}": btc_bg,

        # Kimchi
        "{{KIMCHI_PREM}}": kimchi_html,
        "{{KIMCHI_BG}}": kimchi_bg,
        "{{USDKRW}}": usdkrw_txt,

        # KRW
        "{{KRW_TOTAL_24H}}": fmt_krw_big(combined) if combined is not None else "—",
        "{{KRW_ASOF_KST}}": ts_label if ts_label else str(asof),
        "{{UPBIT_SHARE_24H}}": fmt_share_pct(upbit_share) if upbit_share is not None else "—",
        "{{BITHUMB_SHARE_24H}}": fmt_share_pct(bith_share) if bith_share is not None else "—",
        "{{COINONE_SHARE_24H}}": fmt_share_pct(coin_share) if coin_share is not None else "—",
    }

def render():
    if not TEMPLATE.exists():
        raise FileNotFoundError(f"Missing {TEMPLATE}")
    html = TEMPLATE.read_text(encoding="utf-8")
    ph = build_placeholders()
    for k, v in ph.items():
        html = html.replace(k, v)
    OUT.write_text(html, encoding="utf-8")
    print(f"OK: wrote {OUT}")

if __name__ == "__main__":
    render()
