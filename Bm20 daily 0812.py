#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BM20 daily automation — 2025-08-12 upgrades

Adds three features:
1) Newspaper-style news sentence generator (Korean newsroom tone)
2) Kimchi Premium chart saved alongside daily artifacts
3) Weekly/Monthly report auto-generation using recent history

Assumptions / Integration notes:
- Existing pipeline already fetches market data and computes a BM20 daily snapshot
  including index level, daily % change, market cap coverage, flows (ETF/spot),
  top gainers/losers, and per-coin contributions.
- Existing artifacts (CSV, TXT, PNG, PDF) are saved under a date-stamped folder
  and uploaded to Google Drive via helper `upload_to_gdrive(path, drive_folder_id)`.
- Timezone is Asia/Seoul (KST). GitHub Actions runs daily ~08:00 KST.

Where to integrate:
- Replace the stub functions `get_today_snapshot()` and `get_recent_history()` with your
  actual data sources (internal modules, API calls, cached parquet, etc.).
- If you already have sentence generation or report code, you can drop-in the
  functions from this file and wire them into your existing main() flow.

"""
from __future__ import annotations

import os
import io
import sys
import math
import json
import textwrap
import datetime as dt
from dataclasses import dataclass
from typing import List, Optional, Tuple

import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

try:
    # Python 3.9+
    from zoneinfo import ZoneInfo
except Exception:
    from pytz import timezone as ZoneInfo  # type: ignore

# =============================
# Config
# =============================
KST = ZoneInfo("Asia/Seoul")
TODAY = dt.datetime.now(KST).date()
DATE_STR = TODAY.strftime("%Y-%m-%d")
OUTPUT_ROOT = os.environ.get("BM20_OUTPUT_ROOT", "./out")
DRIVE_FOLDER_ID = os.environ.get("BM20_DRIVE_FOLDER_ID", "")
DAILY_DIR = os.path.join(OUTPUT_ROOT, DATE_STR)

# Create folder early
os.makedirs(DAILY_DIR, exist_ok=True)

# =============================
# Data Types
# =============================
@dataclass
class Bm20Snapshot:
    date: dt.date
    index_level: float
    index_chg_pct: float  # day-on-day percent, e.g., -0.82 for -0.82%
    mcap_total: float     # total market cap of BM20 (USD)
    turnover_usd: float   # 24h volume (USD)
    etf_flow_usd: Optional[float] = None
    cex_netflow_usd: Optional[float] = None
    
    # Top movers dataframe columns expected:
    # [symbol, name, ret_pct, contrib_bps]
    top_movers: Optional[pd.DataFrame] = None
    
    # Per-coin weights and contributions for the day (for charts/reports)
    # [symbol, name, weight, ret_pct, contrib_bps]
    contributions: Optional[pd.DataFrame] = None

# =============================
# Stubs: integrate with your data loaders
# =============================
def get_today_snapshot() -> Bm20Snapshot:
    """Replace with your actual snapshot builder.
    This stub returns minimal fields for dev/testing.
    """
    # TODO: integrate with your existing computation
    rng = np.random.default_rng(42)
    dummy_movers = pd.DataFrame({
        "symbol": ["BTC", "ETH", "XRP", "SOL", "ADA"],
        "name":   ["Bitcoin", "Ethereum", "XRP", "Solana", "Cardano"],
        "ret_pct": rng.normal(0, 1.5, 5).round(2),
        "contrib_bps": rng.normal(0, 8, 5).round(1),
    }).sort_values("contrib_bps", ascending=False)

    dummy_contrib = pd.DataFrame({
        "symbol": ["BTC", "ETH", "SOL", "XRP", "ADA"],
        "name":   ["Bitcoin", "Ethereum", "Solana", "XRP", "Cardano"],
        "weight": [0.38, 0.25, 0.08, 0.06, 0.04],
        "ret_pct": rng.normal(0, 1.2, 5).round(2),
        "contrib_bps": rng.normal(0, 8, 5).round(1),
    })

    return Bm20Snapshot(
        date=TODAY,
        index_level=1000 + rng.normal(0, 10),
        index_chg_pct=rng.normal(0, 1.0),
        mcap_total=6.3e11,
        turnover_usd=2.1e10,
        etf_flow_usd=rng.normal(0, 1.5e8),
        cex_netflow_usd=rng.normal(0, 7.5e7),
        top_movers=dummy_movers,
        contributions=dummy_contrib,
    )


def get_recent_history(days: int = 35) -> pd.DataFrame:
    """Return recent daily index history with columns: [date, index_level, index_ret_pct].
    Replace with your actual historical source.
    """
    dates = pd.date_range(end=TODAY, periods=days, tz=KST).date
    rng = np.random.default_rng(123)
    rets = rng.normal(0, 1.0, len(dates)) / 100
    idx = 1000 * (1 + pd.Series(rets)).cumprod().values
    return pd.DataFrame({
        "date": dates,
        "index_level": idx,
        "index_ret_pct": (pd.Series(rets) * 100).round(2),
    })

# =============================
# 1) Newspaper-style sentence generator (Korean newsroom tone)
# =============================

def _fmt_pct(x: float, digits: int = 2) -> str:
    return f"{x:+.{digits}f}%"


def _fmt_usd(x: Optional[float]) -> str:
    if x is None or (isinstance(x, float) and (math.isnan(x) or math.isinf(x))):
        return "자료 없음"
    # Scale to bn/mn
    sign = "-" if x < 0 else ""
    ax = abs(x)
    if ax >= 1e9:
        return f"{sign}{ax/1e9:.2f}억달러"
    elif ax >= 1e6:
        return f"{sign}{ax/1e6:.1f}백만달러"
    else:
        return f"{sign}{ax:,.0f}달러"


def build_news_sentences(s: Bm20Snapshot) -> List[str]:
    """Produce 4~8 crisp sentences in a Korean newspaper tone for the daily TXT.
    """
    headline_dir = "하락" if s.index_chg_pct < 0 else ("상승" if s.index_chg_pct > 0 else "보합")
    dir_word = "내렸다" if s.index_chg_pct < 0 else ("올랐다" if s.index_chg_pct > 0 else "변동 없었다")
    chg = _fmt_pct(s.index_chg_pct)

    sentences = []
    sentences.append(
        f"BM20 지수 {DATE_STR} {headline_dir}. 전일 대비 {chg}를 기록했다."
    )

    sentences.append(
        f"시가총액은 약 {s.mcap_total/1e9:.1f}억달러, 24시간 거래대금은 {s.turnover_usd/1e9:.1f}억달러로 집계됐다."
    )

    if s.etf_flow_usd is not None:
        etf_flow_txt = "순유입" if s.etf_flow_usd > 0 else ("순유출" if s.etf_flow_usd < 0 else "변동 미미")
        sentences.append(
            f"현물 ETF 자금은 {etf_flow_txt}을 보이며 { _fmt_usd(s.etf_flow_usd) } 규모로 추정된다."
        )

    if s.top_movers is not None and len(s.top_movers) > 0:
        top = s.top_movers.nlargest(3, "contrib_bps").copy()
        losers = s.top_movers.nsmallest(3, "contrib_bps").copy()
        top_txt = ", ".join([f"{r.name}({r.symbol} {r.ret_pct:+.2f}%)" for r in top.itertuples()])
        los_txt = ", ".join([f"{r.name}({r.symbol} {r.ret_pct:+.2f}%)" for r in losers.itertuples()])
        sentences.append(f"기여 상위: {top_txt}.")
        sentences.append(f"기여 하위: {los_txt}.")

    if s.contributions is not None and len(s.contributions) > 0:
        w = s.contributions.sort_values("weight", ascending=False).head(5)
        top_w = ", ".join([f"{r.symbol} {r.weight*100:.1f}%" for r in w.itertuples()])
        sentences.append(f"지수 비중 상위: {top_w}.")

    sentences.append("(데이터: Blockmedia BM20)")
    return sentences


def save_daily_news_txt(sentences: List[str], out_dir: str) -> str:
    txt = "\n".join(sentences)
    path = os.path.join(out_dir, f"BM20_daily_{DATE_STR}.txt")
    with open(path, "w", encoding="utf-8") as f:
        f.write(txt + "\n")
    return path

# =============================
# 2) Kimchi Premium computation & chart
# =============================

def compute_kimchi_premium(btc_krw: pd.Series, btc_usd: pd.Series, krw_usd: pd.Series) -> pd.DataFrame:
    """Compute kimchi premium: (BTC_KRW / (BTC_USD*KRWUSD) - 1) * 100 (%).
    Input series should be indexed by tz-aware timestamps (KST or UTC) and aligned to the
    same frequency (we resample to 5-min by default upstream; here we forward-fill and align).
    """
    df = pd.concat({"btc_krw": btc_krw, "btc_usd": btc_usd, "krw_usd": krw_usd}, axis=1).sort_index()
    df = df.ffill().dropna()
    fair_krw = df["btc_usd"] * df["krw_usd"]
    prem = (df["btc_krw"] / fair_krw - 1.0) * 100.0
    out = pd.DataFrame({"kimchi_premium_pct": prem})
    return out


def save_kimchi_premium_chart(prem: pd.DataFrame, out_dir: str) -> Tuple[str, str]:
    png_path = os.path.join(out_dir, f"kimchi_premium_{DATE_STR}.png")
    pdf_path = os.path.join(out_dir, f"kimchi_premium_{DATE_STR}.pdf")

    for path in [png_path, pdf_path]:
        plt.figure(figsize=(9, 4.5))
        plt.plot(prem.index, prem["kimchi_premium_pct"])  # no explicit colors
        plt.axhline(0, linewidth=1)
        plt.title(f"Kimchi Premium — {DATE_STR}")
        plt.xlabel("Time (KST)")
        plt.ylabel("% vs. offshore")
        plt.tight_layout()
        if path.endswith(".png"):
            plt.savefig(path, dpi=160)
        else:
            with PdfPages(path) as pdf:
                pdf.savefig()
        plt.close()
    return png_path, pdf_path

# =============================
# 3) Weekly & Monthly report generation
# =============================

def build_periodic_summary(history: pd.DataFrame, period: str) -> Tuple[str, pd.DataFrame]:
    """Summarize BM20 over a period ('W' or 'M').
    history: columns [date, index_level, index_ret_pct]
    Returns: (title, summary_df)
    """
    df = history.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(KST, nonexistent="shift_forward", ambiguous="NaT").dt.date
    df = df.sort_values("date")

    if period == "W":
        start = (TODAY - dt.timedelta(days=TODAY.weekday()+1))  # last Sun
        end = TODAY - dt.timedelta(days=1)  # yesterday
        title = f"BM20 주간 리포트 ({start}~{end})"
    elif period == "M":
        first_day = TODAY.replace(day=1)
        last_month_end = first_day - dt.timedelta(days=1)
        start = last_month_end.replace(day=1)
        end = last_month_end
        title = f"BM20 월간 리포트 ({start}~{end})"
    else:
        raise ValueError("period must be 'W' or 'M'")

    mask = (df["date"] >= start) & (df["date"] <= end)
    window = df.loc[mask].copy()

    if window.empty:
        # fall back to last 7 or 30
        days = 7 if period == "W" else 30
        window = df.tail(days).copy()
        start, end = window["date"].min(), window["date"].max()
        title = f"BM20 {('주간' if period=='W' else '월간')} 리포트 ({start}~{end})"

    level_start = window["index_level"].iloc[0]
    level_end = window["index_level"].iloc[-1]
    chg_pct = (level_end/level_start - 1)*100

    vol = window["index_ret_pct"].std() * np.sqrt(len(window))

    summary = pd.DataFrame({
        "metric": ["기간 시작", "기간 종료", "지수 변화", "변동성(단순)", "관측 일수"],
        "value": [f"{level_start:,.2f}", f"{level_end:,.2f}", f"{chg_pct:+.2f}%", f"{vol:.2f}pp", len(window)]
    })
    return title, summary


def save_periodic_report(history: pd.DataFrame, period: str, out_dir: str) -> Tuple[str, str]:
    title, summary = build_periodic_summary(history, period)

    # TXT
    txt_path = os.path.join(out_dir, f"BM20_{'weekly' if period=='W' else 'monthly'}_{DATE_STR}.txt")
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write(title + "\n")
        f.write("-"*len(title) + "\n\n")
        for m, v in summary.itertuples(index=False):
            f.write(f"{m}: {v}\n")

    # PDF — include line chart
    pdf_path = os.path.join(out_dir, f"BM20_{'weekly' if period=='W' else 'monthly'}_{DATE_STR}.pdf")
    with PdfPages(pdf_path) as pdf:
        # cover page
        plt.figure(figsize=(8.27, 11.69))  # A4 portrait
        plt.axis('off')
        plt.text(0.5, 0.85, title, ha='center', va='center', fontsize=20)
        now = dt.datetime.now(KST).strftime("%Y-%m-%d %H:%M KST")
        plt.text(0.5, 0.80, f"발행: Blockmedia · {now}", ha='center', va='center')
        pdf.savefig(); plt.close()

        # summary table as text
        plt.figure(figsize=(8.27, 11.69))
        plt.axis('off')
        y = 0.9
        for m, v in summary.itertuples(index=False):
            plt.text(0.1, y, f"• {m}", fontsize=12)
            plt.text(0.6, y, str(v), fontsize=12)
            y -= 0.06
        pdf.savefig(); plt.close()

        # history chart
        plt.figure(figsize=(8.27, 5))
        window = history.copy()
        # use same dates as in build_periodic_summary
        # Recompute the mask to ensure consistency
        # (quick reuse)
        # This is acceptable because it's deterministic for a given TODAY
        # and 'period'.
        if period == 'W':
            start = (TODAY - dt.timedelta(days=TODAY.weekday()+1))
            end = TODAY - dt.timedelta(days=1)
        else:
            first_day = TODAY.replace(day=1)
            last_month_end = first_day - dt.timedelta(days=1)
            start = last_month_end.replace(day=1)
            end = last_month_end
        mask = (pd.to_datetime(window['date']).dt.date >= start) & (pd.to_datetime(window['date']).dt.date <= end)
        window = window.loc[mask]
        if window.empty:
            window = history.tail(30 if period=='M' else 7)
        plt.plot(pd.to_datetime(window['date']), window['index_level'])
        plt.title("BM20 지수 추이")
        plt.xlabel("Date")
        plt.ylabel("Index Level")
        plt.tight_layout()
        pdf.savefig(); plt.close()

    return txt_path, pdf_path

# =============================
# Upload helper
# =============================

def upload_if_configured(local_path: str) -> None:
    if not local_path:
        return
    if not DRIVE_FOLDER_ID:
        return
    try:
        # Expect an existing helper in your project
        from gdrive_uploader import upload_to_gdrive  # type: ignore
        upload_to_gdrive(local_path, DRIVE_FOLDER_ID)
    except Exception as e:
        print(f"[WARN] Upload skipped or failed for {local_path}: {e}")

# =============================
# Main flow (wire-in to your existing bm20_daily.py)
# =============================

def main():
    # 0) Load data
    snap = get_today_snapshot()

    # 1) Newspaper-style TXT
    sentences = build_news_sentences(snap)
    txt_path = save_daily_news_txt(sentences, DAILY_DIR)
    upload_if_configured(txt_path)

    # 2) Kimchi Premium chart — integrate your series below
    try:
        # TODO: replace with your actual series (tz-aware index). Examples:
        # btc_krw: Upbit BTC/KRW mid price
        # btc_usd: Coinbase BTC/USD mid price
        # krw_usd: USD/KRW FX rate inverted to KRW per USD (i.e., KRWUSD)
        idx = pd.date_range(TODAY, periods=24*12, freq='5min', tz=KST)
        rng = np.random.default_rng(7)
        btc_usd = pd.Series(60000 + rng.normal(0, 80, len(idx)).cumsum()/10, index=idx)
        krw_usd = pd.Series(1380 + rng.normal(0, 0.5, len(idx)).cumsum()/50, index=idx)
        btc_krw = pd.Series(btc_usd.values * krw_usd.values * (1 + rng.normal(0.02, 0.005, len(idx))), index=idx)

        prem = compute_kimchi_premium(btc_krw, btc_usd, krw_usd)
        png_k, pdf_k = save_kimchi_premium_chart(prem, DAILY_DIR)
        upload_if_configured(png_k)
        upload_if_configured(pdf_k)
    except Exception as e:
        print(f"[WARN] Kimchi premium step skipped: {e}")

    # 3) Weekly / Monthly Reports — trigger conditions
    try:
        history = get_recent_history(days=60)
        # Weekly: generate on Mondays for the prior week
        if dt.datetime.now(KST).weekday() == 0:  # Monday
            w_txt, w_pdf = save_periodic_report(history, period='W', out_dir=DAILY_DIR)
            upload_if_configured(w_txt); upload_if_configured(w_pdf)
        # Monthly: generate on 1st day (cover prior month)
        if TODAY.day == 1:
            m_txt, m_pdf = save_periodic_report(history, period='M', out_dir=DAILY_DIR)
            upload_if_configured(m_txt); upload_if_configured(m_pdf)
    except Exception as e:
        print(f"[WARN] Periodic report step skipped: {e}")

    print(f"BM20 daily pipeline completed for {DATE_STR}.")


if __name__ == "__main__":
    main()
