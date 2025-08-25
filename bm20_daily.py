#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BM20 daily automation — Complete (BMindex primary, news sentences included)

- Primary data source: BMindex (2018~ long history)
- Fallback: index_history (if needed)
- Also reads: weights (daily or quarterly), constituents (optional), performance summary (optional)
- Outputs (under out/YYYY-MM-DD/):
  * BM20_daily_YYYY-MM-DD.txt      — Korean newsroom-style sentences
  * kimchi_premium_YYYY-MM-DD.png  — synthetic example unless you wire real series
  * kimchi_premium_YYYY-MM-DD.pdf
  * BM20_weekly_YYYY-MM-DD.(txt|pdf)    — generated on Mondays (covers prior week)
  * BM20_monthly_YYYY-MM-DD.(txt|pdf)   — generated on the 1st (covers prior month)

Environment:
- BM20_OUTPUT_ROOT (default: ./out)
- BM20_DRIVE_FOLDER_ID (optional; if you have a gdrive_uploader helper)

Time zone: Asia/Seoul
"""
from __future__ import annotations

import os, sys, math, json, textwrap
import datetime as dt
from dataclasses import dataclass
from typing import Optional, List, Tuple

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

try:
    from zoneinfo import ZoneInfo  # Py3.9+
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
os.makedirs(DAILY_DIR, exist_ok=True)

# === Google Sheets (published CSV) ===
URL_BM_INDEX      = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTndyrPd3WWwFtfzv2CZxJeDcH-l8ibQIdO5ouYS4HsaGpbeXQQbs6WEr9qPqqZbRoT6cObdFxJpief/pub?gid=720141148&single=true&output=csv"
URL_INDEX_HISTORY = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTndyrPd3WWwFtfzv2CZxJeDcH-l8ibQIdO5ouYS4HsaGpbeXQQbs6WEr9qPqqZbRoT6cObdFxJpief/pub?gid=1685318213&single=true&output=csv"
URL_CONSTITUENTS  = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTndyrPd3WWwFtfzv2CZxJeDcH-l8ibQIdO5ouYS4HsaGpbeXQQbs6WEr9qPqqZbRoT6cObdFxJpief/pub?gid=352245628&single=true&output=csv"
URL_WEIGHTS_Q     = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTndyrPd3WWwFtfzv2CZxJeDcH-l8ibQIdO5ouYS4HsaGpbeXQQbs6WEr9qPqqZbRoT6cObdFxJpief/pub?gid=1645238012&single=true&output=csv"
URL_WEIGHTS_DAILY = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTndyrPd3WWwFtfzv2CZxJeDcH-l8ibQIdO5ouYS4HsaGpbeXQQbs6WEr9qPqqZbRoT6cObdFxJpief/pub?gid=1533548287&single=true&output=csv"
URL_SUMMARY_PERF  = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTndyrPd3WWwFtfzv2CZxJeDcH-l8ibQIdO5ouYS4HsaGpbeXQQbs6WEr9qPqqZbRoT6cObdFxJpief/pub?gid=1065627907&single=true&output=csv"

# =============================
# Utils
# =============================
def _col(df: pd.DataFrame, *cands: str) -> Optional[str]:
    cmap = {c.lower().strip(): c for c in df.columns}
    for k in cands:
        if k in cmap:
            return cmap[k]
    for c in df.columns:  # allow exact match
        if c in cands:
            return c
    return None

def _as_date(col: pd.Series) -> pd.Series:
    return pd.to_datetime(col, errors="coerce").dt.date

# =============================
# Data types
# =============================
@dataclass
class Bm20Snapshot:
    date: dt.date
    index_level: float
    index_chg_pct: float  # %
    mcap_total: float     # USD, optional (nan if unknown)
    turnover_usd: float   # USD, optional (nan if unknown)
    etf_flow_usd: Optional[float] = None
    cex_netflow_usd: Optional[float] = None
    top_movers: Optional[pd.DataFrame] = None  # [symbol,name,ret_pct,contrib_bps]
    contributions: Optional[pd.DataFrame] = None  # [symbol,name,weight,ret_pct,contrib_bps]

# =============================
# Loaders
# =============================
def load_bm_index_full() -> pd.DataFrame:
    df = pd.read_csv(URL_BM_INDEX)
    c_date = _col(df, "date", "날짜")
    c_idx  = _col(df, "bm20_index", "bm_index", "index_level", "지수", "index")
    c_ret  = _col(df, "index_ret_pct", "ret_pct", "chg_pct", "d1", "수익률(%)")
    if not c_date or not c_idx:
        raise ValueError("BMindex 시트에 date / index_level(=bm20_index) 컬럼이 필요합니다.")
    df = df[[c_date, c_idx] + ([c_ret] if c_ret else [])].copy()
    df.columns = ["date", "index_level"] + (["index_ret_pct"] if c_ret else [])
    df["date"] = _as_date(df["date"])
    df = df.dropna(subset=["date", "index_level"]).sort_values("date")
    if "index_ret_pct" not in df.columns:
        df["index_ret_pct"] = df["index_level"].pct_change() * 100.0
    return df

def load_index_history_fallback() -> pd.DataFrame:
    df = pd.read_csv(URL_INDEX_HISTORY)
    c_date = _col(df, "date", "날짜")
    c_idx  = _col(df, "bm20_index", "bm_index", "index_level", "지수", "index")
    c_ret  = _col(df, "index_ret_pct", "ret_pct", "chg_pct", "d1", "수익률(%)")
    if not c_date or not c_idx:
        raise ValueError("index_history에 date / index_level(=bm20_index) 컬럼이 필요합니다.")
    df = df[[c_date, c_idx] + ([c_ret] if c_ret else [])].copy()
    df.columns = ["date", "index_level"] + (["index_ret_pct"] if c_ret else [])
    df["date"] = _as_date(df["date"])
    df = df.dropna(subset=["date", "index_level"]).sort_values("date")
    if "index_ret_pct" not in df.columns:
        df["index_ret_pct"] = df["index_level"].pct_change() * 100.0
    return df

def _load_weights_today() -> Optional[pd.DataFrame]:
    for url in [URL_WEIGHTS_DAILY, URL_WEIGHTS_Q]:
        try:
            w = pd.read_csv(url)
            c_sym = _col(w, "symbol", "티커", "코드")
            c_nam = _col(w, "name", "종목명")
            c_wgt = _col(w, "weight", "bm20_weight", "가중치")
            if c_sym and c_wgt:
                out = w[[c_sym] + ([c_nam] if c_nam else []) + [c_wgt]].copy()
                out.columns = ["symbol"] + (["name"] if c_nam else []) + ["weight"]
                if "name" not in out.columns:
                    out["name"] = out["symbol"]
                s = pd.to_numeric(out["weight"], errors="coerce").fillna(0.0)
                if s.max() > 1.0:
                    s = s / 100.0
                total = s.sum()
                if total <= 0:
                    return None
                out["weight"] = s / total
                return out[["symbol","name","weight"]]
        except Exception:
            pass
    return None

def _load_daily_returns_today() -> Optional[pd.DataFrame]:
    try:
        df = pd.read_csv(URL_SUMMARY_PERF)
        c_date = _col(df, "date", "날짜")
        c_sym  = _col(df, "symbol", "티커", "코드")
        c_ret  = _col(df, "ret_pct", "d1", "chg_pct", "return_pct", "수익률(%)")
        if c_date and c_sym and c_ret:
            df = df[[c_date, c_sym, c_ret]].copy()
            df.columns = ["date", "symbol", "ret_pct"]
            df["date"] = _as_date(df["date"])
            df = df[df["date"] == TODAY]
            df["ret_pct"] = pd.to_numeric(df["ret_pct"], errors="coerce")
            df = df.dropna(subset=["symbol", "ret_pct"])
            return df[["symbol","ret_pct"]]
    except Exception:
        pass
    return None

# =============================
# Snapshot & history
# =============================
def get_recent_history(days: int = 60) -> pd.DataFrame:
    try:
        df = load_bm_index_full()
    except Exception:
        df = load_index_history_fallback()
    if days and len(df) > days:
        df = df.tail(days).copy()
    return df[["date","index_level","index_ret_pct"]]

@dataclass
class Bm20Snapshot  # re-declared for type checkers
class Bm20Snapshot:
    pass

def get_today_snapshot() -> Bm20Snapshot:
    try:
        full = load_bm_index_full()
    except Exception:
        full = load_index_history_fallback()
    latest = full.iloc[-1]
    prev   = full.iloc[-2] if len(full) >= 2 else latest

    weights = _load_weights_today()
    rets    = _load_daily_returns_today()

    top_movers_df = None
    contrib_df = None
    if weights is not None and rets is not None and len(rets) > 0:
        contrib_df = weights.merge(rets, on="symbol", how="left")
        contrib_df["ret_pct"] = pd.to_numeric(contrib_df["ret_pct"], errors="coerce").fillna(0.0)
        contrib_df["contrib_bps"] = contrib_df["weight"] * contrib_df["ret_pct"] * 100.0
        top_movers_df = contrib_df[["symbol","name","ret_pct","contrib_bps"]].copy()

    mcap_total = float("nan")
    turnover_usd = float("nan")
    etf_flow_usd = None
    cex_netflow_usd = None

    return Bm20Snapshot(
        date=latest["date"],
        index_level=float(latest["index_level"]),
        index_chg_pct=float(latest["index_ret_pct"]),
        mcap_total=mcap_total,
        turnover_usd=turnover_usd,
        etf_flow_usd=etf_flow_usd,
        cex_netflow_usd=cex_netflow_usd,
        top_movers=top_movers_df,
        contributions=contrib_df
    )

# =============================
# News sentence generator
# =============================
def _fmt_pct(x: float, digits: int = 2) -> str:
    return f"{x:+.{digits}f}%"

def _fmt_usd(x: Optional[float]) -> str:
    if x is None or (isinstance(x, float) and (math.isnan(x) or math.isinf(x))):
        return "자료 없음"
    sign = "-" if x < 0 else ""
    ax = abs(x)
    if ax >= 1e9:
        return f"{sign}{ax/1e9:.2f}억달러"
    elif ax >= 1e6:
        return f"{sign}{ax/1e6:.1f}백만달러"
    else:
        return f"{sign}{ax:,.0f}달러"

def build_news_sentences(s) -> List[str]:
    headline_dir = "하락" if s.index_chg_pct < 0 else ("상승" if s.index_chg_pct > 0 else "보합")
    chg = _fmt_pct(s.index_chg_pct)

    sentences: List[str] = []
    sentences.append(f"BM20 지수 {DATE_STR} {headline_dir}. 전일 대비 {chg}를 기록했다.")

    if not (math.isnan(s.mcap_total) or math.isnan(s.turnover_usd)):
        sentences.append(
            f"시가총액은 약 {s.mcap_total/1e9:.1f}억달러, 24시간 거래대금은 {s.turnover_usd/1e9:.1f}억달러로 집계됐다."
        )

    if s.etf_flow_usd is not None:
        etf_flow_txt = "순유입" if s.etf_flow_usd > 0 else ("순유출" if s.etf_flow_usd < 0 else "변동 미미")
        sentences.append(f"현물 ETF 자금은 {etf_flow_txt}을 보이며 {_fmt_usd(s.etf_flow_usd)} 규모로 추정된다.")

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
# Kimchi premium (placeholder synthetic)
# =============================
def compute_kimchi_premium(btc_krw: pd.Series, btc_usd: pd.Series, krw_usd: pd.Series) -> pd.DataFrame:
    df = pd.concat({"btc_krw": btc_krw, "btc_usd": btc_usd, "krw_usd": krw_usd}, axis=1).sort_index().ffill().dropna()
    fair_krw = df["btc_usd"] * df["krw_usd"]
    prem = (df["btc_krw"] / fair_krw - 1.0) * 100.0
    return pd.DataFrame({"kimchi_premium_pct": prem})

def save_kimchi_premium_chart(prem: pd.DataFrame, out_dir: str) -> Tuple[str, str]:
    png_path = os.path.join(out_dir, f"kimchi_premium_{DATE_STR}.png")
    pdf_path = os.path.join(out_dir, f"kimchi_premium_{DATE_STR}.pdf")
    for path in [png_path, pdf_path]:
        plt.figure(figsize=(9, 4.5))
        plt.plot(prem.index, prem["kimchi_premium_pct"])
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
# Weekly & Monthly reports
# =============================
def build_periodic_summary(history: pd.DataFrame, period: str) -> Tuple[str, pd.DataFrame]:
    df = history.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(KST, nonexistent="shift_forward", ambiguous="NaT").dt.date
    df = df.sort_values("date")

    if period == "W":
        start = (TODAY - dt.timedelta(days=TODAY.weekday()+1))
        end = TODAY - dt.timedelta(days=1)
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
        days = 7 if period == "W" else 30
        window = df.tail(days).copy()
        start, end = window["date"].min(), window["date"].max()
        title = f"BM20 {('주간' if period=='W' else '월간')} 리포트 ({start}~{end})"

    level_start = window["index_level"].iloc[0]
    level_end   = window["index_level"].iloc[-1]
    chg_pct = (level_end/level_start - 1)*100
    vol = window["index_ret_pct"].std() * np.sqrt(max(len(window), 1))

    summary = pd.DataFrame({
        "metric": ["기간 시작", "기간 종료", "지수 변화", "변동성(단순)", "관측 일수"],
        "value":  [f"{level_start:,.2f}", f"{level_end:,.2f}", f"{chg_pct:+.2f}%", f"{vol:.2f}pp", len(window)]
    })
    return title, summary

def save_periodic_report(history: pd.DataFrame, period: str, out_dir: str) -> Tuple[str, str]:
    title, summary = build_periodic_summary(history, period)
    txt_path = os.path.join(out_dir, f"BM20_{'weekly' if period=='W' else 'monthly'}_{DATE_STR}.txt")
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write(title + "\n")
        f.write("-"*len(title) + "\n\n")
        for m, v in summary.itertuples(index=False):
            f.write(f"{m}: {v}\n")

    pdf_path = os.path.join(out_dir, f"BM20_{'weekly' if period=='W' else 'monthly'}_{DATE_STR}.pdf")
    with PdfPages(pdf_path) as pdf:
        plt.figure(figsize=(8.27, 11.69))
        plt.axis('off'); plt.text(0.5, 0.85, title, ha='center', va='center', fontsize=20)
        now = dt.datetime.now(KST).strftime("%Y-%m-%d %H:%M KST")
        plt.text(0.5, 0.80, f"발행: Blockmedia · {now}", ha='center', va='center')
        pdf.savefig(); plt.close()

        plt.figure(figsize=(8.27, 11.69))
        plt.axis('off')
        y = 0.9
        for m, v in summary.itertuples(index=False):
            plt.text(0.1, y, f"• {m}", fontsize=12)
            plt.text(0.6, y, str(v), fontsize=12)
            y -= 0.06
        pdf.savefig(); plt.close()

        plt.figure(figsize=(8.27, 5))
        df = history.copy()
        if period == 'W':
            start = (TODAY - dt.timedelta(days=TODAY.weekday()+1)); end = TODAY - dt.timedelta(days=1)
        else:
            first_day = TODAY.replace(day=1); last_month_end = first_day - dt.timedelta(days=1)
            start = last_month_end.replace(day=1); end = last_month_end
        mask = (pd.to_datetime(df['date']).dt.date >= start) & (pd.to_datetime(df['date']).dt.date <= end)
        win = df.loc[mask]
        if win.empty: win = df.tail(30 if period=='M' else 7)
        plt.plot(pd.to_datetime(win['date']), win['index_level'])
        plt.title("BM20 지수 추이"); plt.xlabel("Date"); plt.ylabel("Index Level"); plt.tight_layout()
        pdf.savefig(); plt.close()

    return txt_path, pdf_path

# =============================
# Upload helper (optional)
# =============================
def upload_if_configured(local_path: str) -> None:
    if not local_path or not DRIVE_FOLDER_ID:
        return
    try:
        from gdrive_uploader import upload_to_gdrive  # type: ignore
        upload_to_gdrive(local_path, DRIVE_FOLDER_ID)
    except Exception as e:
        print(f"[WARN] Upload skipped or failed for {local_path}: {e}")

# =============================
# Main
# =============================
def main():
    snap = get_today_snapshot()
    sentences = build_news_sentences(snap)
    txt_path = save_daily_news_txt(sentences, DAILY_DIR)
    upload_if_configured(txt_path)

    try:
        idx = pd.date_range(TODAY, periods=24*12, freq='5min', tz=KST)
        rng = np.random.default_rng(7)
        btc_usd = pd.Series(60000 + rng.normal(0, 80, len(idx)).cumsum()/10, index=idx)
        krw_usd = pd.Series(1380 + rng.normal(0, 0.5, len(idx)).cumsum()/50, index=idx)
        btc_krw = pd.Series(btc_usd.values * krw_usd.values * (1 + rng.normal(0.02, 0.005, len(idx))), index=idx)
        prem = compute_kimchi_premium(btc_krw, btc_usd, krw_usd)
        png_k, pdf_k = save_kimchi_premium_chart(prem, DAILY_DIR)
        upload_if_configured(png_k); upload_if_configured(pdf_k)
    except Exception as e:
        print(f"[WARN] Kimchi premium step skipped: {e}")

    try:
        hist = load_bm_index_full()
        if dt.datetime.now(KST).weekday() == 0:
            w_txt, w_pdf = save_periodic_report(hist, period='W', out_dir=DAILY_DIR)
            upload_if_configured(w_txt); upload_if_configured(w_pdf)
        if TODAY.day == 1:
            m_txt, m_pdf = save_periodic_report(hist, period='M', out_dir=DAILY_DIR)
            upload_if_configured(m_txt); upload_if_configured(m_pdf)
    except Exception as e:
        print(f"[WARN] Periodic report step skipped: {e}")

    print(f"BM20 daily pipeline completed for {DATE_STR}. Output: {DAILY_DIR}")

if __name__ == "__main__":
    main()
