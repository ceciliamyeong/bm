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
# Ensure news fields (attr guard)
# =============================
import math
from types import SimpleNamespace

def ensure_news_fields(s: SimpleNamespace) -> SimpleNamespace:
    """
    build_news_sentences()가 기대하는 필드를 s에 보장.
    없으면 안전한 기본값으로 채운다.
    """
    defaults = {
        "index_level": None,
        "index_chg": None,
        "index_chg_pct": 0.0,   # 없으면 '보합' 처리
        "mcap_total": float("nan"),
        "turnover_usd": float("nan"),
        "etf_flow_usd": None,
        "top_movers": None,
        "contributions": None,
        "btc_usd": None,
        "eth_usd": None,
        "bm20_over_btc": None,
        "bm20_over_eth": None,
        "date": "",
    }
    for k, v in defaults.items():
        if not hasattr(s, k):
            setattr(s, k, v)
    return s

# =============================
# Snapshot adapter (dict -> attr object)
# =============================
from types import SimpleNamespace
import math
import pandas as pd

def _to_float_or_nan(x):
    try:
        return float(x)
    except Exception:
        return float("nan")

def _to_float_or_none(x):
    try:
        return float(x)
    except Exception:
        return None

def _df_or_none(obj, expected_cols=None):
    """
    obj가 list[dict] 또는 dict의 리스트 형태라면 DataFrame으로, 아니면 None.
    expected_cols가 주어지면 해당 컬럼이 없더라도 일단 DataFrame 생성 후 진행.
    """
    try:
        if obj is None:
            return None
        if isinstance(obj, pd.DataFrame):
            return obj
        if isinstance(obj, list):
            df = pd.DataFrame(obj)
            return df
        # dict인 경우 values가 list라면 시도
        if isinstance(obj, dict):
            # 가장 '길이 있는' 값 후보를 고름
            for v in obj.values():
                if isinstance(v, list) and len(v) > 0 and isinstance(v[0], dict):
                    return pd.DataFrame(v)
        return None
    except Exception:
        return None

def coerce_snapshot_to_attr(snap: dict) -> SimpleNamespace:
    """
    build_news_sentences()가 기대하는 속성:
      - index_chg_pct (float)
      - index_level (float, optional)
      - index_chg (float, optional)
      - mcap_total (float, NaN 허용)
      - turnover_usd (float, NaN 허용)
      - etf_flow_usd (float or None)
      - top_movers (DataFrame or None)  # columns: name, symbol, ret_pct, contrib_bps
      - contributions (DataFrame or None)  # columns: symbol, weight
      - date (str)
    누락되면 안전한 기본값을 채움.
    """
    bm20 = (snap or {}).get("bm20", {}) if isinstance(snap, dict) else {}

    # BM20 지수 변화
    index_level   = _to_float_or_none(bm20.get("index_level"))
    index_chg     = _to_float_or_none(bm20.get("d1_change"))
    index_chg_pct = _to_float_or_none(bm20.get("d1_change_pct"))
    if index_chg_pct is None:
        # 기본 0.0이면 상승/하락 판정에서 '보합'으로 처리됨
        index_chg_pct = 0.0

    # 시총/거래대금 (없으면 NaN으로 채워 math.isnan 체크에 안전)
    mcap_total  = _to_float_or_nan(snap.get("mcap_total"))
    turnover_usd = _to_float_or_nan(snap.get("turnover_usd"))

    # ETF 자금 유입/유출 (없으면 None → 문장 생략)
    etf_flow_usd = _to_float_or_none(snap.get("etf_flow_usd"))

    # BTC/ETH (있으면 사용, 없어도 build_news_sentences는 사용 안해도 됨)
    btc_usd = _to_float_or_none(snap.get("btc_usd"))
    eth_usd = _to_float_or_none(snap.get("eth_usd"))

    # 상대지수(있으면 사용)
    bm20_over_btc = _to_float_or_none(snap.get("bm20_over_btc"))
    bm20_over_eth = _to_float_or_none(snap.get("bm20_over_eth"))

    # Top movers / contributions (list[dict] 또는 DF 예상)
    top_movers = _df_or_none(snap.get("top_movers"))
    contributions = _df_or_none(snap.get("contributions"))

    # 날짜
    date_str = str(snap.get("date") or "")

    return SimpleNamespace(
        index_level=index_level,
        index_chg=index_chg,
        index_chg_pct=index_chg_pct,
        mcap_total=mcap_total,
        turnover_usd=turnover_usd,
        etf_flow_usd=etf_flow_usd,
        btc_usd=btc_usd,
        eth_usd=eth_usd,
        bm20_over_btc=bm20_over_btc,
        bm20_over_eth=bm20_over_eth,
        top_movers=top_movers,
        contributions=contributions,
        date=date_str,
    )


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

# === Yahoo Finance 기반: 종목별 1D 수익률 생성 ===
# - weights CSV(또는 URL)에서 Top20 심볼을 읽고
# - yfinance로 최근 2영업일 종가 가져와 (오늘/어제 - 1) 계산
# - 반환: DataFrame[symbol, ret_pct]
import pandas as pd
import numpy as np
import datetime as dt
import yfinance as yf

# 선택: weights CSV URL이 있다면 지정 (예: 구글시트 publish to CSV)
# 없으면 None 두세요. 그럼 로컬 weights.csv(있으면) 또는 실패 시 None 반환.
URL_WEIGHTS = globals().get("URL_WEIGHTS", None)

def _get_symbols_top20_from_weights(url_weights: str | None) -> list[str] | None:
    try:
        if url_weights:
            w = pd.read_csv(url_weights)
        else:
            try:
                w = pd.read_csv("weights.csv")  # 레포에 있으면 사용
            except Exception:
                return None
        cols = {c.strip().lower(): c for c in w.columns}
        if "symbol" not in cols:
            return None
        sym_col = cols["symbol"]
        w[sym_col] = w[sym_col].astype(str).str.upper().str.strip()
        if "weight" in cols:
            w["__w__"] = pd.to_numeric(w[cols["weight"]], errors="coerce")
            w = w.sort_values("__w__", ascending=False)
        return w[sym_col].dropna().astype(str).str.upper().str.strip().head(20).tolist()
    except Exception:
        return None

def _map_to_yf(symbols: list[str]) -> dict[str, str]:
    # 심플 룰: SYMBOL-USD
    m = {}
    for s in (symbols or []):
        s = str(s).upper().strip()
        if not s:
            continue
        m[s] = f"{s}-USD"
    return m

def _download_last2_closes_yf(tickers: list[str]) -> pd.DataFrame:
    if not tickers:
        return pd.DataFrame(columns=["yf_ticker","date","close"])
    now = pd.Timestamp.utcnow()
    start = now - pd.Timedelta(days=10)
    end   = now + pd.Timedelta(days=1)
    data = yf.download(
        tickers=" ".join(tickers),
        start=start.strftime("%Y-%m-%d"),
        end=end.strftime("%Y-%m-%d"),
        interval="1d",
        group_by="ticker",
        auto_adjust=False,
        threads=False,
        progress=False,
    )
    rows = []
    if isinstance(data.columns, pd.MultiIndex):
        for tkr in tickers:
            try:
                closes = data[(tkr, "Close")].dropna().sort_index()
                if len(closes) >= 2:
                    rows.append(pd.DataFrame({"yf_ticker": tkr, "date": closes.index, "close": closes.values}))
            except Exception:
                continue
    else:
        # 단일 티커 케이스
        closes = data["Close"].dropna().sort_index()
        if len(closes) >= 2:
            rows.append(pd.DataFrame({"yf_ticker": tickers[0], "date": closes.index, "close": closes.values}))
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(columns=["yf_ticker","date","close"])

def _load_daily_returns_today() -> pd.DataFrame | None:
    """
    Yahoo Finance로 종목별 일일 수익률을 계산해 반환.
    실패/데이터 없음 시 None 반환 (리포트 쪽에서 안전 스킵).
    """
    try:
        symbols = _get_symbols_top20_from_weights(URL_WEIGHTS)
        if not symbols:
            return None
        yf_map = _map_to_yf(symbols)
        tickers = list(dict.fromkeys(yf_map.values()))  # unique
        raw = _download_last2_closes_yf(tickers)
        if raw.empty:
            return None

        raw = raw.sort_values(["yf_ticker","date"])
        last2 = raw.groupby("yf_ticker").tail(2).copy()
        last2["rn"] = last2.groupby("yf_ticker").cumcount()
        piv = last2.pivot(index="yf_ticker", columns="rn", values="close").rename(columns={0:"prev",1:"cur"})
        piv = piv.dropna(subset=["prev","cur"]).reset_index()
        piv["ret_pct"] = piv["cur"] / piv["prev"] - 1.0

        inv = {v:k for k,v in yf_map.items()}  # yf_ticker -> symbol
        piv["symbol"] = piv["yf_ticker"].map(inv)
        out = piv[["symbol","ret_pct"]].dropna()
        out["symbol"] = out["symbol"].astype(str).str.upper().str.strip()
        out = out[out["symbol"].isin(symbols)].copy()  # Top20만
        if out.empty:
            return None
        # 소수점 자리수 약간 제한
        out["ret_pct"] = pd.to_numeric(out["ret_pct"], errors="coerce").round(6)
        return out[["symbol","ret_pct"]]
    except Exception:
        return None
 




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
    idx_pct = getattr(s, "index_chg_pct", 0.0) or 0.0
    headline_dir = "하락" if idx_pct < 0 else ("상승" if idx_pct > 0 else "보합")
    chg = _fmt_pct(idx_pct)

    sentences: List[str] = []
    sentences.append(f"BM20 지수 {DATE_STR} {headline_dir}. 전일 대비 {chg}를 기록했다.")

    mcap_total = getattr(s, "mcap_total", float("nan"))
    turnover_usd = getattr(s, "turnover_usd", float("nan"))
    if not (math.isnan(mcap_total) or math.isnan(turnover_usd)):
        sentences.append(
            f"시가총액은 약 {mcap_total/1e9:.1f}억달러, 24시간 거래대금은 {turnover_usd/1e9:.1f}억달러로 집계됐다."
        )

    etf_flow = getattr(s, "etf_flow_usd", None)
    if etf_flow is not None:
        etf_flow_txt = "순유입" if etf_flow > 0 else ("순유출" if etf_flow < 0 else "변동 미미")
        sentences.append(f"현물 ETF 자금은 {etf_flow_txt}을 보이며 {_fmt_usd(etf_flow)} 규모로 추정된다.")

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
# Snapshot adapter (dict -> attr)
# =============================
from types import SimpleNamespace

def _to_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default

def coerce_snapshot_to_attr(snap: dict) -> SimpleNamespace:
    """
    build_news_sentences()가 s.index_chg_pct 같은 속성 접근을 쓰는 경우를 대비해
    dict 스냅샷을 속성 객체로 변환한다.
    기대 속성(추정):
      - index_level, index_chg, index_chg_pct
      - btc_usd, eth_usd
      - bm20_over_btc, bm20_over_eth
      - date
    누락 시 안전한 기본값(0.0 또는 '' )을 채운다.
    """
    bm20 = snap.get("bm20", {}) if isinstance(snap, dict) else {}
    return SimpleNamespace(
        # BM20 지수 수준/변화
        index_level   = _to_float(bm20.get("index_level")),
        index_chg     = _to_float(bm20.get("d1_change")),
        index_chg_pct = _to_float(bm20.get("d1_change_pct")),

        # BTC/ETH USD
        btc_usd = _to_float(snap.get("btc_usd")),
        eth_usd = _to_float(snap.get("eth_usd")),

        # 상대지수(있으면)
        bm20_over_btc = _to_float(snap.get("bm20_over_btc")),
        bm20_over_eth = _to_float(snap.get("bm20_over_eth")),

        # 날짜
        date = str(snap.get("date") or ""),
    )



# =============================
# Snapshot builder (drop-in)
# =============================
import os, json, datetime as dt
from pathlib import Path

def _read_json_first(paths):
    """여러 경로 후보 중 존재하는 첫 JSON을 로드."""
    for p in paths:
        try:
            pp = Path(p)
            if pp.is_file():
                with open(pp, "r", encoding="utf-8") as f:
                    return json.load(f)
        except Exception as e:
            print(f"[WARN] JSON read failed: {p} -> {e}")
    return None

def _load_bm20_from_history(csv_path="out/history/bm20_index_history.csv"):
    """히스토리 CSV에서 최신값/전일비를 계산 (폴백)."""
    import pandas as pd
    try:
        df = pd.read_csv(csv_path)
        # 컬럼 정규화
        cols = {c.lower(): c for c in df.columns}
        date_col = next(c for c in df.columns if c.lower() == "date")
        idx_col  = next(c for c in df.columns if c.lower() in ("index","level","bm20_index","bm20","index_level"))
        df = df[[date_col, idx_col]].rename(columns={date_col:"date", idx_col:"index_level"})
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date").reset_index(drop=True)
        last = df.iloc[-1]
        prev = df.iloc[-2] if len(df) >= 2 else last
        level = float(last["index_level"])
        prev_level = float(prev["index_level"]) if float(prev["index_level"]) != 0 else level
        chg = level - prev_level
        chg_pct = (chg / prev_level * 100.0) if prev_level != 0 else 0.0
        return {
            "date": str(last["date"].date()),
            "bm20": {"index_level": level, "d1_change": chg, "d1_change_pct": chg_pct}
        }
    except Exception as e:
        print(f"[WARN] load_bm20_from_history failed: {e}")
        return None

def _safe_float(x, default=None):
    try:
        return float(x)
    except Exception:
        return default

def _try_fetch_btc_eth_yf():
    """yfinance가 설치되어 있으면 BTC/ETH 종가(또는 최근가) 폴백 로드."""
    try:
        import yfinance as yf
        # 최근 2일치 받았다가 마지막 유효값 사용
        btc = yf.Ticker("BTC-USD").history(period="5d", interval="1d")["Close"]
        eth = yf.Ticker("ETH-USD").history(period="5d", interval="1d")["Close"]
        btc_v = float(btc.dropna().iloc[-1]) if len(btc.dropna()) else None
        eth_v = float(eth.dropna().iloc[-1]) if len(eth.dropna()) else None
        return {"btc_usd": btc_v, "eth_usd": eth_v}
    except Exception as e:
        print(f"[WARN] yfinance fetch failed: {e}")
        return {"btc_usd": None, "eth_usd": None}

def get_today_snapshot():
    """
    가능한 소스에서 오늘 스냅샷을 구성:
    1) docs/latest.json 또는 out/latest/latest.json 계열
    2) out/history/bm20_index_history.csv 폴백
    3) BTC/ETH는 latest.json → 실패 시 yfinance 폴백
    """
    # 1) 후보 JSON 로드
    latest = _read_json_first([
        "docs/latest.json",
        "docs/bm20_latest.json",
        "site/latest.json",
        "site/bm20_latest.json",
        "out/latest/latest.json",
        "out/latest/bm20_latest.json",
        "latest.json",
        "bm20_latest.json",
    ])

    snap = {
        "date": dt.datetime.now(KST).date().isoformat(),
        "bm20": {"index_level": None, "d1_change": None, "d1_change_pct": None},
        "btc_usd": None,
        "eth_usd": None,
        # 선택 필드 (있으면 build_news_sentences가 활용 가능)
        "bm20_over_btc": None,
        "bm20_over_eth": None,
    }

    # 2) latest.json에서 추출 시도
    if isinstance(latest, dict):
        # 흔한 키들 최대한 포괄
        # BM20 level
        lvl = (latest.get("bm20_level") or latest.get("index_level") or
               latest.get("bm20", {}).get("index_level"))
        chg = (latest.get("bm20_d1_change") or latest.get("d1_change") or
               latest.get("bm20", {}).get("d1_change"))
        chg_pct = (latest.get("bm20_d1_change_pct") or latest.get("d1_change_pct") or
                   latest.get("bm20", {}).get("d1_change_pct"))
        snap["bm20"]["index_level"] = _safe_float(lvl)
        snap["bm20"]["d1_change"] = _safe_float(chg)
        snap["bm20"]["d1_change_pct"] = _safe_float(chg_pct)

        # BTC/ETH 가격
        btc = (latest.get("btc_usd") or latest.get("btc", {}).get("usd") or latest.get("btc_price_usd"))
        eth = (latest.get("eth_usd") or latest.get("eth", {}).get("usd") or latest.get("eth_price_usd"))
        snap["btc_usd"] = _safe_float(btc)
        snap["eth_usd"] = _safe_float(eth)

        # 상대지수(있으면)
        snap["bm20_over_btc"] = _safe_float(latest.get("bm20_over_btc"))
        snap["bm20_over_eth"] = _safe_float(latest.get("bm20_over_eth"))

        # 날짜
        date_key = (latest.get("date") or latest.get("asof") or latest.get("as_of"))
        if date_key:
            try:
                snap["date"] = str(pd.to_datetime(date_key).date())
            except Exception:
                pass

    # 3) BM20이 비어 있으면 히스토리 폴백
    if snap["bm20"]["index_level"] is None:
        hist_fallback = _load_bm20_from_history()
        if hist_fallback:
            snap["date"] = hist_fallback["date"]
            snap["bm20"] = hist_fallback["bm20"]

    # 4) BTC/ETH가 비어 있으면 yfinance 폴백
    if snap["btc_usd"] is None or snap["eth_usd"] is None:
        fx = _try_fetch_btc_eth_yf()
        snap["btc_usd"] = snap["btc_usd"] or fx["btc_usd"]
        snap["eth_usd"] = snap["eth_usd"] or fx["eth_usd"]

    return snap



# =============================
# Main
# =============================
def main():
    snap = get_today_snapshot()          # dict 반환
    s = coerce_snapshot_to_attr(snap)    # ✅ dict → 속성 객체로 변환
    sentences = build_news_sentences(s)  # ✅ 속성 접근 사용
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
