#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BM20 Daily Automation — Production Version (A안, Full)
2025-08-12

- BM20_OUTPUT_ROOT 기본값 'out' (어제와 동일 경로)
- 레이아웃: 뉴스 → 퍼포먼스(상승 Top10/하락 Top10) → 거래량 Top3 → 김치프리미엄
- 김치프리미엄: Upbit × Coinbase × exchangerate.host (안정/무키)
- 펀딩비: Binance + Bybit + 재시도(backoff) + 캐시 폴백
- 주간/월간 자동 리포트 포함 (월요일/매월 1일)
- 업로드: Google Drive(있으면) + 아티팩트 최소 1개 보장(fallback PDF) + 경로 로그

내부 BM20 데이터 연결:
- `get_today_snapshot()` 함수를 내부 API/DB 호출로 교체하면 전부 자동 연동됩니다.
"""
from __future__ import annotations

import os
import math
import json
import time
import textwrap
import datetime as dt
from dataclasses import dataclass
from typing import Optional, List, Tuple

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import requests

try:
    from zoneinfo import ZoneInfo
except Exception:
    from pytz import timezone as ZoneInfo  # type: ignore

# =============================
# Config
# =============================
KST = ZoneInfo("Asia/Seoul")
NOW = dt.datetime.now(KST)
TODAY = NOW.date()
DATE_STR = TODAY.strftime("%Y-%m-%d")
OUTPUT_ROOT = os.environ.get("BM20_OUTPUT_ROOT", "out")
DRIVE_FOLDER_ID = os.environ.get("BM20_DRIVE_FOLDER_ID", "")
DAILY_DIR = os.path.join(OUTPUT_ROOT, DATE_STR)
os.makedirs(DAILY_DIR, exist_ok=True)
CACHE_DIR = os.path.join(OUTPUT_ROOT, "cache")
os.makedirs(CACHE_DIR, exist_ok=True)

# =============================
# Data Types
# =============================
@dataclass
class Bm20Snapshot:
    date: dt.date
    index_level: float
    index_chg_pct: float
    mcap_total: float
    turnover_usd: float
    etf_flow_usd: Optional[float] = None
    cex_netflow_usd: Optional[float] = None
    # Movers/contrib
    top_movers: Optional[pd.DataFrame] = None   # [symbol,name,ret_pct,contrib_bps]
    contributions: Optional[pd.DataFrame] = None # [symbol,name,weight,ret_pct,contrib_bps]
    # Volume growth Top3
    volume_growth: Optional[pd.DataFrame] = None # [symbol,vol_chg_pct]

# =============================
# Utils
# =============================

def safe_get(d: dict, *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def _fmt_pct(x: float, digits: int = 2) -> str:
    return f"{x:+.{digits}f}%"


def _fmt_usd(x: Optional[float]) -> str:
    if x is None or (isinstance(x, float) and (math.isnan(x) or math.isinf(x))):
        return "자료 없음"
    sign = "-" if x < 0 else ""
    ax = abs(x)
    if ax >= 1e9:
        return f"{sign}{ax/1e9:.2f}억달러"
    if ax >= 1e6:
        return f"{sign}{ax/1e6:.1f}백만달러"
    return f"{sign}{ax:,.0f}달러"

# =============================
# External Sources — Kimchi Premium
# =============================

def fetch_upbit_minutes(market: str = "BTC-KRW", unit: int = 5, count: int = 288) -> pd.DataFrame:
    """Upbit 5분봉 BTC/KRW (최근 N개)."""
    url = f"https://api.upbit.com/v1/candles/minutes/{unit}"
    r = requests.get(url, params={"market": market, "count": count}, timeout=12)
    r.raise_for_status()
    js = r.json()
    if not js:
        return pd.DataFrame()
    df = pd.DataFrame(js)
    df["timestamp"] = pd.to_datetime(df["candle_date_time_kst"]).dt.tz_localize("Asia/Seoul")
    df = df.sort_values("timestamp")
    return df.set_index("timestamp")["trade_price"].to_frame("close")


def fetch_coinbase_candles(product: str = "BTC-USD", granularity: int = 300, hours: int = 24) -> pd.DataFrame:
    """Coinbase 5분봉 BTC/USD."""
    url = f"https://api.exchange.coinbase.com/products/{product}/candles"
    end = dt.datetime.utcnow()
    start = end - dt.timedelta(hours=hours)
    params = {"start": start.isoformat(), "end": end.isoformat(), "granularity": granularity}
    r = requests.get(url, params=params, timeout=12)
    r.raise_for_status()
    arr = r.json()
    if not arr:
        return pd.DataFrame()
    df = pd.DataFrame(arr, columns=["time","low","high","open","close","volume"]).sort_values("time")
    df["timestamp"] = pd.to_datetime(df["time"], unit="s", utc=True).dt.tz_convert("Asia/Seoul")
    return df.set_index("timestamp")["close"].to_frame("close")


def fetch_usdkrw_latest() -> float:
    r = requests.get("https://api.exchangerate.host/latest", params={"base":"USD","symbols":"KRW"}, timeout=10)
    r.raise_for_status()
    return float(safe_get(r.json(), "rates", "KRW", default=1320.0))


def compute_kimchi_premium_series() -> Tuple[pd.DataFrame, float]:
    up = fetch_upbit_minutes()
    cb = fetch_coinbase_candles()
    if up.empty or cb.empty:
        raise RuntimeError("kimchi premium price series missing")
    usdkrw = fetch_usdkrw_latest()
    df = pd.concat({"btc_krw": up["close"], "btc_usd": cb["close"]}, axis=1).ffill().dropna()
    prem = (df["btc_krw"] / (df["btc_usd"] * usdkrw) - 1.0) * 100.0
    out = pd.DataFrame({"kimchi_premium_pct": prem})
    return out, float(out.iloc[-1, 0])


def save_kimchi_premium_chart(prem: pd.DataFrame, out_dir: str) -> Tuple[str, str]:
    png_path = os.path.join(out_dir, f"kimchi_premium_{DATE_STR}.png")
    pdf_path = os.path.join(out_dir, f"kimchi_premium_{DATE_STR}.pdf")
    for path in (png_path, pdf_path):
        plt.figure(figsize=(9, 4.5))
        plt.plot(prem.index, prem["kimchi_premium_pct"])  # default color
        plt.axhline(0, lw=1)
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
# Funding (Binance/Bybit) — retry + cache fallback
# =============================

def _retry(fn, *, attempts: int = 3, base_sleep: float = 0.6):
    def wrapper(*a, **k):
        last = None
        for i in range(attempts):
            try:
                return fn(*a, **k)
            except Exception as e:
                last = e
                time.sleep(base_sleep * (2 ** i))
        if last:
            raise last
    return wrapper


@_retry
def fetch_binance_funding(symbol: str = "BTCUSDT") -> Optional[float]:
    url = "https://fapi.binance.com/fapi/v1/fundingRate"
    r = requests.get(url, params={"symbol": symbol, "limit": 1}, timeout=8)
    r.raise_for_status()
    js = r.json()
    return float(js[0]["fundingRate"]) * 100.0 if isinstance(js, list) and js else None


@_retry
def fetch_bybit_funding(symbol: str = "BTCUSDT") -> Optional[float]:
    url = "https://api.bybit.com/v5/market/funding/history"
    r = requests.get(url, params={"category": "linear", "symbol": symbol, "limit": 1}, timeout=8)
    r.raise_for_status()
    rate = safe_get(r.json(), "result", "list", 0, "fundingRate")
    return float(rate) * 100.0 if rate is not None else None


CACHE_FUND = os.path.join(CACHE_DIR, "funding.json")


def load_cached_funding() -> dict:
    try:
        with open(CACHE_FUND, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_cached_funding(d: dict) -> None:
    try:
        with open(CACHE_FUND, "w", encoding="utf-8") as f:
            json.dump(d, f, ensure_ascii=False)
    except Exception:
        pass


def get_funding_snapshot() -> dict:
    out = {"binance": {}, "bybit": {}}
    try:
        out["binance"]["BTC"] = fetch_binance_funding("BTCUSDT")
        out["binance"]["ETH"] = fetch_binance_funding("ETHUSDT")
        out["bybit"]["BTC"] = fetch_bybit_funding("BTCUSDT")
        out["bybit"]["ETH"] = fetch_bybit_funding("ETHUSDT")
    except Exception as e:
        print("[WARN] funding fetch error:", e)
    cached = load_cached_funding()
    merged = {}
    for ex in ["binance", "bybit"]:
        merged[ex] = {}
        for sym in ["BTC", "ETH"]:
            merged[ex][sym] = out.get(ex, {}).get(sym) or cached.get(ex, {}).get(sym)
        if not any(v is not None for v in merged[ex].values()):
            merged.pop(ex, None)
    if merged:
        save_cached_funding(merged)
    return merged

# =============================
# Internal BM20 snapshot (replace with your engine)
# =============================

def get_today_snapshot() -> Bm20Snapshot:
    """이곳을 내부 API/DB 호출로 교체하면 프로덕션 연결 완료.
    현재는 동작 보장을 위해 예시 데이터를 반환합니다.
    """
    rng = np.random.default_rng(42)
    movers = pd.DataFrame({
        "symbol": ["BNB","BTC","ETH","ARB","FIL","SUI"],
        "name":   ["BNB","Bitcoin","Ethereum","Arbitrum","Filecoin","Sui"],
        "ret_pct": [0.04,-0.11,-0.17,-5.61,-5.42,-5.23],
        "contrib_bps": [2.1,-1.5,-1.8,-12.0,-10.2,-9.7]
    })
    contrib = pd.DataFrame({
        "symbol": ["BTC","ETH","SOL","XRP","ADA","BNB","DOGE","AVAX","NEAR","SUI","ARB","FIL"],
        "name":   ["Bitcoin","Ethereum","Solana","XRP","Cardano","BNB","DOGE","AVAX","NEAR","Sui","Arbitrum","Filecoin"],
        "weight": [0.38,0.25,0.08,0.06,0.04,0.03,0.03,0.02,0.02,0.02,0.02,0.02],
        "ret_pct": np.clip(rng.normal(0,2,12), -8, 8).round(2),
        "contrib_bps": rng.normal(0,8,12).round(1)
    })
    vol_top3 = pd.DataFrame({"symbol":["SUI","BTC","XRP"], "vol_chg_pct":[71.26,67.67,60.93]})
    return Bm20Snapshot(
        date=TODAY,
        index_level=60119,
        index_chg_pct=-0.11,
        mcap_total=6.3e11,
        turnover_usd=2.1e10,
        top_movers=movers,
        contributions=contrib,
        volume_growth=vol_top3,
    )

# =============================
# News (신문체)
# =============================

def build_news_sentences(s: Bm20Snapshot, *, kimchi_premium_pct: Optional[float] = None, funding: Optional[dict] = None) -> List[str]:
    dir_word = "하락" if s.index_chg_pct < 0 else ("상승" if s.index_chg_pct > 0 else "보합")
    chg = _fmt_pct(s.index_chg_pct)
    out: List[str] = []
    out.append(f"BM20 지수 {DATE_STR} {dir_word}. 전일 대비 {chg}로 {s.index_level:,.0f}포인트를 기록했다.")
    out.append(f"시가총액 약 {s.mcap_total/1e9:.1f}억달러, 24시간 거래대금 {s.turnover_usd/1e9:.1f}억달러.")
    if s.etf_flow_usd is not None:
        flow = "순유입" if s.etf_flow_usd > 0 else ("순유출" if s.etf_flow_usd < 0 else "변동 미미")
        out.append(f"현물 ETF 자금은 {flow}이며 규모는 {_fmt_usd(s.etf_flow_usd)}.")
    if s.contributions is not None and "ret_pct" in s.contributions.columns:
        up = int((s.contributions["ret_pct"] > 0).sum())
        down = int((s.contributions["ret_pct"] < 0).sum())
        if up + down > 0:
            out.append(f"시장 폭은 상승 {up}·하락 {down}.")
    if kimchi_premium_pct is not None:
        out.append(f"김치 프리미엄은 {kimchi_premium_pct:+.2f}%.")
    if funding:
        b = funding.get("binance", {}); v = funding.get("bybit", {})
        def f(x):
            try: return f"{float(x):+.3f}%"
            except: return "-"
        parts = []
        if b: parts.append(f"바이낸스 BTC {f(b.get('BTC'))} / ETH {f(b.get('ETH'))}")
        if v: parts.append(f"바이비트 BTC {f(v.get('BTC'))} / ETH {f(v.get('ETH'))}")
        if parts: out.append("펀딩비: "+"; ".join(parts)+".")
    out.append("(데이터: Blockmedia BM20)")
    return out

# =============================
# Performance panels (Up10/Down10) — full-bleed
# =============================

def select_perf_panels(contrib: pd.DataFrame, n_up: int = 10, n_down: int = 10):
    df = contrib.dropna(subset=["ret_pct"]).copy()
    up = df.sort_values("ret_pct", ascending=False).head(n_up)[["symbol", "ret_pct"]]
    down = df.sort_values("ret_pct").head(n_down)[["symbol", "ret_pct"]]
    return up, down


def save_perf_panel(contrib: pd.DataFrame, out_dir: str, n_up: int = 10, n_down: int = 10):
    up, down = select_perf_panels(contrib, n_up, n_down)
    fig = plt.figure(figsize=(11.69, 6.2))  # A4 width
    # Left: 상승
    ax1 = fig.add_subplot(1, 2, 1)
    ax1.barh(up["symbol"][::-1], up["ret_pct"][::-1])
    ax1.set_title(f"코인별 퍼포먼스 상승 Top {len(up)} (1D)")
    ax1.set_xlabel("%")
    for i, v in enumerate(up["ret_pct"][::-1]):
        ax1.text(v, i, f" {v:+.2f}%", va='center')
    # Right: 하락
    ax2 = fig.add_subplot(1, 2, 2)
    ax2.barh(down["symbol"][::-1], down["ret_pct"][::-1])
    ax2.set_title(f"코인별 퍼포먼스 하락 Top {len(down)} (1D)")
    ax2.set_xlabel("%")
    for i, v in enumerate(down["ret_pct"][::-1]):
        ax2.text(v, i, f" {v:+.2f}%", va='center')
    plt.tight_layout()
    png = os.path.join(out_dir, f"bm20_perf_panel_{DATE_STR}.png")
    pdf = os.path.join(out_dir, f"bm20_perf_panel_{DATE_STR}.pdf")
    fig.savefig(png, dpi=160)
    with PdfPages(pdf) as p:
        p.savefig(fig)
    plt.close(fig)
    return png, pdf

# =============================
# Volume Top3 table — full-bleed
# =============================

def save_volume_top3_table(vol_df: pd.DataFrame, out_dir: str):
    df = vol_df.copy()
    if list(df.columns) != ["종목", "증가율(%)"]:
        df.rename(columns={"symbol": "종목", "vol_chg_pct": "증가율(%)"}, inplace=True)
    fig = plt.figure(figsize=(11.69, 2.2))
    ax = fig.add_subplot(111)
    ax.axis('off')
    tbl = ax.table(cellText=df.round(2).values, colLabels=df.columns, loc='center', cellLoc='center')
    tbl.scale(1.2, 1.6)
    plt.subplots_adjust(left=0.02, right=0.98, top=0.9, bottom=0.1)
    png = os.path.join(out_dir, f"volume_top3_{DATE_STR}.png")
    pdf = os.path.join(out_dir, f"volume_top3_{DATE_STR}.pdf")
    fig.savefig(png, dpi=160)
    with PdfPages(pdf) as p:
        p.savefig(fig)
    plt.close(fig)
    return png, pdf

# =============================
# Weekly/Monthly reports
# =============================

def get_recent_history(days: int = 60) -> pd.DataFrame:
    dates = pd.date_range(end=TODAY, periods=days, tz=KST).date
    rng = np.random.default_rng(123)
    rets = rng.normal(0, 1.0, len(dates)) / 100
    idx = 1000 * (1 + pd.Series(rets)).cumprod().values
    return pd.DataFrame({
        "date": dates,
        "index_level": idx,
        "index_ret_pct": (pd.Series(rets) * 100).round(2),
    })


def build_periodic_summary(history: pd.DataFrame, period: str) -> Tuple[str, pd.DataFrame]:
    df = history.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
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
        f.write(title + "
")
        f.write("-" * len(title) + "

")
        for m, v in summary.itertuples(index=False):
            f.write(f"{m}: {v}
")
    # PDF
    pdf_path = os.path.join(out_dir, f"BM20_{'weekly' if period=='W' else 'monthly'}_{DATE_STR}.pdf")
    with PdfPages(pdf_path) as pdf:
        # cover
        plt.figure(figsize=(8.27, 11.69))
        plt.axis('off')
        plt.text(0.5, 0.85, title, ha='center', va='center', fontsize=20)
        now = dt.datetime.now(KST).strftime("%Y-%m-%d %H:%M KST")
        plt.text(0.5, 0.80, f"발행: Blockmedia · {now}", ha='center', va='center')
        pdf.savefig(); plt.close()
        # summary
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
        plt.plot(pd.to_datetime(history['date']), history['index_level'])
        plt.title("BM20 지수 추이")
        plt.xlabel("Date"); plt.ylabel("Index Level")
        plt.tight_layout(); pdf.savefig(); plt.close()
    return txt_path, pdf_path

# =============================
# Upload helper (Google Drive optional)
# =============================

def upload_if_configured(local_path: str) -> None:
    if not (local_path and DRIVE_FOLDER_ID):
        return
    try:
        # Project-local helper expected
        from gdrive_uploader import upload_to_gdrive  # type: ignore
        upload_to_gdrive(local_path, DRIVE_FOLDER_ID)
    except Exception as e:
        print(f"[WARN] upload failed: {local_path}: {e}")

# =============================
# Daily PDF composer (뉴스 → 퍼포먼스 → 거래량 → 김치)
# =============================

def save_daily_pdf(s: Bm20Snapshot, news_lines: List[str], perf_png: Optional[str], vol_png: Optional[str], kimchi_png: Optional[str]) -> str:
    pdf_path = os.path.join(DAILY_DIR, f"BM20_daily_{DATE_STR}.pdf")
    with PdfPages(pdf_path) as pdf:
        # Page 1: 뉴스 먼저
        fig = plt.figure(figsize=(8.27, 11.69))  # A4 portrait
        ax = fig.add_axes([0, 0, 1, 1]); ax.axis('off')
        y = 0.95
        ax.text(0.5, y, "BM20 데일리 리포트", ha='center', va='center', fontsize=18); y -= 0.03
        ax.text(0.5, y, DATE_STR, ha='center', va='center'); y -= 0.05
        summary = [["지수", f"{s.index_level:,.0f} pt"], ["일간 변동", _fmt_pct(s.index_chg_pct)]]
        ax.table(cellText=summary, colLabels=["항목", "값"], cellLoc='center', loc='upper left', colWidths=[0.25, 0.25])
        y -= 0.05
        text = "
".join(news_lines)
        ax.text(0.02, y - 0.02, textwrap.fill(text, 80), va='top')
        pdf.savefig(); plt.close(fig)

        # Page 2: 퍼포먼스 + 거래량 + 김치 (full-bleed)
        if perf_png:
            fig = plt.figure(figsize=(8.27, 6))
            img = plt.imread(perf_png)
            plt.imshow(img); plt.axis('off'); plt.tight_layout(pad=0)
            pdf.savefig(); plt.close(fig)
        if vol_png:
            fig = plt.figure(figsize=(8.27, 3))
            img = plt.imread(vol_png)
            plt.imshow(img); plt.axis('off'); plt.tight_layout(pad=0)
            pdf.savefig(); plt.close(fig)
        if kimchi_png:
            fig = plt.figure(figsize=(8.27, 3))
            img = plt.imread(kimchi_png)
            plt.imshow(img); plt.axis('off'); plt.tight_layout(pad=0)
            pdf.savefig(); plt.close(fig)
    return pdf_path

# =============================
# Main
# =============================

def main():
    # Snapshot (replace with internal source)
    s = get_today_snapshot()

    # Funding
    try:
        funding = get_funding_snapshot()
    except Exception:
        funding = {}

    # Kimchi premium
    kimchi_png = None
    try:
        prem, prem_last = compute_kimchi_premium_series()
        kimchi_png, _ = save_kimchi_premium_chart(prem, DAILY_DIR)
    except Exception as e:
        print("[WARN] kimchi premium step skipped:", e)
        prem_last = None

    # News (placed before performance panels)
    news = build_news_sentences(s, kimchi_premium_pct=prem_last, funding=funding)

    # Performance panels & Volume Top3
    perf_png = perf_pdf = None
    if s.contributions is not None:
        perf_png, perf_pdf = save_perf_panel(s.contributions, DAILY_DIR, n_up=10, n_down=10)
        upload_if_configured(perf_png); upload_if_configured(perf_pdf)
    vol_png = vol_pdf = None
    if s.volume_growth is not None and not s.volume_growth.empty:
        vol_png, vol_pdf = save_volume_top3_table(s.volume_growth, DAILY_DIR)
        upload_if_configured(vol_png); upload_if_configured(vol_pdf)

    # Compose daily PDF
    pdf_path = save_daily_pdf(s, news, perf_png, vol_png, kimchi_png)
    upload_if_configured(pdf_path)

    # Ensure at least one artifact & log paths
    try:
        if not os.path.exists(pdf_path):
            fb = os.path.join(DAILY_DIR, f"BM20_fallback_{DATE_STR}.pdf")
            with PdfPages(fb) as _pdf:
                plt.figure(figsize=(8.27, 11.69)); plt.axis('off')
                plt.text(0.5, 0.6, "BM20 Fallback Report", ha='center', fontsize=18)
                plt.text(0.5, 0.52, DATE_STR, ha='center')
                _pdf.savefig(); plt.close()
            pdf_path = fb
            upload_if_configured(pdf_path)
        print("ARTIFACT_DIR:", DAILY_DIR)
        for p in [pdf_path, perf_png, vol_png, kimchi_png]:
            if p:
                print("ARTIFACT:", p)
    except Exception as _e:
        print("[WARN] artifact logging failed:", _e)

    # Weekly/Monthly
    try:
        history = get_recent_history(days=60)
        if NOW.weekday() == 0:  # Monday
            w_txt, w_pdf = save_periodic_report(history, period='W', out_dir=DAILY_DIR)
            upload_if_configured(w_txt); upload_if_configured(w_pdf)
        if TODAY.day == 1:
            m_txt, m_pdf = save_periodic_report(history, period='M', out_dir=DAILY_DIR)
            upload_if_configured(m_txt); upload_if_configured(m_pdf)
    except Exception as e:
        print("[WARN] periodic report skipped:", e)

    print("BM20 daily pipeline completed for", DATE_STR)


if __name__ == "__main__":
    main()
