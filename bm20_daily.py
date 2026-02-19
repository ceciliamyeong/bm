#!/usr/bin/env python3
# ===================== BM20 Daily — Yahoo Finance (Final, Blockmedia rules) =====================
# 목적: CoinGecko 없이도 리포트(out/YYYY-MM-DD) 생성. 가중치는
#   BTC 30% / ETH 20% / XRP 5% / USDT 5% / BNB 5% / 나머지 15종 = 35% 균등
# - 가격/등락률/7일 추세: yfinance
# - 김치 프리미엄: Upbit KRW-BTC vs (BTC-USD * USDKRW from exchangerate.host), 폴백 1450
# - 펀딩비: Binance/Bybit (API 실패 시 전일 캐시)
# - 산출물: TXT, CSV, PNGs(bar/trend), PDF, HTML
# - 분기 리밸런싱 훅: 1/4/7/10월 1일 감지 구조 포함(현재 유니버스 고정이면 결과 동일)
# - 지수 기준: 최초 기준일 2018-01-01, 시작점 100 (base/bm20_base.json 캐시)
# 의존: pandas, numpy, requests, matplotlib, reportlab, jinja2, yfinance
# 환경: OUT_DIR(옵션), TZ=Asia/Seoul(권장)

import os, json, time
import datetime as dt
from datetime import datetime, timedelta, timezone
from pathlib import Path
import requests
import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "out"

import yfinance as yf

# ---- Matplotlib ----
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm

# ---- ReportLab ----
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.cidfonts import UnicodeCIDFont
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image

# ---- HTML ---
from jinja2 import Template

# ================================
# Runtime Flags (Dashboard Mode)
# ================================
DASHBOARD_ONLY = os.getenv("BM20_DASHBOARD_ONLY", "0") == "1"
DAILY_SNAPSHOT = os.getenv("BM20_DAILY_SNAPSHOT", "0") == "1"

if DASHBOARD_ONLY:
    print("[MODE] Dashboard-only mode enabled (no PNG/PDF/HTML outputs).")

if DAILY_SNAPSHOT:
    print("[MODE] Daily snapshot enabled (history will be updated).")
else:
    print("[MODE] Intraday run (history will NOT be updated).")


# ================== 공통 설정 ==================
OUT_DIR = Path(os.getenv("OUT_DIR", "out"))
OUT_DIR.mkdir(parents=True, exist_ok=True)
KST = timezone(timedelta(hours=9))
YMD = datetime.now(KST).strftime("%Y-%m-%d")
TS  = datetime.now(KST).strftime("%Y%m%d%H%M%S")
OUT_DIR_DATE = OUT_DIR / YMD
OUT_DIR_DATE.mkdir(parents=True, exist_ok=True)

# Paths
txt_path  = OUT_DIR_DATE / f"bm20_news_{YMD}.txt"
csv_path  = OUT_DIR_DATE / f"bm20_daily_data_{YMD}.csv"
bar_png   = OUT_DIR_DATE / f"bm20_bar_{YMD}.png"
trend_png = OUT_DIR_DATE / f"bm20_trend_{YMD}.png"
pdf_path  = OUT_DIR_DATE / f"bm20_daily_{YMD}.pdf"
html_path = OUT_DIR_DATE / f"bm20_daily_{YMD}.html"
kp_path   = OUT_DIR_DATE / f"kimchi_{YMD}.json"

# ================== Fonts (Nanum 우선, 실패 시 CID) ==================
NANUM_PATH = "/usr/share/fonts/truetype/nanum/NanumGothic.ttf"
KOREAN_FONT = "HYSMyeongJo-Medium"
try:
    if os.path.exists(NANUM_PATH):
        pdfmetrics.registerFont(TTFont("NanumGothic", NANUM_PATH))
        KOREAN_FONT = "NanumGothic"
    else:
        pdfmetrics.registerFont(UnicodeCIDFont(KOREAN_FONT))
except Exception:
    pdfmetrics.registerFont(UnicodeCIDFont("HYSMyeongJo-Medium"))
    KOREAN_FONT = "HYSMyeongJo-Medium"
try:
    if os.path.exists(NANUM_PATH):
        fm.fontManager.addfont(NANUM_PATH); plt.rcParams["font.family"] = "NanumGothic"
    plt.rcParams["axes.unicode_minus"] = False
except Exception:
    plt.rcParams["axes.unicode_minus"] = False

# ================== Helper ==================
def fmt_pct(v, digits=2):
    try:
        if v is None or (isinstance(v, float) and np.isnan(v)): return "-"
        return f"{float(v):.{digits}f}%"
    except Exception:
        return "-"

def pct_fmt(v, digits=2):
    try:
        if v is None or (isinstance(v, float) and np.isnan(v)): return "-"
        return f"{float(v):+.{digits}f}%"
    except Exception:
        return "-"

def write_json(path: Path, obj: dict):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False)
    except Exception:
        pass

def read_json(path: Path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

# ================== Universe & Mapping ==================
BEST_COUNT, WORST_COUNT = 3, 3

# 고정 5종 + 균등 15종(총 20)
BM20_IDS = [
    # 고정 가중 5종
    "bitcoin","ethereum","ripple","tether","binancecoin",
    # 균등 15종 (APT 제외, SUI 포함) — 총 20종 되도록 유지
    "solana","toncoin","avalanche-2","chainlink","cardano","tron",
    "near","polkadot","cosmos-hub","litecoin","arbitrum","optimism",
    "internet-computer","shiba-inu","dogecoin",
]

# TON 야후 심볼 자동 탐지(환경마다 TON-USD / TON11419-USD 다를 수 있음)
def _yf_resolve(default_symbol, fallback_list):
    """여러 심볼 중 유효한 데이터를 가진 첫 번째 심볼을 반환합니다."""
    for t in [default_symbol] + fallback_list:
        try:
            # 매우 짧은 기간 데이터를 받아와서 존재하는지 확인
            h = yf.download(t, period="1d", interval="1d", progress=False)
            if h is not None and not h.empty:
                return t
        except Exception:
            continue
    return default_symbol


def _yf_ton_symbol():
    for t in ["TON-USD", "TON11419-USD"]:
        try:
            h = yf.download(t, period="5d", interval="1d", progress=False)
            if h is not None and not h.empty:
                return t
        except Exception:
            pass
    return "TON-USD"
    
YF_MAP = {
    "bitcoin":"BTC-USD",
    "ethereum":"ETH-USD",
    "ripple":"XRP-USD",
    "tether":"USDT-USD",
    "binancecoin":"BNB-USD",
    "solana":"SOL-USD",
    "toncoin":_yf_ton_symbol(), 
    "avalanche-2":"AVAX-USD",
    "chainlink":"LINK-USD",
    "cardano":"ADA-USD",
    "shiba-inu": "SHIB-USD",
    "near":"NEAR-USD",
    "polkadot":"DOT-USD",
    "cosmos-hub":"ATOM-USD",
    "litecoin":"LTC-USD",
    "arbitrum":"ARB-USD",
    "optimism":"OP-USD",
    "internet-computer":"ICP-USD",
    "tron": "TRX-USD",
    "dogecoin":"DOGE-USD",
}

SYMBOL_MAP = {
    "bitcoin":"BTC","ethereum":"ETH","ripple":"XRP","tether":"USDT","binancecoin":"BNB",
    "solana":"SOL","toncoin":"TON","avalanche-2":"AVAX",
    "chainlink":"LINK","cardano":"ADA","shiba-inu": "SHIB","near":"NEAR",
    "polkadot":"DOT","cosmos-hub":"ATOM","litecoin":"LTC","arbitrum":"ARB",
    "optimism":"OP","internet-computer":"ICP","tron": "TRX","dogecoin":"DOGE",
}

# ================== Prices: yfinance ==================
def fetch_yf_prices(ids):
    pairs = {cid: YF_MAP.get(cid) for cid in ids}
    tickers = [t for t in pairs.values() if t]
    if not tickers:
        raise RuntimeError("No Yahoo tickers mapped.")
    end = datetime.utcnow().date()
    start = end - timedelta(days=4)

    raw = yf.download(
        tickers=tickers, start=str(start), end=str(end + timedelta(days=1)),
        interval="1d", auto_adjust=True, progress=False, group_by="ticker"
    )

    # Close 우선, 없으면 Adj Close
    def pick_close(df):
        if isinstance(df.columns, pd.MultiIndex):
            lvl1 = set(df.columns.get_level_values(1))
            use = "Close" if "Close" in lvl1 else ("Adj Close" if "Adj Close" in lvl1 else None)
            return df.xs(use, axis=1, level=1) if use else pd.DataFrame()
        else:
            if "Close" in df.columns: return df[["Close"]]
            if "Adj Close" in df.columns: return df[["Adj Close"]]
        return pd.DataFrame()

    close = pick_close(raw)

    # 멀티가 실패하면 개별 폴백
    if close is None or close.empty:
        cols = {}
        for t in tickers:
            h = yf.download(
                tickers=t, start=str(start), end=str(end + timedelta(days=1)),
                interval="1d", auto_adjust=True, progress=False
            )
            if h is not None and not h.empty:
                col = "Close" if "Close" in h.columns else ("Adj Close" if "Adj Close" in h.columns else None)
                if col:
                    cols[t] = h[col]
        if cols:
            close = pd.DataFrame(cols)

    if close is None or close.empty:
        raise RuntimeError("yfinance returned empty close prices.")

    close = close.ffill().dropna(how="all")

    # 유효 행 2개 미만(전일 비교 불가) 경고
    if close.shape[0] < 2:
        print(f"[WARN] yfinance close rows={close.shape[0]} (chg24 may be 0%)")

    last = close.iloc[-1]
    prev = close.iloc[-2] if close.shape[0] >= 2 else close.iloc[-1]
    rev = {v: k for k, v in pairs.items() if v in last.index}

    rows = []
    for tkr, cur in last.dropna().items():
        cid = rev.get(tkr)
        if not cid: continue
        pre = float(prev.get(tkr, cur))
        chg24 = (float(cur)/float(pre)-1.0)*100.0 if pre else 0.0
        rows.append({
            "id": cid, "name": cid, "sym": SYMBOL_MAP.get(cid, cid.upper()),
            "current_price": float(cur), "previous_price": float(pre),
            "price_change_pct": chg24
        })

    # 누락 채우기(NaN 유지)
    got = {r["id"] for r in rows}
    for m in ids:
        if m in got: continue
        rows.append({
            "id": m, "name": m, "sym": SYMBOL_MAP.get(m, m.upper()),
            "current_price": float("nan"), "previous_price": float("nan"), "price_change_pct": float("nan")
        })
    return pd.DataFrame(rows)

# ================== Kimchi premium ==================
CACHE = OUT_DIR / "cache"; CACHE.mkdir(exist_ok=True)
KP_CACHE = CACHE / "kimchi_last.json"
FD_CACHE = CACHE / "funding_last.json"

def _get(url, params=None, retry=5, timeout=12, headers=None):
    headers = headers or {"User-Agent":"BM20/1.0"}
    last=None
    for i in range(retry):
        try:
            r=requests.get(url, params=params, timeout=timeout, headers=headers)
            if r.status_code==429: time.sleep(1.0*(i+1)); continue
            r.raise_for_status(); return r.json()
        except Exception as e:
            last=e; time.sleep(0.6*(i+1))
    raise last

def get_kimchi(df):
    try:
        u=_get("https://api.upbit.com/v1/ticker", {"markets":"KRW-BTC"})
        btc_krw=float(u[0]["trade_price"]); dom="upbit"
    except Exception:
        last = read_json(KP_CACHE)
        if last: return last.get("kimchi_pct"), {**last, "is_cache": True}
        return None, {"dom":"fallback0","glb":"yf","fx":"fixed1450","btc_krw":None,"btc_usd":None,"usdkrw":1450.0,"is_cache":True}

    try:
        btc_usd=float(df.loc[df["id"]=="bitcoin","current_price"].iloc[0]); glb="yf"
    except Exception:
        btc_usd=None; glb=None
    if btc_usd is None or (isinstance(btc_usd,float) and np.isnan(btc_usd)):
        try:
            y = yf.Ticker("BTC-USD").history(period="2d")["Close"]
            btc_usd=float(y.iloc[-1]); glb="yfinance"
        except Exception:
            last = read_json(KP_CACHE)
            if last: return last.get("kimchi_pct"), {**last, "is_cache": True}
            return None, {"dom":dom,"glb":"fallback0","fx":"fixed1450","btc_krw":round(btc_krw,2),"btc_usd":None,"usdkrw":1450.0,"is_cache":True}
            
    try:
        fxj = _get("https://api.exchangerate.host/latest", {"base": "USD", "symbols": "KRW"})
        usdkrw = float(fxj["rates"]["KRW"])
        fx = "exchangerate.host"
        if not (900 <= usdkrw <= 2000):
            raise ValueError
    except Exception:
        try:
            h = yf.Ticker("USDKRW=X").history(period="2d")
            usdkrw = float(h["Close"].dropna().iloc[-1])
            fx = "yfinance:USDKRW=X"
            if not (900 <= usdkrw <= 2000):
                raise ValueError
        except Exception:
            usdkrw = 1450.0
            fx = "fixed1450"
    
    kp = ((btc_krw / usdkrw) - btc_usd) / btc_usd * 100
    meta = {
        "dom": dom, "glb": glb, "fx": fx,
        "btc_krw": round(btc_krw, 2),
        "btc_usd": round(btc_usd, 2),
        "usdkrw": round(usdkrw, 2),
        "kimchi_pct": round(kp, 6),
        "is_cache": False,
        "ts": int(time.time())
    }
    write_json(KP_CACHE, meta)
    return kp, meta


# ================== Funding ==================
def _get_try(url, params=None, timeout=12, retry=5, headers=None):
    if headers is None:
        headers = {"User-Agent":"BM20/1.0","Accept":"application/json"}
    for i in range(retry):
        try:
            r = requests.get(url, params=params, timeout=timeout, headers=headers)
            if r.status_code == 429:
                time.sleep(1.0*(i+1)); continue
            r.raise_for_status()
            return r.json()
        except Exception:
            time.sleep(0.5*(i+1))
    return None

def get_binance_funding(symbol="BTCUSDT"):
    domains = ["https://fapi.binance.com", "https://fapi1.binance.com", "https://fapi2.binance.com"]
    for d in domains:
        j = _get_try(f"{d}/fapi/v1/premiumIndex", {"symbol":symbol})
        if isinstance(j, dict) and j.get("lastFundingRate") is not None:
            try: return float(j["lastFundingRate"])*100.0
            except: pass
        if isinstance(j, list) and j and j[0].get("lastFundingRate") is not None:
            try: return float(j[0]["lastFundingRate"])*100.0
            except: pass
    for d in domains:
        j = _get_try(f"{d}/fapi/v1/fundingRate", {"symbol":symbol, "limit":1})
        if isinstance(j, list) and j:
            try: return float(j[0]["fundingRate"])*100.0
            except: pass
    return None

def get_bybit_funding(symbol="BTCUSDT"):
    j = _get_try("https://api.bybit.com/v5/market/tickers", {"category":"linear","symbol":symbol})
    try:
        lst = j.get("result",{}).get("list",[])
        if lst and lst[0].get("fundingRate") is not None:
            return float(lst[0]["fundingRate"])*100.0
    except: pass
    return None

def fp(v, dash_text="중"):
    return dash_text if (v is None or (isinstance(v,float) and np.isnan(v))) else f"{float(v):.4f}%"

# ================== Main Data Build ==================
# 1) Prices
df = fetch_yf_prices(BM20_IDS)

# 2) Weights (고정 5종 + 균등 15종) + 분기 리밸런싱 훅
FIXED_WEIGHTS = {
    "bitcoin": 0.30,
    "ethereum": 0.20,
    "ripple":  0.05,
    "tether":  0.05,
    "binancecoin": 0.05,
}
fixed_sum = sum(FIXED_WEIGHTS.values())  # 0.65

def compute_equal_rest_weights(ids_all: list[str]) -> dict[str, float]:
    ids_remain = [cid for cid in ids_all if cid not in FIXED_WEIGHTS]
    n = len(ids_remain)  # 기대값 15
    if n != 15:
        print(f"[WARN] Remaining count = {n} (expected 15). Check BM20_IDS membership.")
    w_rest = (1.0 - fixed_sum) / max(1, n)
    w = {cid: FIXED_WEIGHTS.get(cid, w_rest) for cid in ids_all}
    s = sum(w.values())
    if abs(s - 1.0) > 1e-12:
        w[ids_all[-1]] += (1.0 - s)  # 미세보정
    return w

def is_quarter_rebalance_day(dt_ymd: str) -> bool:
    y, m, d = map(int, dt_ymd.split("-"))
    return (m in (1,4,7,10)) and (d == 1)

weights_map = compute_equal_rest_weights(df["id"].tolist())
# (필요 시) if is_quarter_rebalance_day(YMD): weights_map = compute_equal_rest_weights(...)

df["weight_ratio"] = df["id"].map(weights_map).astype(float)

# 3) Return & contribution
df["contribution"] = (df["current_price"] - df["previous_price"]) * df["weight_ratio"]

today_value = float((df["current_price"]*df["weight_ratio"]).sum())
prev_value  = float((df["previous_price"]*df["weight_ratio"]).sum())
if prev_value == 0 or np.isnan(prev_value):  # 분모 보호
    prev_value = today_value

# ================== Index base: 2018-01-01, 시작점 100 ==================
BASE_DIR = OUT_DIR / "base"; BASE_DIR.mkdir(exist_ok=True)
BASE_FILE = BASE_DIR / "bm20_base.json"
BASE_DATE_TARGET = "2018-01-01"
BASE_INDEX_START = 100.0

def _fetch_close_matrix(tickers: list[str], start: str, end: str) -> pd.DataFrame:
    raw = yf.download(tickers=tickers, start=start, end=end, interval="1d",
                      auto_adjust=True, progress=False, group_by="ticker")
    def _pick(df):
        if isinstance(df.columns, pd.MultiIndex):
            lvl1 = set(df.columns.get_level_values(1))
            use = "Close" if "Close" in lvl1 else ("Adj Close" if "Adj Close" in lvl1 else None)
            return df.xs(use, axis=1, level=1) if use else pd.DataFrame()
        else:
            if "Close" in df.columns: return df[["Close"]]
            if "Adj Close" in df.columns: return df[["Adj Close"]]
        return pd.DataFrame()
    c = _pick(raw)
    if c is None or c.empty:
        cols = {}
        for t in tickers:
            h = yf.download(t, start=start, end=end, interval="1d", auto_adjust=True, progress=False)
            if h is not None and not h.empty:
                col = "Close" if "Close" in h.columns else ("Adj Close" if "Adj Close" in h.columns else None)
                if col: cols[t] = h[col]
        if cols:
            c = pd.DataFrame(cols)
    if c is None or c.empty:
        raise RuntimeError("Empty price matrix for base calculation")
    return c.ffill().dropna(how="all")

def _calc_base_value(ids_all: list[str], weights: dict[str,float]) -> tuple[str, float]:
    tickers = [YF_MAP[i] for i in ids_all if YF_MAP.get(i)]
    # 2018-01-01 ~ 2018-02-15(버퍼) 내 첫 가용영업일을 기준일로
    c = _fetch_close_matrix(tickers, start=BASE_DATE_TARGET, end="2018-02-15")
    if c.shape[0] == 0:
        raise RuntimeError("No base date rows available around 2018-01-01")
    base_row = c.iloc[0]
    rev = {v:k for k,v in YF_MAP.items()}
    base_val = 0.0
    for tkr, px in base_row.dropna().items():
        cid = rev.get(tkr)
        if cid in weights:
            base_val += float(px) * float(weights[cid])
    if base_val <= 0:
        raise RuntimeError("Computed base_value <= 0")
    base_date = base_row.name.strftime("%Y-%m-%d")
    return base_date, base_val

if BASE_FILE.exists():
    bj = read_json(BASE_FILE)
    base_value = float(bj["base_value"])
    base_date  = bj.get("base_date", BASE_DATE_TARGET)
else:
    base_date, base_value = _calc_base_value(df["id"].tolist(), weights_map)
    write_json(BASE_FILE, {"base_date": base_date, "base_value": base_value})

# ================== BM20 level (SSOT) ==================
# 기존의 (가중평균 가격 / 2018기준가격) 방식은 '연속지수(리밸런싱/복리)'와 다른 값(400대)을 만들 수 있음.
# 따라서 SSOT(backfill_current_basket.csv 또는 기존 bm20_series.json)의 마지막 레벨을 기준으로
# 오늘 포트폴리오 1D 수익률을 곱해 최신 레벨을 산출한다.
def _load_series_ssot():
    # 우선순위: out/backfill_current_basket.csv (연속지수) -> 루트 backfill_current_basket.csv -> bm20_series.json
    import csv
    cand = [
        OUT / "backfill_current_basket.csv",
        ROOT / "backfill_current_basket.csv",
        ROOT / "bm20_series.json",
        ROOT / "bm" / "bm20_series.json",
    ]
    for p in cand:
        try:
            if not p.exists():
                continue
            if p.name.endswith(".csv"):
                rows=[]
                with p.open("r", encoding="utf-8") as f:
                    r=csv.DictReader(f)
                    for row in r:
                        d=(row.get("date") or "").strip()[:10]
                        v=row.get("index") or row.get("level") or row.get("bm20Level")
                        if not d or v is None:
                            continue
                        try:
                            rows.append({"date": d, "level": float(v)})
                        except Exception:
                            continue
                if rows:
                    rows.sort(key=lambda x: x["date"])
                    return rows, str(p)
            else:
                obj = read_json(p) or None
                if isinstance(obj, list) and obj:
                    rows=[]
                    for it in obj:
                        if not isinstance(it, dict):
                            continue
                        d=str(it.get("date","")).strip()[:10]
                        v=it.get("level")
                        if d and v is not None:
                            try:
                                rows.append({"date": d, "level": float(v)})
                            except Exception:
                                pass
                    if rows:
                        rows.sort(key=lambda x: x["date"])
                        return rows, str(p)
        except Exception:
            continue
    return None, None

def _level_on_or_before(rows, target_ymd: str):
    for r in reversed(rows):
        if r["date"] <= target_ymd:
            return float(r["level"])
    return None

# 오늘 1D 포트폴리오 수익률(%) 계산: yesterday -> now (weights_map 기준)
# NOTE: backfill의 실제 드리프트/리밸런싱과 100% 동일하지는 않지만, '400대'로 붕괴하는 문제를 막고
# 대시보드/뉴스/루트 JSON이 같은 체계(연속지수)로 움직이게 만든다.
port_ret_1d = 0.0
denom_ok = True
for _, row in df.iterrows():
    cid = row["id"]
    w = float(weights_map.get(cid, 0.0))
    p0 = float(row.get("prev_price") or 0.0)
    p1 = float(row.get("price") or 0.0)
    if w == 0:
        continue
    if p0 <= 0 or p1 <= 0:
        denom_ok = False
        continue
    port_ret_1d += w * ((p1 / p0) - 1.0)

# SSOT series에서 전일 레벨 가져와서 오늘 레벨 계산
today_ymd = YMD
rows_ssot, ssot_src = _load_series_ssot()
if rows_ssot:
    # 기준일: SSOT의 마지막 날짜(=전일 EOD일 수도 있고, 오늘이면 intraday overwrite)
    last_date = rows_ssot[-1]["date"]
    last_level = float(rows_ssot[-1]["level"])
    if last_date == today_ymd:
        # 같은 날 재실행(intraday)인 경우: 전일 레벨을 찾고, 오늘 레벨을 재계산해서 덮어씀
        prev_dt = (dt.datetime.strptime(today_ymd, "%Y-%m-%d") - dt.timedelta(days=1)).strftime("%Y-%m-%d")
        prev_level = _level_on_or_before(rows_ssot, prev_dt) or last_level
        bm20_now = prev_level * (1.0 + port_ret_1d) if denom_ok else last_level
        bm20_prev_level = prev_level
        rows_ssot[-1]["level"] = float(bm20_now)
    else:
        bm20_prev_level = last_level
        bm20_now = last_level * (1.0 + port_ret_1d) if denom_ok else last_level
        rows_ssot.append({"date": today_ymd, "level": float(bm20_now)})
else:
    # 마지막 fallback: 기존(가중평균) 방식 유지
    bm20_now = (today_value / base_value) * BASE_INDEX_START if base_value else 0.0
    bm20_prev_level = (prev_value / base_value) * BASE_INDEX_START if (base_value and prev_value) else None
    ssot_src = "fallback_weighted_avg"

bm20_chg = float(port_ret_1d) * 100.0 if rows_ssot else ((today_value/prev_value - 1) * 100.0 if prev_value else 0.0)

num_up  = int((df["price_change_pct"]>0).sum())
num_down= int((df["price_change_pct"]<0).sum())

best = df.sort_values("price_change_pct", ascending=False).head(BEST_COUNT).reset_index(drop=True)
worst= df.sort_values("price_change_pct", ascending=True ).head(WORST_COUNT).reset_index(drop=True)

# ================== Kimchi & funding ==================
kimchi_pct, kp_meta = get_kimchi(df)
kp_is_cache = bool(kp_meta.get("is_cache")) if kp_meta else False
# 12시간 이상 캐시면 '구캐시' 표시
if kp_is_cache and kp_meta and (time.time() - kp_meta.get("ts", 0) > 12*3600):
    kp_is_cache = True
kp_text_base = fmt_pct(kimchi_pct, 2) if kimchi_pct is not None else "잠정(전일)"
kp_text = kp_text_base + (" (캐시)" if kp_is_cache else "")

btc_f_bin_live = get_binance_funding("BTCUSDT"); time.sleep(0.2)
eth_f_bin_live = get_binance_funding("ETHUSDT"); time.sleep(0.2)
btc_f_byb_live = get_bybit_funding("BTCUSDT");   time.sleep(0.2)
eth_f_byb_live = get_bybit_funding("ETHUSDT")

last_fd = read_json(FD_CACHE) or {}
bin_cache_used = False; byb_cache_used = False

btc_f_bin = btc_f_bin_live or last_fd.get("btc_f_bin"); bin_cache_used |= (btc_f_bin_live is None and btc_f_bin is not None)
eth_f_bin = eth_f_bin_live or last_fd.get("eth_f_bin"); bin_cache_used |= (eth_f_bin_live is None and eth_f_bin is not None)
btc_f_byb = btc_f_byb_live or last_fd.get("btc_f_byb"); byb_cache_used |= (btc_f_byb_live is None and btc_f_byb is not None)
eth_f_byb = eth_f_byb_live or last_fd.get("eth_f_byb"); byb_cache_used |= (eth_f_byb_live is None and eth_f_byb is not None)

write_json(FD_CACHE, {"btc_f_bin":btc_f_bin, "eth_f_bin":eth_f_bin, "btc_f_byb":btc_f_byb, "eth_f_byb":eth_f_byb})

bin_suffix = " (캐시)" if bin_cache_used else ""
byb_suffix = " (캐시)" if byb_cache_used else ""

BIN_TEXT = f"BTC {fp(btc_f_bin)} / ETH {fp(eth_f_bin)}{bin_suffix}"
BYB_TEXT = (None if (btc_f_byb is None and eth_f_byb is None)
            else f"BTC {fp(btc_f_byb)} / ETH {fp(eth_f_byb)}{byb_suffix}")

# ================== News (Best/Worst 표현) ==================
def build_news_editorial():
    def pct(v):  return f"{float(v):+,.2f}%"
    def abs_pct(v): return f"{abs(float(v)):.2f}%"
    def num2(v): s=f"{float(v):,.2f}"; return s.rstrip('0').rstrip('.')
    trend_word = "상승" if bm20_chg>0 else ("하락" if bm20_chg<0 else "보합")
    title = f"BM20 {abs_pct(bm20_chg)} {trend_word}…지수 {num2(bm20_now)}pt, 김치프리미엄 {fmt_pct(kimchi_pct,2)}" + (" (캐시)" if kp_is_cache else "")

    def list_line(df_rank, label):
        arr = [f"{r['sym']}({pct(r['price_change_pct'])})" for _,r in df_rank.iterrows() if not np.isnan(r['price_change_pct'])]
        return f"BM20 {label} {len(arr)}: " + ", ".join(arr) if arr else ""

    best_line = list_line(best, "Best")
    worst_line= list_line(worst, "Worst")

    def line_for(coin_id, display=None):
        row = df.loc[df["id"]==coin_id]
        if row.empty: return ""
        r = row.iloc[0]
        chg = r["price_change_pct"]; price = r["current_price"]; sym = r["sym"]
        if np.isnan(chg) or np.isnan(price): return ""
        name = display or coin_id.upper()
        return f"{name}({sym})는 {pct(chg)} {'하락' if chg<0 else ('상승' if chg>0 else '보합')}한 {num2(price)}달러."

    btc_line = line_for("bitcoin","비트코인")
    eth_line = line_for("ethereum","이더리움")

    breadth_word = "강세 우위" if num_up>num_down else ("약세 우위" if num_down>num_up else "중립")
    breadth = f"시장 폭은 상승 {num_up}·하락 {num_down}로 {breadth_word}다."

    kp_side = "국내 거래소가 해외 대비 소폭 할인되어" if (kimchi_pct is not None and kimchi_pct<0) else "국내 거래소가 소폭 할증되어"
    kp_line = f"국내외 가격 차이를 나타내는 김치 프리미엄은 {fmt_pct(kimchi_pct,2)}로{(' (캐시)' if kp_is_cache else '')}, {kp_side} 거래됐다."
    fund_line = f"바이낸스 기준 펀딩비는 {BIN_TEXT}" + ("" if BYB_TEXT is None else f", 바이빗은 {BYB_TEXT}") + "로 집계됐다."

    body = " ".join([
        f"BM20 지수가 {YMD} 전일 대비 {pct(bm20_chg)} {trend_word}해 {num2(bm20_now)}포인트를 기록했다.",
        breadth,
        best_line, worst_line,
        btc_line, eth_line,
        kp_line, fund_line
    ])
    return title, body

news_title, news_body = build_news_editorial()
news = f"{news_title}\n{news_body}"

with open(txt_path,"w",encoding="utf-8") as f: f.write(news)
df_out = df[["sym","id","current_price","previous_price","price_change_pct","weight_ratio","contribution"]].rename(columns={"sym":"symbol"})
df_out.to_csv(csv_path, index=False, encoding="utf-8")
write_json(kp_path, {"date":YMD, **(kp_meta or {}), "kimchi_pct": (None if kimchi_pct is None else round(float(kimchi_pct),4))})

# ================== Charts ==================
# A) 퍼포먼스 바 (Best/Worst와 일관성)
perf = df.sort_values("price_change_pct", ascending=False)[["sym","price_change_pct"]].reset_index(drop=True)
plt.figure(figsize=(10.6, 4.6))
x = range(len(perf)); y = perf["price_change_pct"].values
colors_v = ["#2E7D32" if (isinstance(v,(int,float)) and v >= 0) else "#C62828" for v in y]
y_plot = [0.0 if (isinstance(v,float) and np.isnan(v)) else v for v in y]
plt.bar(x, y_plot, color=colors_v, width=0.82, edgecolor="#263238", linewidth=0.2)
plt.xticks(x, perf["sym"], rotation=0, fontsize=10)
plt.axhline(0, linewidth=1, color="#90A4AE")
if len(y_plot)>0:
    y_max = max(y_plot); y_min = min(y_plot)
else:
    y_max = y_min = 0
for i, v in enumerate(y):
    if isinstance(v,float) and np.isnan(v): continue
    off = (max(y_max,0)*0.03 if v>=0 else -abs(min(y_min,0))*0.03) or (0.25 if v>=0 else -0.25)
    va  = "bottom" if v>=0 else "top"
    plt.text(i, v + off, f"{v:+.2f}%", ha="center", va=va, fontsize=10, fontweight="600")
plt.title("코인별 퍼포먼스 (1D, USD)", fontsize=13, loc="left", pad=10)
plt.ylabel("%"); plt.tight_layout(); plt.savefig(bar_png, dpi=180); plt.close()

# B) BTC/ETH 7일 추세 (yfinance)
def get_pct_series_yf(ticker, days=8):
    try:
        end = datetime.utcnow().date()
        start = end - timedelta(days=days+1)
        h = yf.download(tickers=ticker, start=str(start), end=str(end + timedelta(days=1)),
                        interval="1d", auto_adjust=True, progress=False)
        if h is None or h.empty: return []
        col = "Close" if "Close" in h.columns else ("Adj Close" if "Adj Close" in h.columns else None)
        if not col: return []
        s = h[col].dropna().tolist()
        if not s: return []
        base = s[0]
        return [ (v/base - 1.0)*100.0 for v in s ]
    except Exception as e:
        print(f"[WARN] yfinance trend failed for {ticker}: {e}")
        return []

btc7=get_pct_series_yf("BTC-USD", 8); time.sleep(0.2)
eth7=get_pct_series_yf("ETH-USD", 8)
plt.figure(figsize=(10.6, 3.8))
if btc7: plt.plot(range(len(btc7)), btc7, label="BTC")
if eth7: plt.plot(range(len(eth7)), eth7, label="ETH")
if btc7 or eth7: plt.legend(loc="upper left")
plt.title("BTC & ETH 7일 가격 추세", fontsize=13, loc="left", pad=8)
plt.ylabel("% (from start)"); plt.tight_layout(); plt.savefig(trend_png, dpi=180); plt.close()

# ================== Index history & returns ==================
HIST_DIR = OUT_DIR / "history"; HIST_DIR.mkdir(parents=True, exist_ok=True)
HIST_CSV = HIST_DIR / "bm20_index_history.csv"

today_row = {"date": YMD, "index": round(float(bm20_now), 6)}
if HIST_CSV.exists():
    hist = pd.read_csv(HIST_CSV, dtype={"date":str})
    if "date" not in hist.columns or "index" not in hist.columns:
        raise RuntimeError(f"Invalid history schema: {HIST_CSV}")
    hist = hist[hist["date"] != YMD]
    hist = pd.concat([hist, pd.DataFrame([today_row])], ignore_index=True)
else:
    hist = pd.DataFrame([today_row])
hist = hist.sort_values("date").reset_index(drop=True)
if DAILY_SNAPSHOT:
    hist.to_csv(HIST_CSV, index=False, encoding="utf-8")
    print("[OK] History updated (daily snapshot).")
else:
    print("[SKIP] Intraday run: history not updated.")

def period_return(days: int):
    if len(hist) < 2: return None
    try:
        ref_date = (datetime.strptime(YMD, "%Y-%m-%d") - timedelta(days=days)).strftime("%Y-%m-%d")
        ref_series = hist[hist["date"] <= ref_date]
        if ref_series.empty: return None
        ref_idx = float(ref_series.iloc[-1]["index"])
        cur_idx = float(hist.iloc[-1]["index"])
        if ref_idx == 0: return None
        return (cur_idx / ref_idx - 1.0) * 100.0
    except Exception:
        return None

RET_1D  = period_return(1)
RET_7D  = period_return(7)
RET_30D = period_return(30)

today_dt = datetime.strptime(YMD, "%Y-%m-%d")
month_start = today_dt.replace(day=1).strftime("%Y-%m-%d")
year_start  = today_dt.replace(month=1, day=1).strftime("%Y-%m-%d")

def level_on_or_before(yyyymmdd: str):
    s = hist[hist["date"] <= yyyymmdd]
    return None if s.empty else float(s.iloc[-1]["index"])

lvl_month = level_on_or_before(month_start)
lvl_year  = level_on_or_before(year_start)
lvl_now   = float(hist.iloc[-1]["index"])
RET_MTD = None if not lvl_month or lvl_month==0 else (lvl_now/lvl_month - 1)*100
RET_YTD = None if not lvl_year  or lvl_year==0  else (lvl_now/lvl_year  - 1)*100

def _to_ratio(x):
    """Accept percent(12.3), ratio(0.123), string '12.3%' and normalize to ratio(0.123)."""
    if x is None:
        return None
    try:
        if isinstance(x, str):
            s = x.strip().replace("%", "")
            v = float(s)
            # 문자열이 %였다고 가정
            return v / 100.0
        v = float(x)
        # 2 이상이면 %일 확률이 매우 높음 (예: 82.8 = 82.8%)
        return v / 100.0 if abs(v) >= 2.0 else v
    except Exception:
        return None



# ================== Latest JSON (Dashboard 핵심) ==================

LATEST_JSON = Path("bm20_latest.json")
SERIES_JSON = Path("bm20_series.json")

# --- ensure breadth always exists (intraday safe) ---
if "breadth" not in locals():
    breadth = {"up": None, "down": None}

# 1D는 "레벨로부터" 확정 계산 (가장 안전)
bm20ChangePct = None
if bm20_prev_level not in (None, 0):
    bm20ChangePct = (float(bm20_now) / float(bm20_prev_level)) - 1.0

latest_obj = {
    "asOf": YMD,
    "bm20Level": round(float(bm20_now), 6),
    "bm20PrevLevel": (round(float(bm20_prev_level), 6) if bm20_prev_level is not None else None),
    "bm20PointChange": (round(float(bm20_now - bm20_prev_level), 6) if bm20_prev_level is not None else None),

    # ✅ ratio로 고정
    "bm20ChangePct": bm20ChangePct,

    "returns": {
        # ✅ 1D도 ratio로 고정 (레벨 기반이 SSOT)
        "1D": bm20ChangePct,
        # ✅ 나머지는 들어오는 값이 %든 ratio든 자동으로 ratio로 정규화
        "7D": _to_ratio(RET_7D),
        "30D": _to_ratio(RET_30D),
        "MTD": _to_ratio(RET_MTD),
        "YTD": _to_ratio(RET_YTD),
    },

    "breadth": breadth,

    # ✅ 김치프리미엄은 "퍼센트 숫자"로 유지하는 게 네 UI에 맞음
    "kimchiPremiumPct": kimchi_pct,
    "kimchi_premium_pct": kimchi_pct,
}

# series.json
# series: SSOT 우선(rows_ssot), 없으면 history CSV를 사용
if 'rows_ssot' in globals() and rows_ssot:
    series_obj = rows_ssot
else:
    series_obj = [{"date": d, "level": float(v)} for d, v in history]
SERIES_JSON.write_text(json.dumps(series_obj, ensure_ascii=False, indent=2), encoding="utf-8")

LATEST_JSON.write_text(json.dumps(latest_obj, ensure_ascii=False, indent=2), encoding="utf-8")
print(f"[OK] Written: {LATEST_JSON}")


# ================== Series JSON (Index history 기반) ==================

series_list = []

try:
    for _, row in hist.tail(365).iterrows():
        series_list.append({
            "date": row["date"],
            "level": round(float(row["index"]), 6)
        })
except Exception as e:
    print("[WARN] Failed building series:", e)

series_obj = {
    "updated": YMD,
    "count": len(series_list),
    "series": series_list
}

with open(SERIES_JSON, "w", encoding="utf-8") as f:
    json.dump(series_obj, f, ensure_ascii=False, indent=2)

print(f"[OK] Written: {SERIES_JSON}")


# ================== PDF ==================
styles = getSampleStyleSheet()
title_style    = ParagraphStyle("Title",    fontName=KOREAN_FONT, fontSize=18, alignment=1, spaceAfter=6)
subtitle_style = ParagraphStyle("Subtitle", fontName=KOREAN_FONT, fontSize=12.5, alignment=1,
                                textColor=colors.HexColor("#546E7A"), spaceAfter=12)
section_h      = ParagraphStyle("SectionH", fontName=KOREAN_FONT, fontSize=13,  alignment=0,
                                textColor=colors.HexColor("#1A237E"), spaceBefore=4, spaceAfter=8)
body_style     = ParagraphStyle("Body",     fontName=KOREAN_FONT, fontSize=11,  alignment=0, leading=16)
small_style    = ParagraphStyle("Small",    fontName=KOREAN_FONT, fontSize=9,   alignment=1, textColor=colors.HexColor("#78909C"))

def card(flowables, pad=10, bg="#FFFFFF", border="#E5E9F0"):
    tbl = Table([[flowables]], colWidths=[16.4*cm])
    tbl.setStyle(TableStyle([
        ("FONTNAME", (0,0), (-1,-1), KOREAN_FONT),
        ("LEFTPADDING",(0,0),(-1,-1), pad), ("RIGHTPADDING",(0,0),(-1,-1), pad),
        ("TOPPADDING",(0,0),(-1,-1), pad),  ("BOTTOMPADDING",(0,0),(-1,-1), pad),
        ("BACKGROUND",(0,0),(-1,-1), colors.HexColor(bg)),
        ("BOX",(0,0),(-1,-1),0.75, colors.HexColor(border)),
        ("VALIGN",(0,0),(-1,-1),"TOP"),
    ]))
    return tbl

def style_table_basic(t, header_bg="#EEF4FF", box="#CFD8DC", grid="#E5E9F0", fs=10.5):
    t.setStyle(TableStyle([
        ("FONTNAME",(0,0),(-1,-1), KOREAN_FONT),
        ("FONTSIZE",(0,0),(-1,-1), fs),
        ("BACKGROUND",(0,0),(-1,0), colors.HexColor(header_bg)),
        ("BOX",(0,0),(-1,-1),0.5, colors.HexColor(box)),
        ("INNERGRID",(0,0),(-1,-1),0.25, colors.HexColor(grid)),
        ("ALIGN",(0,0),(-1,-1),"LEFT"),
        ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
    ]))

doc = SimpleDocTemplate(str(pdf_path), pagesize=A4,
                        leftMargin=1.8*cm, rightMargin=1.8*cm,
                        topMargin=1.6*cm, bottomMargin=1.6*cm)

story = []
story += [Paragraph("BM20 데일리 리포트 (Yahoo Finance / Custom Weights)", title_style),
          Paragraph(f"{YMD}", subtitle_style)]

metrics = [
    ["지수",        f"{bm20_now:,.2f} pt"],
    ["일간 변동",   f"{bm20_chg:+.2f}%"],
    ["상승/하락",   f"{num_up} / {num_down}"],
    ["수익률(1D/7D/30D/MTD/YTD)", f"{pct_fmt(RET_1D)} / {pct_fmt(RET_7D)} / {pct_fmt(RET_30D)} / {pct_fmt(RET_MTD)} / {pct_fmt(RET_YTD)}"],
    ["김치 프리미엄", kp_text],
    ["펀딩비(Binance)", BIN_TEXT],
]
if BYB_TEXT:
    metrics.append(["펀딩비(Bybit)", BYB_TEXT])
mt = Table(metrics, colWidths=[5.0*cm, 11.0*cm]); style_table_basic(mt)
story += [card([mt]), Spacer(1, 0.45*cm)]

best_tbl = [["Best 3","등락률"], *[[r["sym"], f"{r['price_change_pct']:+.2f}%"] for _,r in best.iterrows() if not np.isnan(r["price_change_pct"])]]
worst_tbl= [["Worst 3","등락률"], *[[r["sym"], f"{r['price_change_pct']:+.2f}%"] for _,r in worst.iterrows() if not np.isnan(r["price_change_pct"])]]
t_best = Table(best_tbl,  colWidths=[8.0*cm, 3.5*cm]); t_worst = Table(worst_tbl, colWidths=[8.0*cm, 3.5*cm])
style_table_basic(t_best); style_table_basic(t_worst)
story += [card([Paragraph("Best/Worst (1D, USD)", section_h), Spacer(1,4), t_best, Spacer(1,6), t_worst]),
          Spacer(1, 0.45*cm)]

perf_block = [Paragraph("코인별 퍼포먼스 (1D, USD)", section_h)]
if bar_png.exists(): perf_block += [Image(str(bar_png), width=16.0*cm, height=6.6*cm)]
story += [card(perf_block), Spacer(1, 0.45*cm)]

trend_block = [Paragraph("BTC & ETH 7일 가격 추세", section_h)]
if trend_png.exists(): trend_block += [Image(str(trend_png), width=16.0*cm, height=5.2*cm)]
story += [card(trend_block), Spacer(1, 0.45*cm)]

story += [card([Paragraph("BM20 데일리 뉴스", section_h), Spacer(1,2), Paragraph(news.replace("\n","<br/>"), body_style)]),
          Spacer(1, 0.45*cm)]
story += [Paragraph("© Blockmedia · Data: Yahoo Finance, Upbit · Funding: Binance & Bybit",
                    small_style)]
doc.build(story)

# ================== HTML ==================
html_tpl = Template(r"""
<!doctype html><html lang="ko"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>BM20 데일리 {{ ymd }}</title>
<style>
body{font-family:-apple-system,BlinkMacSystemFont,"NanumGothic","Noto Sans CJK","Malgun Gothic",Arial,sans-serif;background:#fafbfc;color:#111;margin:0}
.wrap{max-width:760px;margin:0 auto;padding:20px}
.card{background:#fff;border:1px solid #e5e9f0;border-radius:12px;padding:20px;margin-bottom:16px}
h1{font-size:22px;margin:0 0 8px 0;text-align:center} h2{font-size:15px;margin:16px 0 8px 0;color:#1A237E}
.muted{color:#555;text-align:center} .center{text-align:center}
table{width:100%;border-collapse:collapse;font-size:14px} th,td{border:1px solid #e5e9f0;padding:8px} th{background:#eef4ff}
.footer{font-size:12px;color:#666;text-align:center;margin-top:16px}
img{max-width:100%}
</style></head><body>
<div class="wrap">
  <div class="card">
    <h1>BM20 데일리 리포트</h1>
    <div class="muted">{{ ymd }}</div>
    <table style="margin-top:10px">
      <tr><th>지수</th><td>{{ bm20_now }} pt</td></tr>
      <tr><th>일간 변동</th><td>{{ bm20_chg }}</td></tr>
      <tr><th>상승/하락</th><td>{{ num_up }} / {{ num_down }}</td></tr>
      <tr><th>수익률(1D/7D/30D/MTD/YTD)</th><td>{{ ret_1d }} / {{ ret_7d }} / {{ ret_30d }} / {{ ret_mtd }} / {{ ret_ytd }}</td></tr>
      <tr><th>김치 프리미엄</th><td>{{ kp_text }}</td></tr>
      <tr><th>펀딩비(Binance)</th><td>{{ bin_text }}</td></tr>
      {% if byb_text %}<tr><th>펀딩비(Bybit)</th><td>{{ byb_text }}</td></tr>{% endif %}
    </table>
  </div>
  <div class="card">
    <h2>Best/Worst (1D, USD)</h2>
    <table><tr><th>Best</th><th style="text-align:right">등락률</th></tr>
      {% for r in best %}<tr><td>{{ r.sym }}</td><td style="text-align:right">{{ r.pct }}</td></tr>{% endfor %}
    </table><br>
    <table><tr><th>Worst</th><th style="text-align:right">등락률</th></tr>
      {% for r in worst %}<tr><td>{{ r.sym }}</td><td style="text-align:right">{{ r.pct }}</td></tr>{% endfor %}
    </table>
  </div>
  <div class="card">
    <h2>코인별 퍼포먼스 (1D, USD)</h2>
    {% if bar_png %}<p class="center"><img src="{{ bar_png }}?v={{ ts }}" alt="Performance"></p>{% endif %}
    <h2>BTC & ETH 7일 가격 추세</h2>
    {% if trend_png %}<p class="center"><img src="{{ trend_png }}?v={{ ts }}" alt="Trend"></p>{% endif %}
  </div>
  <div class="card"><h2>BM20 데일리 뉴스</h2><p>{{ news_html }}</p></div>
  <div class="footer">© Blockmedia · Data: Yahoo Finance, Upbit · Funding: Binance & Bybit</div>
</div></body></html>
""")
html = html_tpl.render(
    ymd=YMD, bm20_now=f"{bm20_now:,.2f}", bm20_chg=f"{bm20_chg:+.2f}%",
    num_up=num_up, num_down=num_down,
    ret_1d=pct_fmt(RET_1D), ret_7d=pct_fmt(RET_7D), ret_30d=pct_fmt(RET_30D),
    ret_mtd=pct_fmt(RET_MTD), ret_ytd=pct_fmt(RET_YTD),
    kp_text=kp_text, bin_text=BIN_TEXT, byb_text=BYB_TEXT,
    best=[{"sym":r["sym"], "pct": f"{r['price_change_pct']:+.2f}%"} for _,r in best.iterrows() if not np.isnan(r["price_change_pct"])],
    worst=[{"sym":r["sym"], "pct": f"{r['price_change_pct']:+.2f}%"} for _,r in worst.iterrows() if not np.isnan(r["price_change_pct"])],
    bar_png=os.path.basename(bar_png), trend_png=os.path.basename(trend_png),
    news_html=news.replace("\n","<br/>"),
    ts=TS
)
with open(html_path, "w", encoding="utf-8") as f: f.write(html)
