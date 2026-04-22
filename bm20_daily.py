#!/usr/bin/env python3
# ===================== BM20 Daily — Yahoo Finance (Final, Blockmedia rules) =====================
# 목적: CoinGecko 없이도 리포트(out/YYYY-MM-DD) 생성. 가중치는
#   BTC 32% / ETH 20% / XRP 5% / USDT 5% / BNB 5% / SOL 5% / 나머지 14종 × 2% 균등
# - 가격/등락률/7일 추세: yfinance
# - 김치 프리미엄: Upbit KRW-BTC vs (BTC-USD * USDKRW from exchangerate.host), 폴백 1480
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

# ================== Market Indices Helper (BTC, NASDAQ, KOSPI) ==================
def update_market_indices():
    """Yahoo Finance API 직접 호출로 나스닥/코스피/BTC 시리즈 업데이트 (yfinance 라이브러리 우회)"""
    import datetime as _dt

    indices = {
        "btc_usd": "BTC-USD",
        "nasdaq":  "%5EIXIC",   # ^IXIC URL 인코딩
        "kospi":   "%5EKS11",   # ^KS11 URL 인코딩
    }

    print("\n--- 시장 지수 및 비트코인 데이터 업데이트 시작 ---")
    import datetime as _dt2
    period1 = int(_dt2.datetime(2018, 1, 1).timestamp())
    period2 = int((_dt2.datetime.utcnow() + _dt2.timedelta(days=2)).timestamp())

    for name, symbol in indices.items():
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
            r = requests.get(
                url,
                headers={"User-Agent": "Mozilla/5.0"},
                params={"interval": "1d", "period1": period1, "period2": period2},
                timeout=20,
            )
            r.raise_for_status()
            result = r.json()["chart"]["result"][0]
            timestamps = result["timestamp"]
            closes = result["indicators"]["quote"][0]["close"]

            output_list = []
            for ts, price in zip(timestamps, closes):
                if price is None:
                    continue
                date_str = _dt.datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
                output_list.append({"date": date_str, "price": int(round(price))})

            if not output_list:
                print(f"[ERR] {name} update failed: empty data")
                continue

            with open(f"{name}_series.json", "w", encoding="utf-8") as f:
                json.dump(output_list, f, ensure_ascii=False, indent=2)
            print(f"[OK] {name}_series.json updated. ({output_list[-1]['date']}, {len(output_list)}개)")
        except Exception as e:
            print(f"[ERR] {name} update failed: {e}")



# ================== Universe & Mapping ==================
BEST_COUNT, WORST_COUNT = 3, 3

# 고정 6종 + 균등 14종(총 20)
BM20_IDS = [
    # 고정 가중 6종 (T1)
    "bitcoin","ethereum","ripple","tether","binancecoin","solana",
    # 균등 14종 (2026 Q2 CMC 스냅샷 기반) — 총 20종
    "dogecoin","tron","hyperliquid","cardano","bitcoin-cash",
    "chainlink","stellar","litecoin","zcash","avalanche-2",
    "hedera-hashgraph","shiba-inu","sui","canton",
]

YF_MAP = {
    "bitcoin":"BTC-USD",
    "ethereum":"ETH-USD",
    "ripple":"XRP-USD",
    "tether":"USDT-USD",
    "binancecoin":"BNB-USD",
    "solana":"SOL-USD",
    "dogecoin":"DOGE-USD",
    "tron":"TRX-USD",
    "cardano":"ADA-USD",
    "hyperliquid":"HYPE32196-USD",
    "chainlink":"LINK-USD",
    "sui":"SUI20947-USD",
    "avalanche-2":"AVAX-USD",
    "stellar":"XLM-USD",
    "bitcoin-cash":"BCH-USD",
    "hedera-hashgraph":"HBAR-USD",
    "litecoin":"LTC-USD",
    "shiba-inu":"SHIB-USD",
    "zcash":"ZEC-USD",
    "canton":"CC37263-USD",
}

SYMBOL_MAP = {
    "bitcoin":"BTC","ethereum":"ETH","ripple":"XRP","tether":"USDT","binancecoin":"BNB",
    "solana":"SOL","dogecoin":"DOGE","tron":"TRX","cardano":"ADA",
    "hyperliquid":"HYPE","chainlink":"LINK","sui":"SUI","avalanche-2":"AVAX",
    "stellar":"XLM","bitcoin-cash":"BCH","hedera-hashgraph":"HBAR",
    "litecoin":"LTC","shiba-inu":"SHIB",
    "zcash":"ZEC","canton":"CC",
}

# ================== Prices: CoinMarketCap ==================
# CMC slug → symbol 매핑 (BM20_IDS 기준)
CMC_SYMBOL_MAP = {
    "bitcoin": "BTC", "ethereum": "ETH", "ripple": "XRP",
    "tether": "USDT", "binancecoin": "BNB", "solana": "SOL",
    "dogecoin": "DOGE", "tron": "TRX",
    "cardano": "ADA", "hyperliquid": "HYPE", "chainlink": "LINK",
    "sui": "SUI", "avalanche-2": "AVAX", "stellar": "XLM",
    "bitcoin-cash": "BCH", "hedera-hashgraph": "HBAR",
    "litecoin": "LTC", "shiba-inu": "SHIB",
    "zcash": "ZEC", "canton": "CC",
}

def fetch_yf_prices(ids):
    """CMC API로 20개 코인 가격 + 24h 등락률 가져오기 (yfinance 대체)"""
    api_key = os.getenv("CMC_API_KEY", "")
    if not api_key:
        raise RuntimeError("CMC_API_KEY 환경변수 없음")

    # CMC는 심볼로 조회
    symbols = [CMC_SYMBOL_MAP.get(cid, cid.upper()) for cid in ids]
    symbol_str = ",".join(symbols)

    r = requests.get(
        "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest",
        headers={"X-CMC_PRO_API_KEY": api_key},
        params={"symbol": symbol_str, "convert": "USD"},
        timeout=15,
    )
    r.raise_for_status()
    data = r.json().get("data", {})
    print(f"[INFO] CMC 응답 코인 수: {len(data)}개")

    # symbol → cid 역매핑
    sym_to_cid = {v: k for k, v in CMC_SYMBOL_MAP.items()}

    rows = []
    got = set()
    for sym, entries in data.items():
        # CMC는 같은 심볼이 여러 개일 때 리스트로 반환
        entry = entries[0] if isinstance(entries, list) else entries
        quote = entry.get("quote", {}).get("USD", {})
        price = quote.get("price")
        chg24 = quote.get("percent_change_24h")
        if price is None:
            continue
        cid = sym_to_cid.get(sym.upper())
        if not cid:
            continue
        price = float(price)
        chg24 = float(chg24) if chg24 is not None else 0.0
        # previous_price 역산 (chg24 기반)
        prev_price = price / (1.0 + chg24 / 100.0) if chg24 != -100 else price
        rows.append({
            "id": cid, "name": cid, "sym": SYMBOL_MAP.get(cid, cid.upper()),
            "current_price": price,
            "previous_price": prev_price,
            "price_change_pct": chg24,
        })
        got.add(cid)

    # 누락 채우기(NaN 유지)
    for m in ids:
        if m in got:
            continue
        rows.append({
            "id": m, "name": m, "sym": SYMBOL_MAP.get(m, m.upper()),
            "current_price": float("nan"), "previous_price": float("nan"), "price_change_pct": float("nan")
        })

    print(f"[INFO] 가격 조회 성공: {len(got)}/{len(ids)}개")
    return pd.DataFrame(rows)

# ================== Kimchi premium ==================
CACHE = OUT_DIR / "cache"; CACHE.mkdir(exist_ok=True)
KP_CACHE = CACHE / "kimchi_last.json"

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
        return None, {"dom":"fallback0","glb":"yf","fx":"fixed1480","btc_krw":None,"btc_usd":None,"usdkrw":1480.0,"is_cache":True}

    # 글로벌 BTC 기준: 바이낸스 실시간 (smart_kimchi_8h.py 와 동일 기준)
    btc_usd = None; glb = None
    for binance_base in ["https://api.binance.com", "https://data-api.binance.vision"]:
        try:
            j = _get(f"{binance_base}/api/v3/ticker/price", {"symbol": "BTCUSDT"})
            btc_usd = float(j["price"]); glb = "binance"
            break
        except Exception:
            continue
    if btc_usd is None:
        # 바이낸스 실패 시 yfinance 폴백
        try:
            y = yf.Ticker("BTC-USD").history(period="2d")["Close"]
            btc_usd = float(y.iloc[-1]); glb = "yfinance"
        except Exception:
            last = read_json(KP_CACHE)
            if last: return last.get("kimchi_pct"), {**last, "is_cache": True}
            return None, {"dom":dom,"glb":"fallback0","fx":"fixed1480","btc_krw":round(btc_krw,2),"btc_usd":None,"usdkrw":1480.0,"is_cache":True}

    # 환율: Yahoo Finance API 직접 호출 (yfinance 라이브러리 버그 우회)
    try:
        _r = requests.get(
            "https://query1.finance.yahoo.com/v8/finance/chart/USDKRW=X",
            headers={"User-Agent": "Mozilla/5.0"},
            params={"interval": "1d", "range": "2d"},
            timeout=10,
        )
        _r.raise_for_status()
        usdkrw = float(_r.json()["chart"]["result"][0]["meta"]["regularMarketPrice"])
        fx = "yahoo_api:USDKRW=X"
        if not (900 <= usdkrw <= 2000):
            raise ValueError(f"환율 이상값: {usdkrw}")
        print(f"[INFO] USDKRW={usdkrw:.2f} (Yahoo API)")
    except Exception as _e:
        print(f"[WARN] USDKRW fetch failed: {_e} → 1480 fallback")
        usdkrw = 1480.0
        fx = "fixed1480"
    
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

# ================== Main Data Build ==================
# 1) Prices
df = fetch_yf_prices(BM20_IDS)

# 2) Weights (고정 6종 BTC 0.32 + 균등 14종 × 0.02) + 분기 리밸런싱 훅
FIXED_WEIGHTS = {
    "bitcoin": 0.32,
    "ethereum": 0.20,
    "ripple":  0.05,
    "tether":  0.05,
    "binancecoin": 0.05,
    "solana":  0.05,
}
fixed_sum = sum(FIXED_WEIGHTS.values())  # 0.72

def compute_equal_rest_weights(ids_all: list[str]) -> dict[str, float]:
    ids_remain = [cid for cid in ids_all if cid not in FIXED_WEIGHTS]
    n = len(ids_remain)  # 기대값 14
    if n != 14:
        print(f"[WARN] Remaining count = {n} (expected 14). Check BM20_IDS membership.")
    w_rest = (1.0 - fixed_sum) / max(1, n)  # 0.28 / 14 = 0.02
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
# price_change_pct 기반으로 직접 계산 (CMC에서 이미 정확한 24h 변동률 제공)
port_ret_1d = 0.0
denom_ok = True
for _, row in df.iterrows():
    cid = row["id"]
    w = float(weights_map.get(cid, 0.0))
    pct = row.get("price_change_pct")
    if w == 0:
        continue
    if pct is None or (isinstance(pct, float) and np.isnan(pct)):
        denom_ok = False
        continue
    port_ret_1d += w * (float(pct) / 100.0)

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

        # ── 갭필: last_date ~ today 사이 빠진 날짜를 전일값으로 채우기 ──
        try:
            from datetime import date as _date
            last_dt  = dt.datetime.strptime(last_date, "%Y-%m-%d").date()
            today_dt2 = dt.datetime.strptime(today_ymd, "%Y-%m-%d").date()
            gap_days = (today_dt2 - last_dt).days
            if gap_days > 1:
                print(f"[INFO] Gap detected: {last_date} → {today_ymd} ({gap_days-1}일 누락), 전일값으로 채움")
                fill_level = last_level
                for i in range(1, gap_days):
                    fill_date = (last_dt + dt.timedelta(days=i)).strftime("%Y-%m-%d")
                    rows_ssot.append({"date": fill_date, "level": float(fill_level)})
                    print(f"[GAPFILL] {fill_date}: {fill_level:.4f} (전일값 복사)")
        except Exception as _ge:
            print(f"[WARN] Gap fill failed: {_ge}")
        # ────────────────────────────────────────────────────────────────

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

# ================== Kimchi premium ==================
kimchi_pct, kp_meta = get_kimchi(df)
kp_is_cache = bool(kp_meta.get("is_cache")) if kp_meta else False
# 12시간 이상 캐시면 '구캐시' 표시
if kp_is_cache and kp_meta and (time.time() - kp_meta.get("ts", 0) > 12*3600):
    kp_is_cache = True
kp_text_base = fmt_pct(kimchi_pct, 2) if kimchi_pct is not None else "잠정(전일)"
kp_text = kp_text_base + (" (캐시)" if kp_is_cache else "")

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

    body = " ".join([
        f"BM20 지수가 {YMD} 전일 대비 {pct(bm20_chg)} {trend_word}해 {num2(bm20_now)}포인트를 기록했다.",
        breadth,
        best_line, worst_line,
        btc_line, eth_line,
        kp_line
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

# B) BTC/ETH 7일 추세 (Yahoo Finance API 직접 호출)
def get_pct_series_yf(ticker, days=8):
    try:
        import datetime as _dt3
        period1 = int((_dt3.datetime.utcnow() - _dt3.timedelta(days=days+2)).timestamp())
        period2 = int((_dt3.datetime.utcnow() + _dt3.timedelta(days=1)).timestamp())
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        r = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0"},
            params={"interval": "1d", "period1": period1, "period2": period2},
            timeout=10,
        )
        r.raise_for_status()
        result = r.json()["chart"]["result"][0]
        closes = [c for c in result["indicators"]["quote"][0]["close"] if c is not None]
        if not closes: return []
        base = closes[0]
        return [(v / base - 1.0) * 100.0 for v in closes]
    except Exception as e:
        print(f"[WARN] trend fetch failed for {ticker}: {e}")
        return []

btc7=get_pct_series_yf("BTC-USD", 8); time.sleep(0.2)
eth7=get_pct_series_yf("ETH-USD", 8)
plt.figure(figsize=(10.6, 3.8))
if btc7: plt.plot(range(len(btc7)), btc7, label="BTC")
if eth7: plt.plot(range(len(eth7)), eth7, label="ETH")
if btc7 or eth7: plt.legend(loc="upper left")
plt.title("BTC & ETH 7일 가격 추세", fontsize=13, loc="left", pad=8)
plt.ylabel("% (from start)"); plt.tight_layout(); plt.savefig(trend_png, dpi=180); plt.close()

# ================== Returns (backfill_current_basket.csv SSOT 기반) ==================
HIST_DIR = OUT_DIR / "history"; HIST_DIR.mkdir(parents=True, exist_ok=True)

# 수익률 계산: rows_ssot (backfill_current_basket.csv) 기반
def period_return_ssot(days: int):
    if not rows_ssot or len(rows_ssot) < 2:
        return None
    try:
        ref_date = (datetime.strptime(YMD, "%Y-%m-%d") - timedelta(days=days)).strftime("%Y-%m-%d")
        candidates = [r for r in rows_ssot if r["date"] <= ref_date]
        if not candidates:
            return None
        ref_idx = float(candidates[-1]["level"])
        cur_idx = float(bm20_now)
        if ref_idx == 0:
            return None
        return (cur_idx / ref_idx - 1.0) * 100.0
    except Exception:
        return None

def level_on_or_before_ssot(yyyymmdd: str):
    candidates = [r for r in (rows_ssot or []) if r["date"] <= yyyymmdd]
    return float(candidates[-1]["level"]) if candidates else None

RET_1D  = period_return_ssot(1)
RET_7D  = period_return_ssot(7)
RET_30D = period_return_ssot(30)

today_dt = datetime.strptime(YMD, "%Y-%m-%d")
month_start = today_dt.replace(day=1).strftime("%Y-%m-%d")
year_start  = today_dt.replace(month=1, day=1).strftime("%Y-%m-%d")

lvl_month = level_on_or_before_ssot(month_start)
lvl_year  = level_on_or_before_ssot(year_start)
lvl_now   = float(bm20_now)
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
# bm20_series.json은 yaml의 별도 스텝에서 backfill 업데이트 후 생성 (여기서 저장 안 함)
LATEST_JSON.write_text(json.dumps(latest_obj, ensure_ascii=False, indent=2), encoding="utf-8")
print(f"[OK] Written: {LATEST_JSON}")


# Series JSON은 위에서 rows_ssot 기반으로 이미 저장됨 (중복 저장 제거)


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
]
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
    kp_text=kp_text,
    best=[{"sym":r["sym"], "pct": f"{r['price_change_pct']:+.2f}%"} for _,r in best.iterrows() if not np.isnan(r["price_change_pct"])],
    worst=[{"sym":r["sym"], "pct": f"{r['price_change_pct']:+.2f}%"} for _,r in worst.iterrows() if not np.isnan(r["price_change_pct"])],
    bar_png=os.path.basename(bar_png), trend_png=os.path.basename(trend_png),
    news_html=news.replace("\n","<br/>"),
    ts=TS
)
with open(html_path, "w", encoding="utf-8") as f: f.write(html)

# 마지막 단계: 나스닥 데이터 업데이트 실행
update_market_indices()

# ================== Market History CSV (append) ==================
MARKET_HIST_CSV = HIST_DIR / "market_history.csv"

def _get_btc_dominance_cmc() -> float | None:
    """BTC 도미넌스 — CoinMarketCap /global-metrics"""
    api_key = os.getenv("CMC_API_KEY") or os.getenv("COINMARKETCAP_API_KEY")
    if not api_key:
        print("[WARN] CMC_API_KEY 없음 — btc_dominance 스킵")
        return None
    try:
        r = requests.get(
            "https://pro-api.coinmarketcap.com/v1/global-metrics/quotes/latest",
            headers={"X-CMC_PRO_API_KEY": api_key},
            timeout=10,
        )
        r.raise_for_status()
        pct = r.json()["data"]["btc_dominance"]
        return round(float(pct), 2)
    except Exception as e:
        print(f"[WARN] BTC dominance CMC failed: {e}")
        return None


def _append_market_history():
    """매일 market_history.csv 에 한 줄 append.

    CSV 컬럼 순서(고정):
      date, bm20_level, bm20_chg_pct,
      sentiment_value, sentiment_label,
      kimchi_pct, usdkrw, k_share_percent,
      btc_dominance
    """
    btc_dominance = _get_btc_dominance_cmc()

    # ── sentiment (공포탐욕) — alternative.me ──────────────────────
    sentiment_value = None
    sentiment_label = None
    try:
        r = requests.get(
            "https://api.alternative.me/fng/?limit=1&format=json",
            timeout=10,
        )
        r.raise_for_status()
        fng = r.json().get("data", [{}])[0]
        sentiment_value = int(fng.get("value", 0)) or None
        sentiment_label = fng.get("value_classification") or None
    except Exception as e:
        print(f"[WARN] FNG fetch failed: {e}")

    # ── k_share_percent — k_xrp_share_24h_latest.json ──────────────
    k_share_percent = None
    try:
        kxrp_path = ROOT / "out" / "global" / "k_xrp_share_24h_latest.json"
        if kxrp_path.exists():
            kxrp = json.loads(kxrp_path.read_text(encoding="utf-8"))
            k_share_percent = round(float(kxrp.get("k_share_pct", 0) or 0), 4) or None
    except Exception as e:
        print(f"[WARN] k_share fetch failed: {e}")

    def _safe_round(v, n=6):
        try:
            return round(float(v), n) if v is not None else None
        except Exception:
            return None

    # ── CSV 컬럼과 1:1 매핑 ─────────────────────────────────────────
    COLUMNS = [
        "date", "bm20_level", "bm20_chg_pct",
        "sentiment_value", "sentiment_label",
        "kimchi_pct", "usdkrw", "k_share_percent",
        "btc_dominance",
    ]

    row = {
        "date":             YMD,
        "bm20_level":       round(float(bm20_now), 4),
        "bm20_chg_pct":     round(float(bm20_chg), 4),
        "sentiment_value":  sentiment_value,
        "sentiment_label":  sentiment_label,
        "kimchi_pct":       round(float(kimchi_pct), 4) if kimchi_pct is not None else None,
        "usdkrw":           round(float(kp_meta.get("usdkrw", 0)), 2) if kp_meta else None,
        "k_share_percent":  k_share_percent,
        "btc_dominance":    btc_dominance,
    }

    if MARKET_HIST_CSV.exists():
        hist_df = pd.read_csv(MARKET_HIST_CSV, dtype={"date": str})
        # 기존 CSV에 없는 컬럼 추가 (하위 호환)
        for col in COLUMNS:
            if col not in hist_df.columns:
                hist_df[col] = None
        hist_df = hist_df[COLUMNS]           # 컬럼 순서 고정
        hist_df = hist_df[hist_df["date"] != YMD]  # 오늘 중복 제거
    else:
        hist_df = pd.DataFrame(columns=COLUMNS)

    new_row_df = pd.DataFrame([row], columns=COLUMNS)
    hist_df = pd.concat([hist_df, new_row_df], ignore_index=True)
    hist_df.to_csv(MARKET_HIST_CSV, index=False, encoding="utf-8")

    print(
        f"[OK] market_history.csv → {len(hist_df)}행 "
        f"(date={YMD}, sentiment={sentiment_value}/{sentiment_label}, "
        f"btc_dom={btc_dominance}%, k_share={k_share_percent}%)"
    )


# _append_market_history() → yaml의 별도 스텝에서 실행 (backfill 업데이트 이후)

# ================== Components History CSV (append) ==================
COMPONENTS_HIST_CSV = HIST_DIR / "components_history.csv"

def _append_components_history():
    """
    매일 종목별 데이터를 components_history.csv 에 append
    컬럼: date, symbol, weight, price, return_1d, contribution
    """
    rows = []
    for _, r in df.iterrows():
        rows.append({
            "date":         YMD,
            "symbol":       str(r["sym"]),
            "weight":       round(float(r["weight_ratio"]), 6),
            "price":        round(float(r["current_price"]), 6) if not np.isnan(r["current_price"]) else None,
            "return_1d":    round(float(r["price_change_pct"]), 4) if not np.isnan(r["price_change_pct"]) else None,
            "contribution": round(float(r["contribution"]), 8) if not np.isnan(r["contribution"]) else None,
        })

    COLUMNS = ["date", "symbol", "weight", "price", "return_1d", "contribution"]
    new_df = pd.DataFrame(rows, columns=COLUMNS)

    if COMPONENTS_HIST_CSV.exists():
        hist_df = pd.read_csv(COMPONENTS_HIST_CSV, dtype={"date": str})
        hist_df = hist_df[hist_df["date"] != YMD]  # 오늘 중복 제거
        hist_df = pd.concat([hist_df, new_df], ignore_index=True)
    else:
        hist_df = new_df

    hist_df.to_csv(COMPONENTS_HIST_CSV, index=False, encoding="utf-8")
    print(f"[OK] components_history.csv → {len(hist_df)}행 ({YMD}, {len(rows)}종목 추가)")


_append_components_history()

# ================== BM20 vs BTC Comparison JSON ==================
try:
    btc_series_path = ROOT / "btc_usd_series.json"
    bm20_series_path = Path("bm20_series.json")
    if btc_series_path.exists() and bm20_series_path.exists():
        btc_raw  = json.loads(btc_series_path.read_text(encoding="utf-8"))
        bm20_raw = json.loads(bm20_series_path.read_text(encoding="utf-8"))

        # bm20_series.json 포맷 처리 (list 또는 dict)
        if isinstance(bm20_raw, list):
            bm20_d = {r["date"]: r["level"] for r in bm20_raw}
        elif isinstance(bm20_raw, dict) and "series" in bm20_raw:
            bm20_d = {r["date"]: r["level"] for r in bm20_raw["series"]}
        else:
            bm20_d = {}

        # btc_usd_series.json 포맷 처리
        if isinstance(btc_raw, list):
            btc_d = {r["date"]: r["price"] for r in btc_raw}
        else:
            btc_d = {}

        if bm20_d and btc_d:
            btc_base = btc_d.get("2018-01-01", 1.0)
            comparison = []
            last_btc = None
            for d in sorted(bm20_d.keys()):
                if d in btc_d:
                    last_btc = round(btc_d[d] / btc_base * 100, 4)
                if last_btc is not None:
                    comparison.append({
                        "date": d,
                        "bm20": round(bm20_d[d], 4),
                        "btc":  last_btc
                    })
            comp_path = OUT / "bm20_comparison.json"
            comp_path.write_text(
                json.dumps(comparison, separators=(",", ":")),
                encoding="utf-8"
            )
            print(f"[OK] bm20_comparison.json → {len(comparison)}개 ({comparison[-1]['date']})")
        else:
            print("[WARN] bm20_comparison.json: 데이터 부족으로 스킵")
    else:
        print("[WARN] bm20_comparison.json: btc_usd_series.json 또는 bm20_series.json 없음")
except Exception as _ce:
    print(f"[WARN] bm20_comparison.json 생성 실패: {_ce}")

print(f"\n[SUCCESS] 모든 업데이트가 완료되었습니다. ({YMD})")
