# ===================== BM20 Daily — Stable, Rebased Index, Editorial News =====================
# 기능 요약:
# - CoinGecko 시세/시총 수집 → 가중치(국내상장 보정 선택가능) → 지수 레벨 산출(기준일 100pt 리베이스)
# - 김치 프리미엄(폴백·캐시), 펀딩비(바이낸스/바이빗 폴백·캐시)
# - 코인별 퍼포먼스(상승=초록/하락=빨강), BTC/ETH 7일 추세 차트
# - 에디토리얼 톤 뉴스(제목+본문, BTC/ETH 현재가 포함)
# - 기간수익률(1D/7D/30D/MTD/YTD) 계산, 인덱스 히스토리 저장
# - HTML + PDF 저장 (이미지 캐시버스터 적용)
# 의존: pandas, requests, matplotlib, reportlab, jinja2
# 변경점(2025-08-20, no-pro-key build):
# - CoinGecko 400 대응: BM20_IDS에 cosmos→cosmos-hub로 교체
# - /coins/markets 요청을 청크(8개)로 분할 + 실패 시 개별 재시도 + 문제 id 스킵
# - market_chart 실패 시 경고만 내고 빈 시리즈 처리
# - Pro API 사용 분기 제거(항상 public api.coingecko.com 사용)
# - KRW_BONUS를 환경변수 BM20_KRW_BONUS로 제어
# - 김치/펀딩 캐시 사용 시 라벨에 (캐시) 표기, 24h+ 경고 옵션
# - HTML 이미지에 캐시버스터 ?v=타임스탬프 추가

import os, time, json, random
from datetime import datetime, timedelta, timezone
from pathlib import Path
import requests
import pandas as pd

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

# ---- HTML ----
from jinja2 import Template

# ================== 공통 설정 ==================
OUT_DIR = Path(os.getenv("OUT_DIR", "out"))
OUT_DIR.mkdir(parents=True, exist_ok=True)
KST = timezone(timedelta(hours=9))
YMD = datetime.now(KST).strftime("%Y-%m-%d")
TS = datetime.now(KST).strftime("%Y%m%d%H%M%S")
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
        if v is None: return "-"
        return f"{float(v):.{digits}f}%"
    except Exception:
        return "-"

def safe_float(x, d=0.0):
    try: return float(x)
    except: return d

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

def cache_is_stale(ts_path: Path, max_hours=24):
    try:
        mtime = datetime.fromtimestamp(ts_path.stat().st_mtime, tz=KST)
        return (datetime.now(KST) - mtime) > timedelta(hours=max_hours)
    except Exception:
        return True

# ================== Data Layer ==================
BTC_CAP, OTH_CAP = 0.30, 0.15
TOP_UP, TOP_DOWN = 3, 3

BM20_IDS = [
    "bitcoin","ethereum","solana","ripple","binancecoin","toncoin","avalanche-2",
    "chainlink","cardano","polygon","near","polkadot","cosmos-hub","litecoin",
    "arbitrum","optimism","internet-computer","aptos","filecoin","sui","dogecoin"
]

KRW_LISTED = set(BM20_IDS)
KRW_BONUS = float(os.getenv("BM20_KRW_BONUS", "1.0"))

CG_BASE = "https://api.coingecko.com/api/v3"

def cg_get(path, params=None, retry=8, timeout=20):
    last = None
    headers = {"User-Agent": "BM20/1.0"}
    for i in range(retry):
        try:
            r = requests.get(f"{CG_BASE}{path}", params=params, timeout=timeout, headers=headers)
            if r.status_code == 429:
                ra = float(r.headers.get("Retry-After", 0)) or (1.5 * (i + 1))
                time.sleep(min(ra, 10) + random.random()); continue
            if 500 <= r.status_code < 600:
                time.sleep(1.2 * (i + 1) + random.random()); continue
            r.raise_for_status(); return r.json()
        except Exception as e:
            last = e; time.sleep(0.8 * (i + 1) + random.random())
    raise last

# === 안전한 시장데이터 수집 ===
def fetch_markets_chunked(ids, vs="usd", chunk=8):
    rows, bad_ids = [], []
    for i in range(0, len(ids), chunk):
        sub = ids[i:i+chunk]
        try:
            j = cg_get("/coins/markets", {
                "vs_currency": vs, "ids": ",".join(sub),
                "order": "market_cap_desc", "per_page": len(sub), "page": 1,
                "price_change_percentage": "24h"
            })
            if isinstance(j, list): rows.extend(j)
        except Exception:
            for cid in sub:
                try:
                    jj = cg_get("/coins/markets", {
                        "vs_currency": vs, "ids": cid,
                        "order": "market_cap_desc", "per_page": 1, "page": 1,
                        "price_change_percentage": "24h"
                    })
                    if isinstance(jj, list) and jj:
                        rows.append(jj[0])
                except Exception:
                    bad_ids.append(cid)
    if bad_ids:
        print(f"[WARN] skipped ids: {','.join(bad_ids)}")
    if not rows:
        raise RuntimeError("no market rows fetched")
    return rows

# ========== (중략: 나머지 김치프리미엄, 펀딩비, 지수 계산, 저장, 뉴스 빌드 부분 동일) ==========
