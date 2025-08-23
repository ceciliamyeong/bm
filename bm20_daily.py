# ===================== BM20 Daily (Yahoo-safe, 2025-08) =====================
# 기능: 유니버스/가중치(확정) → 시세 수집 (Yahoo v7 + v8 폴백) → 기준가(기본 2024-01-01=100)
#      → 지수 산출 → 김치 프리미엄 · 펀딩비 → Best/Worst → 뉴스 문장 → 차트
# 산출물: out/YYYY-MM-DD/ (csv/txt/png/pdf), site/bm20_latest.json
#
# 환경변수:
#   - OUT_DIR: 출력 루트 (기본 out)
#   - BM20_BASE_DATE: 기준일(YYYY-MM-DD), 기본 2024-01-01
#   - BM20_BASE_EXCLUDE_CONSISTENT=1 → 기준가에서 제외된 코인을 분자(당일/전일 가치)에서도 제외

import os, json, time, math, random
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Optional

import requests
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

try:
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas
    from reportlab.lib.units import cm
    REPORTLAB_AVAILABLE = True
except Exception:
    REPORTLAB_AVAILABLE = False

# ------------------------- Paths / Dates -------------------------
OUT_DIR = os.getenv("OUT_DIR", "out")
os.makedirs(OUT_DIR, exist_ok=True)
KST = timezone(timedelta(hours=9))
TODAY = datetime.now(KST)
YMD = TODAY.strftime("%Y-%m-%d")
OUT_DIR_DATE = os.path.join(OUT_DIR, YMD)
os.makedirs(OUT_DIR_DATE, exist_ok=True)

SITE_DIR = "site"
os.makedirs(SITE_DIR, exist_ok=True)

# ------------------------- Universe & Weights --------------------
UNIVERSE: List[str] = [
    "BTC","ETH","XRP","USDT","BNB",
    "DOGE","TON","SUI","SOL","ADA","AVAX","DOT","MATIC",
    "LINK","LTC","ATOM","NEAR","APT","FIL","ICP"
]

# Yahoo Finance 티커 매핑 (Toncoin 고정: TON11419-USD)
YF_TICKER: Dict[str,str] = {
    "BTC":"BTC-USD","ETH":"ETH-USD","XRP":"XRP-USD","USDT":"USDT-USD","BNB":"BNB-USD",
    "DOGE":"DOGE-USD","TON":"TON11419-USD","SUI":"SUI-USD","SOL":"SOL-USD","ADA":"ADA-USD",
    "AVAX":"AVAX-USD","DOT":"DOT-USD","MATIC":"MATIC-USD","LINK":"LINK-USD","LTC":"LTC-USD",
    "ATOM":"ATOM-USD","NEAR":"NEAR-USD","APT":"APT-USD","FIL":"FIL-USD","ICP":"ICP-USD"
}

# 가중치 (BTC 30 / ETH 20 / XRP·USDT·BNB 각 5 / 기타 35% 균등)
W: Dict[str,float] = {"BTC":0.30,"ETH":0.20,"XRP":0.05,"USDT":0.05,"BNB":0.05}
OTHERS = [s for s in UNIVERSE if s not in W]
eq = 0.35/len(OTHERS)
for s in OTHERS:
    W[s] = round(eq,6)

# 기준일 (환경변수로 제어; 기본 2024-01-01)
BASE_DATE_STR = os.getenv("BM20_BASE_DATE", "2024-01-01")
BASE_CACHE_PATH = os.path.join(OUT_DIR, "base_cache.json")

# ------------------------- HTTP util ------------------------------
UA = {"User-Agent":"Mozilla/5.0"}
def _get(url, params=None, retry=3, sleep=0.6):
    last=None
    for i in range(retry):
        try:
            r = requests.get(url, params=params, timeout=15, headers=UA)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last=e; time.sleep(sleep*(i+1) + 0.2*random.random())
    raise last

def _safe_float(x, d: Optional[float]=None) -> Optional[float]:
    try: return float(x)
    except Exception: return d

# ------------------------- Yahoo helpers --------------------------
def _yf_quote_v7(symbols: List[str]) -> Dict[str, dict]:
    tickers = ",".join([YF_TICKER[s] for s in symbols if s in YF_TICKER])
    if not tickers: return {}
    j = _get("https://query1.finance.yahoo.com/v7/finance/quote", {"symbols": tickers})
    out = {}
    for q in j.get("quoteResponse", {}).get("result", []):
        t = q.get("symbol")
        out[t] = {
            "price": q.get("regularMarketPrice"),
            "prev_close": q.get("regularMarketPreviousClose"),
            "change_pct": q.get("regularMarketChangePercent"),
            "market_cap": q.get("marketCap"),
        }
    return out

def _yf_quote_single_via_chart(yf_ticker: str) -> Dict[str, Optional[float]]:
    """v8 chart로 2일 일봉 → 현재가/전일가. 인증/쿠키 불필요."""
    try:
        j = _get(f"https://query2.finance.yahoo.com/v8/finance/chart/{yf_ticker}",
                 {"range":"2d","interval":"1d"})
        res = j["chart"]["result"][0]
        closes = res["indicators"]["quote"][0]["close"]
        if not closes: return {"price": None, "prev_close": None}
        if len(closes)==1: return {"price": _safe_float(closes[0]), "prev_close": None}
        return {"price": _safe_float(closes[-1]), "prev_close": _safe_float(closes[-2])}
    except Exception:
        return {"price": None, "prev_close": None}

def _yf_quote(symbols: List[str]) -> Dict[str, dict]:
    """v7 일괄 → 실패(401 등)시 v8 개별 폴백"""
    try:
        return _yf_quote_v7(symbols)
    except requests.HTTPError as e:
        if getattr(e.response,"status_code",None) != 401:
            raise
    except Exception:
        pass
    # fallback to chart
    out={}
    for s in symbols:
        t = YF_TICKER[s]
        out[t] = _yf_quote_single_via_chart(t)
        time.sleep(0.08)
    return out

# 안전한 기준일 가격 로더: 날짜 없으면 '최초 가용 종가' 폴백
def _yf_price_on_date(yf_ticker: str, date_ymd: str) -> Optional[float]:
    try:
        j = _get(f"https://query2.finance.yahoo.com/v8/finance/chart/{yf_ticker}",
                 {"range":"max","interval":"1d"})
        ch = j.get("chart", {})
        if ch.get("error"): return None
        res = (ch.get("result") or [None])[0]
        if not res: return None
        ts = res.get("timestamp") or []
        cl = (res.get("indicators",{}).get("quote",[{}])[0].get("close")) or []
        if not ts or not cl: return None
        target = datetime.fromisoformat(date_ymd).date()
        for t,v in zip(ts,cl):
            if v is None: continue
            if datetime.fromtimestamp(t, timezone.utc).date() == target:
                return float(v)
        for v in cl:
            if v is not None: return float(v)
        return None
    except Exception:
        return None

def fetch_price_on_2018(symbol:str)->Optional[float]:
    # 이름은 호환성 유지, 실사용 기준일은 BASE_DATE_STR
    return _yf_price_on_date(YF_TICKER[symbol], BASE_DATE_STR)

# ------------------------- Market Snapshot ------------------------
def fetch_markets(symbols: List[str]) -> pd.DataFrame:
    q = _yf_quote(symbols)  # v7 or fallback v8
    rows = []
    for s in symbols:
        t = YF_TICKER[s]
        qq = q.get(t, {}) or {}
        price = _safe_float(qq.get("price"))
        prev  = _safe_float(qq.get("prev_close"))
        chg   = _safe_float(qq.get("change_pct"))
        # v8 폴백에는 change_pct가 없을 수 있으므로 계산
        if chg is None and price not in (None,0) and prev not in (None,0):
            chg = (price/prev - 1) * 100.0
        rows.append({
            "symbol": s,
            "current_price": price if price is not None else 0.0,
            "previous_price": prev,   # 후속 보정
            "ret_1d": chg
        })
    return pd.DataFrame(rows).set_index("symbol").reindex(symbols)

def fetch_yday_close(symbol: str) -> Optional[float]:
    t = YF_TICKER[symbol]
    qq = _yf_quote_single_via_chart(t)
    return _safe_float(qq.get("prev_close"))

def fill_previous_prices(df:pd.DataFrame)->pd.DataFrame:
    prevs = []
    for s in df.index:
        v = df.at[s, "previous_price"]
        if v in (None, 0) or (isinstance(v,float) and math.isnan(v)):
            try:
                v = fetch_yday_close(s)
            except Exception:
                v = None
        if v in (None, 0):
            cur = df.at[s, "current_price"]
            ch = df.at[s, "ret_1d"]
            if cur not in (None,0) and ch not in (None,):
                try: v = cur/(1+float(ch)/100.0)
                except Exception: v = cur
        prevs.append(v if v is not None else 0.0)
        time.sleep(0.03)
    df["previous_price"] = prevs
    return df

# ------------------------- Rebase (BASE_DATE_STR=100) -------------
def get_base_value() -> Tuple[float, List[str]]:
    # 캐시 재사용 (기준일/유니버스 매칭 시)
    if os.path.exists(BASE_CACHE_PATH):
        try:
            with open(BASE_CACHE_PATH,"r",encoding="utf-8") as f:
                c=json.load(f)
            if c.get("base_date")==BASE_DATE_STR and set(c.get("universe",[]))==set(UNIVERSE):
                return float(c["portfolio_value_usd"]), c.get("excluded",[])
        except Exception:
            pass

    base_val = 0.0
    excluded: List[str] = []
    for s in UNIVERSE:
        p = fetch_price_on_2018(s)   # BASE_DATE_STR 사용
        if p in (None, 0):
            excluded.append(s)       # ✅ 옵션 A: 기준가 합산에서만 제외
            continue
        base_val += p * W[s]
        time.sleep(0.03)

    with open(BASE_CACHE_PATH,"w",encoding="utf-8") as f:
        json.dump({
            "base_date": BASE_DATE_STR,
            "universe": UNIVERSE,
            "weights": W,
            "portfolio_value_usd": round(base_val,10),
            "excluded": excluded
        }, f, ensure_ascii=False)
    return base_val, excluded

# ------------------------- Kimchi/Funding -------------------------
def _fetch_usd_krw() -> Optional[float]:
    try:
        j=_get("https://query1.finance.yahoo.com/v7/finance/quote", {"symbols":"USDKRW=X"})
        return float(j["quoteResponse"]["result"][0]["regularMarketPrice"])
    except Exception:
        return None

def _fetch_btc_usd() -> Optional[float]:
    try:
        j=_get("https://api.binance.com/api/v3/ticker/price", {"symbol":"BTCUSDT"})
        return float(j["price"])
    except Exception:
        return None

def _fetch_btc_krw() -> Optional[float]:
    try:
        j=_get("https://api.upbit.com/v1/ticker", {"markets":"KRW-BTC"})
        return float(j[0]["trade_price"])
    except Exception:
        return None

def compute_kimchi_premium() -> str:
    usdkrw = _fetch_usd_krw() or 1350.0
    btc_usd = _fetch_btc_usd()
    btc_krw = _fetch_btc_krw()
    if not all([btc_usd, btc_krw, usdkrw]): return "—"
    prem=(btc_krw/(btc_usd*usdkrw))-1.0
    return f"{prem*100:+.2f}%"

def get_funding()->Tuple[str,str]:
    def f(sym):
        try:
            j=_get("https://fapi.binance.com/fapi/v1/fundingRate", {"symbol":sym, "limit":1})
            return f"{float(j[0]['fundingRate'])*100:+.4f}%"
        except Exception:
            return "중립권"
    return f("BTCUSDT"), f("ETHUSDT")

# ------------------------- Charts ---------------------------------
def _set_dark():
    plt.rcParams.update({
        "figure.facecolor":"#0b1020","axes.facecolor":"#121831",
        "axes.labelcolor":"#e6ebff","xtick.color":"#cfd6ff","ytick.color":"#cfd6ff",
        "text.color":"#e6ebff","font.size":11})

def plot_bar(names,vals,out_png):
    _set_dark(); import numpy as np
    y=np.arange(len(names)); colors=np.where(np.array(vals)>=0,"#2E7D32","#C62828")
    fig,ax=plt.subplots(figsize=(10,6),dpi=150)
    ax.barh(y,vals,color=colors,alpha=0.95); ax.axvline(0,color="#3a4569",lw=1)
    ax.set_yticks(y,labels=names); ax.invert_yaxis()
    for yi,v in zip(y,vals):
        ax.text(v+(0.05 if v>=0 else -0.05), yi, f"{v:+.2f}%", va="center",
                ha=("left" if v>=0 else "right"))
    ax.set_title(f"BM20 Daily Performance  ({YMD})")
    fig.savefig(out_png,dpi=180); plt.close(fig)

# ------------------------- News Builder ---------------------------
def build_news(d:Dict)->str:
    return (
        f"BM20 지수는 {d['asOf']} 전일 대비 {d['bm20ChangePct']:+.2f}% 변동해 "
        f"{d['bm20Level']:.0f}pt를 기록했습니다. {BASE_DATE_STR}(=100) 대비 "
        f"{d['rebasedMultiple']:.2f}배 수준입니다. 구성 종목 {d['total']}개 중 "
        f"상승 {d['upCount']}·하락 {d['downCount']}였고, "
        f"상승 상위 {', '.join([f'{s}({v:+.2f}%)' for s,v in d['best3']])}, "
        f"하락 상위 {', '.join([f'{s}({v:+.2f}%)' for s,v in d['worst3']])}였습니다. "
        f"BTC는 {d['btcPrice']}에서 {d['btcChangePct']:+.2f}%, ETH는 {d['ethPrice']}에서 "
        f"{d['ethChangePct']:+.2f}%였고, 김치 프리미엄은 {d['kimchi']}입니다. "
        f"펀딩비는 BTC {d['funding']['btc']} / ETH {d['funding']['eth']}."
    )

# ------------------------- Main ----------------------------------
def main():
    # 1) Market snapshot
    df=fetch_markets(UNIVERSE); df=fill_previous_prices(df)

    # 2) Base (옵션 A 일반화)
    base_val, excluded_for_base = get_base_value()

    # 3) 분자 집계: 기본은 전 종목 포함; 일관 모드면 제외 반영
    consistent = os.getenv("BM20_BASE_EXCLUDE_CONSISTENT","0")=="1"
    df_num = df.drop(index=[s for s in excluded_for_base if s in df.index]) if consistent else df.copy()
    df_num["weight"]=df_num.index.map(W)

    port_prev=float((df_num["previous_price"]*df_num["weight"]).sum())
    port_now =float((df_num["current_price"] *df_num["weight"]).sum())

    bm20_prev=(port_prev/base_val)*100 if base_val else 0.0
    bm20_now =(port_now /base_val)*100 if base_val else 0.0
    bm20_point_change = bm20_now - bm20_prev
    bm20_change_pct = ((bm20_now/bm20_prev)-1)*100 if bm20_prev else 0.0
    rebased_multiple = (bm20_now/100.0) if bm20_now else 0.0

    # 4) Per-asset returns & breadth
    df["ret_1d"]=(df["current_price"]/df["previous_price"]-1)*100
    df["ret_1d"]=pd.to_numeric(df["ret_1d"],errors="coerce")
    up_count=int((df["ret_1d"]>0).sum()); down_count=int((df["ret_1d"]<0).sum())

    best3=df.sort_values("ret_1d",ascending=False).head(3)
    worst3=df.sort_values("ret_1d",ascending=True).head(3)
    best3_list=[[i,float(r["ret_1d"]) if pd.notna(r["ret_1d"]) else 0.0] for i,r in best3.iterrows()]
    worst3_list=[[i,float(r["ret_1d"]) if pd.notna(r["ret_1d"]) else 0.0] for i,r in worst3.iterrows()]

    # 5) Kimchi & Funding
    kimchi=compute_kimchi_premium(); fbtc,feth=get_funding()

    # 6) BTC/ETH snapshot for text
    btc_price=df.at["BTC","current_price"] if "BTC" in df.index else None
    eth_price=df.at["ETH","current_price"] if "ETH" in df.index else None
    btc_ret=df.at["BTC","ret_1d"] if "BTC" in df.index else 0.0
    eth_ret=df.at["ETH","ret_1d"] if "ETH" in df.index else 0.0

    # 7) Charts
    bar_png=os.path.join(OUT_DIR_DATE,f"bm20_bar_{YMD}.png")
    names=[x[0] for x in best3_list+worst3_list]; vals=[x[1] for x in best3_list+worst3_list]
    plot_bar(names, vals, bar_png)

    # 8) JSON for site
    d = {
        "asOf": YMD,
        "bm20Level": round(bm20_now, 2),
        "bm20PrevLevel": round(bm20_prev, 2),
        "bm20PointChange": round(bm20_point_change, 2),
        "bm20ChangePct": round(bm20_change_pct, 2),
        "rebasedMultiple": round(rebased_multiple, 2),
        "total": len(UNIVERSE),
        "upCount": up_count,
        "downcount": down_count,
        "breadth": f"{up_count} ↑ / {down_count} ↓",
        "best3": best3_list,
        "worst3": worst3_list,
        "btcPrice": (f"${btc_price:,.0f}" if btc_price else "—"),
        "btcChangePct": round(float(btc_ret), 2) if isinstance(btc_ret,(int,float)) else 0.0,
        "ethPrice": (f"${eth_price:,.0f}" if eth_price else "—"),
        "ethChangePct": round(float(eth_ret), 2) if isinstance(eth_ret,(int,float)) else 0.0,
        "kimchi": kimchi,
        "funding": {"btc": fbtc, "eth": feth},
    }
    d["news"]=build_news(d)

    os.makedirs(SITE_DIR, exist_ok=True)
    with open(os.path.join(SITE_DIR,"bm20_latest.json"),"w",encoding="utf-8") as f:
        json.dump(d,f,ensure_ascii=False)

    # 9) CSV / TXT
    df_out=df[["current_price","previous_price","ret_1d"]].copy()
    df_out["name"]=df_out.index; df_out["weight_ratio"]=df_out.index.map(W)
    df_out=df_out[["name","current_price","previous_price","weight_ratio","ret_1d"]]
    df_out.to_csv(os.path.join(OUT_DIR_DATE,f"bm20_daily_data_{YMD}.csv"),index=False,encoding="utf-8")

    with open(os.path.join(OUT_DIR_DATE,f"bm20_news_{YMD}.txt"),"w",encoding="utf-8") as f:
        f.write(d["news"])

    # 10) PDF (선택)
    if REPORTLAB_AVAILABLE:
        pdf_path = os.path.join(OUT_DIR_DATE, f"bm20_daily_{YMD}.pdf")
        c = canvas.Canvas(pdf_path, pagesize=A4)
        w, h = A4; margin = 1.5*cm; y = h - margin
        c.setFont("Helvetica-Bold", 14); c.drawString(margin, y, f"BM20 데일리 리포트  {YMD}")
        y -= 0.8*cm; c.setFont("Helvetica", 10)
        text = d["news"]; line_len = 68
        lines = [text[i:i+line_len] for i in range(0, len(text), line_len)]
        for seg in lines:
            c.drawString(margin, y, seg); y -= 0.5*cm
        if os.path.exists(bar_png):
            y -= 0.3*cm; img_w = w - 2*margin; img_h = img_w * 0.5
            c.drawImage(bar_png, margin, margin, width=img_w, height=img_h, preserveAspectRatio=True, anchor='sw')
        c.showPage(); c.save()

    print("Saved:",
          os.path.join(OUT_DIR_DATE,f"bm20_daily_data_{YMD}.csv"),
          bar_png,
          os.path.join(SITE_DIR,"bm20_latest.json"),
          "excluded_from_base:", excluded_for_base)

if __name__=="__main__":
    main()
