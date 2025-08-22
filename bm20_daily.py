import os, json, time, random, math
from datetime import datetime, timedelta, timezone, date as _date
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

from bm20.price_sources.yahoo import get_price_usd  # ★ Yahoo 가격 소스 사용

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
BASE_CACHE_PATH = os.path.join(OUT_DIR, "base_cache.json")  # 2018-01-01 기준 포트폴리오 가치 캐시

# ------------------------- Constants ----------------------------
CG = "https://api.coingecko.com/api/v3"  # (김프 폴백에서만 사용)
REQ_UA = {"User-Agent":"Mozilla/5.0"}
REQ_TIMEOUT = 15

# BM20 Universe (20) — 현재 정의
UNIVERSE: List[str] = [
    "BTC","ETH","XRP","USDT","BNB",
    "DOGE","TON","SUI","SOL","ADA","AVAX","DOT","MATIC","LINK","LTC","ATOM","NEAR","APT","FIL","ICP"
]
CG_ID: Dict[str,str] = {
    "BTC":"bitcoin","ETH":"ethereum","XRP":"ripple","USDT":"tether","BNB":"binancecoin",
    "DOGE":"dogecoin","TON":"toncoin","SUI":"sui","SOL":"solana","ADA":"cardano","AVAX":"avalanche-2",
    "DOT":"polkadot","MATIC":"polygon","LINK":"chainlink","LTC":"litecoin","ATOM":"cosmos",
    "NEAR":"near","APT":"aptos","FIL":"filecoin","ICP":"internet-computer"
}

# 가중치 (BTC 30 / ETH 20 / XRP·USDT·BNB 5 / 나머지 균등 35%)
W: Dict[str,float] = {"BTC":0.30, "ETH":0.20, "XRP":0.05, "USDT":0.05, "BNB":0.05}
OTHERS = [s for s in UNIVERSE if s not in W]
eq = 0.35 / len(OTHERS)
for s in OTHERS:
    W[s] = round(eq, 6)

BASE_DATE_STR = "2018-01-01"

# ------------------------- HTTP Utils ---------------------------
def _get(url: str, params: dict=None, timeout: int=REQ_TIMEOUT, headers: dict=REQ_UA, retry: int=3, sleep: float=0.6):
    last = None
    for i in range(retry):
        try:
            r = requests.get(url, params=params, timeout=timeout, headers=headers)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            time.sleep(sleep*(i+1) + 0.2*random.random())
    raise last

def _safe_float(x, d: float=0.0) -> float:
    try: return float(x)
    except Exception: return d

# ------------------------- Market Snapshot (Yahoo) --------------
def fetch_markets(symbols: List[str]) -> pd.DataFrame:
    rows = []
    yday = (datetime.now(KST) - timedelta(days=1)).date()
    for s in symbols:
        cid = CG_ID[s]
        cur = get_price_usd(cid, None)
        try:
            prev = get_price_usd(cid, yday)
        except Exception:
            prev = None
        chg24 = 0.0 if (prev in (None, 0)) else ((cur / prev - 1.0) * 100.0)
        rows.append({"symbol": s,"current_price": float(cur),"market_cap": None,"chg24": float(chg24)})
        time.sleep(0.02)
    return pd.DataFrame(rows).set_index("symbol").reindex(symbols)

def fetch_yday_close(symbol: str) -> Optional[float]:
    yday = (datetime.now(KST) - timedelta(days=1)).date()
    try: return float(get_price_usd(CG_ID[symbol], yday))
    except Exception: return None

def fill_previous_prices(df: pd.DataFrame) -> pd.DataFrame:
    prevs = []
    for s in df.index:
        v = None
        try: v = fetch_yday_close(s)
        except Exception: v = None
        if v in (None, 0):
            cur = df.at[s, "current_price"]
            ch = (df.at[s, "chg24"] or 0.0)/100.0
            v = cur/(1+ch) if (cur and not math.isclose(ch, -1.0)) else cur
        prevs.append(v); time.sleep(0.01)
    df["previous_price"] = prevs
    return df

# ------------------------- Rebase -------------------------------
def fetch_price_on_2018(symbol: str) -> Optional[float]:
    try: return float(get_price_usd(CG_ID[symbol], _date(2018, 1, 1)))
    except Exception: return None

def get_base_value() -> float:
    if os.path.exists(BASE_CACHE_PATH):
        try:
            with open(BASE_CACHE_PATH, "r", encoding="utf-8") as f:
                c = json.load(f)
            if c.get("base_date") == BASE_DATE_STR and set(c.get("universe",[])) == set(UNIVERSE):
                return float(c["portfolio_value_usd"])
        except Exception: pass
    base_val = 0.0
    for s in UNIVERSE:
        p = fetch_price_on_2018(s)
        if p: base_val += p * W[s]
        time.sleep(0.01)
    with open(BASE_CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump({"base_date": BASE_DATE_STR,"universe": UNIVERSE,"weights": W,"portfolio_value_usd": round(base_val,10)}, f, ensure_ascii=False)
    return base_val

# ------------------------- Kimchi Premium -----------------------
def _fetch_usdkrw_yahoo() -> Optional[float]:
    try:
        j = _get("https://query1.finance.yahoo.com/v7/finance/quote", params={"symbols":"USDKRW=X"})
        return float(j["quoteResponse"]["result"][0]["regularMarketPrice"])
    except Exception: return None

def _fetch_btc_usd_binance() -> Optional[float]:
    try: return float(_get("https://api.binance.com/api/v3/ticker/price", params={"symbol":"BTCUSDT"})["price"])
    except Exception: return None

def _fetch_btc_krw_upbit() -> Optional[float]:
    try: return float(_get("https://api.upbit.com/v1/ticker", params={"markets":"KRW-BTC"})[0]["trade_price"])
    except Exception: return None

def compute_kimchi_premium(df: pd.DataFrame) -> Tuple[str, Dict]:
    btc_usd, btc_krw, usdkrw = _fetch_btc_usd_binance(), _fetch_btc_krw_upbit(), _fetch_usdkrw_yahoo()
    if btc_usd is None or btc_krw is None or usdkrw is None:
        try:
            j = _get(f"{CG}/simple/price", params={"ids":"bitcoin,tether","vs_currencies":"usd,krw"})
            btc_usd = btc_usd or float(j["bitcoin"]["usd"])
            btc_krw = btc_krw or float(j["bitcoin"].get("krw",0)) or None
            usdkrw = usdkrw or float(j["tether"]["krw"])
        except Exception: pass
    if btc_usd is None: btc_usd = float(df.loc["BTC","current_price"])
    if usdkrw is None: usdkrw = 1350.0
    if not all([btc_usd, btc_krw, usdkrw]) or usdkrw <= 0: return "—", {}
    prem = (btc_krw / (btc_usd*usdkrw)) - 1.0
    return f"{prem*100:+.2f}%", {}

# ------------------------- Funding ------------------------------
def _funding_binance(symbol: str) -> Optional[float]:
    try:
        j = _get("https://fapi.binance.com/fapi/v1/fundingRate", params={"symbol":symbol,"limit":1})
        if j and isinstance(j,list): return float(j[0]["fundingRate"])*100
    except Exception: return None
    return None

def _funding_bybit(symbol: str) -> Optional[float]:
    try:
        j = _get("https://api.bybit.com/v5/market/funding/history", params={"category":"linear","symbol":symbol,"limit":1})
        if j.get("retCode")==0 and j.get("result",{}).get("list"): return float(j["result"]["list"][0]["fundingRate"])*100
    except Exception: return None
    return None

def get_funding_display_pair() -> Tuple[str,str]:
    def fmt(x): return "중립권" if x is None else f"{x:+.4f}%"
    btc = _funding_binance("BTCUSDT") or _funding_bybit("BTCUSDT")
    eth = _funding_binance("ETHUSDT") or _funding_bybit("ETHUSDT")
    return fmt(btc), fmt(eth)

# ------------------------- Charts -------------------------------
def _set_dark_style():
    plt.rcParams.update({
        "figure.facecolor":"#0b1020","axes.facecolor":"#121831",
        "axes.edgecolor":"#28324d","axes.labelcolor":"#e6ebff",
        "xtick.color":"#cfd6ff","ytick.color":"#cfd6ff","text.color":"#e6ebff",
        "savefig.facecolor":"#0b1020","savefig.bbox":"tight","font.size":11,
    })

def plot_perf_barh(names: List[str], rets_pct: List[float], out_png: str):
    _set_dark_style(); import numpy as np
    y = np.arange(len(names)); vals = np.array(rets_pct,float)
    colors = np.where(vals>=0,"#2E7D32","#C62828")
    fig, ax = plt.subplots(figsize=(10,6), dpi=150)
    ax.barh(y, vals, color=colors); ax.axvline(0,color="#3a4569",lw=1)
    ax.set_yticks(y, labels=names); ax.set_xlabel("Daily Change (%)"); ax.invert_yaxis()
    for yi,v in zip(y,vals): ax.text(v+(0.05 if v>=0 else -0.05), yi, f"{v:+.2f}%", va="center", ha=("left" if v>=0 else "right"))
    ax.set_title(f"BM20 Daily Performance ({YMD})"); fig.savefig(out_png, dpi=180); plt.close(fig)

def plot_btc_eth_trend_7d(out_png: str):
    _set_dark_style()
    days = [(datetime.now(KST)-timedelta(days=i)).date() for i in range(6,-1,-1)]
    ts = [datetime.combine(d, datetime.min.time(), tzinfo=KST) for d in days]
    b_vals,e_vals=[],[]
    for d in days:
        try: b_vals.append(float(get_price_usd("bitcoin", d)))
        except: b_vals.append(None)
        try: e_vals.append(float(get_price_usd("ethereum", d)))
        except: e_vals.append(None)
        time.sleep(0.01)
    fig,ax=plt.subplots(figsize=(10,5),dpi=150)
    ax.plot(ts,b_vals,label="BTC"); ax.plot(ts,e_vals,label="ETH")
    ax.set_title("BTC · ETH — 7D Trend (USD)"); ax.legend(); fig.autofmt_xdate()
    fig.savefig(out_png,dpi=180); plt.close(fig)

# ------------------------- Main Pipeline ------------------------
def main():
    df = fetch_markets(UNIVERSE); df = fill_previous_prices(df)
    base_val = get_base_value(); df["weight"] = df.index.map(W)
    port_prev = float((df["previous_price"]*df["weight"]).sum())
    port_now = float((df["current_price"]*df["weight"]).sum())
    bm20_prev=(port_prev/base_val)*100 if base_val else 0.0
    bm20_now=(port_now/base_val)*100 if base_val else 0.0
    bm20_point_change=bm20_now-bm20_prev
    bm20_change_pct=((bm20_now/bm20_prev)-1)*100 if bm20_prev else 0.0
    rebased_multiple=(bm20_now/100.0) if bm20_now else 0.0
    df["ret_1d"]=(df["current_price"]/df["previous_price"]-1)*100
    up_count=int((df["ret_1d"]>0).sum()); down_count=int((df["ret_1d"]<0).sum())
    best3=df.sort_values("ret_1d",ascending=False).head(3)[["ret_1d"]]
    worst3=df.sort_values("ret_1d",ascending=True).head(3)[["ret_1d"]]
    best3_list=[[idx,float(r.ret_1d)] for idx,r in best3.itertuples()]
    worst3_list=[[idx,float(r.ret_1d)] for idx,r in worst3.itertuples()]
    kimchi_str,_=compute_kimchi_premium(df); fund_btc,fund_eth=get_funding_display_pair()
    btc_price=df.at["BTC","current_price"]; eth_price=df.at["ETH","current_price"]
    btc_ret=df.at["BTC","ret_1d"]; eth_ret=df.at["ETH","ret_1d"]
    bar_png=os.path.join(OUT_DIR_DATE,f"bm20_bar_{YMD}.png"); plot_perf_barh([x[0] for x in best3_list]+[x[0] for x in worst3_list],[x[1] for x in best3_list]+[x[1] for x in worst3_list],bar_png)
    trend_png=os.path.join(OUT_DIR_DATE,f"bm20_trend_{YMD}.png")
    try: plot_btc_eth_trend_7d(trend_png)
    except: pass
    d={"asOf":YMD,"bm20Level":round(bm20_now,2),"bm20PrevLevel":round(bm20_prev,2),"bm20PointChange":round(bm20_point_change,2),
       "bm20ChangePct":round(bm20_change_pct,2),"rebasedMultiple":round(rebased_multiple,2),
       "total":len(UNIVERSE),"upCount":up_count,"downCount":down_count,"breadth":f"{up_count} ↑ / {down_count} ↓",
       "best3":best3_list,"worst3":worst3_list,
       "btcPrice":f"${btc_price:,.0f}","btcChangePct":round(float(btc_ret),2),
       "ethPrice":f"${eth_price:,.0f}","ethChangePct":round(float(eth_ret),2),
       "kimchi":kimchi_str,"funding":{"btc":fund_btc,"eth":fund_eth}}
    with open(os.path.join(SITE_DIR,"bm20_latest.json"),"w",encoding="utf-8") as f: json.dump(d,f,ensure_ascii=False)
    df_out=df[["current_price","previous_price","ret_1d"]].copy(); df_out["name"]=df_out.index; df_out["weight_ratio"]=df_out.index.map(W)
    df_out.to_csv(os.path.join(OUT_DIR_DATE,f"bm20_daily_data_{YMD}.csv"),index=False,encoding="utf-8")
    with open(os.path.join(OUT_DIR_DATE,f"bm20_news_{YMD}.txt"),"w",encoding="utf-8") as f: f.write(str(d))
    if REPORTLAB_AVAILABLE:
        pdf_path=os.path.join(OUT_DIR_DATE,f"bm20_daily_{YMD}.pdf")
        c=canvas.Canvas(pdf_path,pagesize=A4); w,h=A4; margin=1.5*cm; y=h-margin
        c.setFont("Helvetica-Bold",14); c.drawString(margin,y,f"BM20 데일리 리포트 {YMD}"); y-=0.8*cm; c.setFont("Helvetica",10)
        text=str(d); lines=[text[i:i+68] for i in range(0,len(text),68)]
        for seg in lines: c.drawString(margin,y,seg); y-=0.5*cm
        if os.path.exists(bar_png): img_w=w-2*margin; img_h=img_w*0.5; c.drawImage(bar_png,margin,margin,width=img_w,height=img_h,preserveAspectRatio=True,anchor='sw')
        c.showPage(); c.save()
    print("Saved:",os.path.join(OUT_DIR_DATE,f"bm20_daily_data_{YMD}.csv"),os.path.join(OUT_DIR_DATE,f"bm20_news_{YMD}.txt"),bar_png,trend_png,os.path.join(SITE_DIR,"bm20_latest.json"))

if __name__=="__main__":
    main()
