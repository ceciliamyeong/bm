# ===================== BM20 Daily (Yahoo-safe, 2025-08) =====================
# 기능: 유니버스/가중치(확정) → 시세 수집 (Yahoo) → 기준가(2018-01-01=100)
#      → 지수 산출 → 김치 프리미엄 · 펀딩비 → Best/Worst → 뉴스 문장 → 차트
# 산출물: out/YYYY-MM-DD/ (csv/txt/png/pdf), site/bm20_latest.json

import os, json, time, math
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
BASE_CACHE_PATH = os.path.join(OUT_DIR, "base_cache.json")

# ------------------------- Universe & Weights --------------------
UNIVERSE: List[str] = [
    "BTC","ETH","XRP","USDT","BNB",
    "DOGE","TON","SUI","SOL","ADA","AVAX","DOT","MATIC",
    "LINK","LTC","ATOM","NEAR","APT","FIL","ICP"
]

YF_TICKER: Dict[str,str] = {
    "BTC":"BTC-USD","ETH":"ETH-USD","XRP":"XRP-USD","USDT":"USDT-USD","BNB":"BNB-USD",
    "DOGE":"DOGE-USD","TON":"TON11419-USD","SUI":"SUI-USD","SOL":"SOL-USD","ADA":"ADA-USD",
    "AVAX":"AVAX-USD","DOT":"DOT-USD","MATIC":"MATIC-USD","LINK":"LINK-USD","LTC":"LTC-USD",
    "ATOM":"ATOM-USD","NEAR":"NEAR-USD","APT":"APT-USD","FIL":"FIL-USD","ICP":"ICP-USD"
}

W: Dict[str,float] = {"BTC":0.30,"ETH":0.20,"XRP":0.05,"USDT":0.05,"BNB":0.05}
OTHERS = [s for s in UNIVERSE if s not in W]
eq = 0.35/len(OTHERS)
for s in OTHERS:
    W[s] = round(eq,6)

BASE_DATE_STR = "2018-01-01"

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
            last=e; time.sleep(sleep*(i+1))
    raise last

# ------------------------- Yahoo fetch ----------------------------
def _yf_price_on_date(yf_ticker: str, date_ymd: str):
    """range=max → 해당 날짜 없으면 최초가용 종가"""
    try:
        j=_get(f"https://query2.finance.yahoo.com/v8/finance/chart/{yf_ticker}",
               {"range":"max","interval":"1d"})
        ch=j.get("chart",{})
        if ch.get("error"): return None
        res=(ch.get("result") or [None])[0]
        if not res: return None
        ts=res.get("timestamp") or []
        cl=(res.get("indicators",{}).get("quote",[{}])[0].get("close")) or []
        if not ts or not cl: return None
        target=datetime.fromisoformat(date_ymd).date()
        for t,v in zip(ts,cl):
            if v is None: continue
            if datetime.fromtimestamp(t,timezone.utc).date()==target:
                return float(v)
        for v in cl:
            if v is not None: return float(v)
        return None
    except Exception:
        return None

def fetch_price_on_2018(symbol:str)->Optional[float]:
    return _yf_price_on_date(YF_TICKER[symbol], BASE_DATE_STR)

# ------------------------- Market Snapshot ------------------------
def fetch_markets(symbols: List[str]) -> pd.DataFrame:
    rows=[]
    for s in symbols:
        try:
            j=_get("https://query1.finance.yahoo.com/v7/finance/quote",
                   {"symbols":YF_TICKER[s]})
            q=j["quoteResponse"]["result"][0]
            rows.append({"symbol":s,
                         "current_price":float(q.get("regularMarketPrice") or 0),
                         "chg24":float(q.get("regularMarketChangePercent") or 0)})
        except Exception:
            rows.append({"symbol":s,"current_price":0.0,"chg24":0.0})
        time.sleep(0.2)
    return pd.DataFrame(rows).set_index("symbol").reindex(symbols)

def fill_previous_prices(df:pd.DataFrame)->pd.DataFrame:
    prevs=[]
    for s in df.index:
        cur=df.at[s,"current_price"]
        ch=(df.at[s,"chg24"]/100.0) if df.at[s,"chg24"] else 0
        v=cur/(1+ch) if cur and not math.isclose(ch,-1.0) else cur
        prevs.append(v)
    df["previous_price"]=prevs
    return df

# ------------------------- Rebase ---------------------------------
def get_base_value()->float:
    if os.path.exists(BASE_CACHE_PATH):
        try:
            c=json.load(open(BASE_CACHE_PATH,"r",encoding="utf-8"))
            if c.get("base_date")==BASE_DATE_STR:
                return float(c["portfolio_value_usd"])
        except: pass
    base=0.0
    for s in UNIVERSE:
        p=fetch_price_on_2018(s)
        if p: base+=p*W[s]
    json.dump({"base_date":BASE_DATE_STR,
               "portfolio_value_usd":round(base,10)},
              open(BASE_CACHE_PATH,"w",encoding="utf-8"))
    return base

# ------------------------- Kimchi/Funding -------------------------
def compute_kimchi_premium()->str:
    try:
        j=_get("https://query1.finance.yahoo.com/v7/finance/quote",
               {"symbols":"USDKRW=X"})
        usdkrw=float(j["quoteResponse"]["result"][0]["regularMarketPrice"])
    except: usdkrw=1350.0
    try:
        j=_get("https://api.binance.com/api/v3/ticker/price",
               {"symbol":"BTCUSDT"})
        btc_usd=float(j["price"])
    except: btc_usd=None
    try:
        j=_get("https://api.upbit.com/v1/ticker",{"markets":"KRW-BTC"})
        btc_krw=float(j[0]["trade_price"])
    except: btc_krw=None
    if not all([btc_usd,btc_krw,usdkrw]): return "—"
    prem=(btc_krw/(btc_usd*usdkrw))-1.0
    return f"{prem*100:+.2f}%"

def get_funding()->Tuple[str,str]:
    def f(sym):
        try:
            j=_get("https://fapi.binance.com/fapi/v1/fundingRate",
                   {"symbol":sym,"limit":1})
            return f"{float(j[0]['fundingRate'])*100:+.4f}%"
        except: return "중립권"
    return f("BTCUSDT"), f("ETHUSDT")

# ------------------------- Charts ---------------------------------
def _set_dark():
    plt.rcParams.update({"figure.facecolor":"#0b1020","axes.facecolor":"#121831",
                         "axes.labelcolor":"#e6ebff","xtick.color":"#cfd6ff","ytick.color":"#cfd6ff",
                         "text.color":"#e6ebff","font.size":11})

def plot_bar(names,vals,out_png):
    _set_dark(); import numpy as np
    y=np.arange(len(names)); colors=np.where(np.array(vals)>=0,"#2E7D32","#C62828")
    fig,ax=plt.subplots(figsize=(10,6),dpi=150)
    ax.barh(y,vals,color=colors); ax.axvline(0,color="#3a4569",lw=1)
    ax.set_yticks(y,labels=names); ax.invert_yaxis()
    for yi,v in zip(y,vals): ax.text(v+(0.05 if v>=0 else -0.05),yi,f"{v:+.2f}%",
                                     va="center",ha=("left" if v>=0 else "right"))
    ax.set_title(f"BM20 Daily Perf {YMD}"); fig.savefig(out_png,dpi=180); plt.close(fig)

# ------------------------- News Builder ---------------------------
def build_news(d:Dict)->str:
    return (f"BM20 지수는 {d['asOf']} 전일 대비 {d['bm20ChangePct']:+.2f}% 변동, "
            f"{d['bm20Level']:.0f}pt 기록. 상승 {d['upCount']} / 하락 {d['downCount']}. "
            f"상승 상위 {d['best3']}, 하락 상위 {d['worst3']}. "
            f"BTC {d['btcPrice']}({d['btcChangePct']:+.2f}%), "
            f"ETH {d['ethPrice']}({d['ethChangePct']:+.2f}%). "
            f"김치 프리미엄 {d['kimchi']}, 펀딩비 BTC {d['funding']['btc']}, ETH {d['funding']['eth']}.")

# ------------------------- Main ----------------------------------
def main():
    df=fetch_markets(UNIVERSE); df=fill_previous_prices(df)
    base_val=get_base_value()
    df["weight"]=df.index.map(W)
    port_prev=float((df["previous_price"]*df["weight"]).sum())
    port_now=float((df["current_price"]*df["weight"]).sum())
    bm20_prev=(port_prev/base_val)*100; bm20_now=(port_now/base_val)*100
    bm20_change_pct=((bm20_now/bm20_prev)-1)*100 if bm20_prev else 0

    df["ret_1d"]=(df["current_price"]/df["previous_price"]-1)*100
    df["ret_1d"]=pd.to_numeric(df["ret_1d"],errors="coerce")

    up_count=int((df["ret_1d"]>0).sum()); down_count=int((df["ret_1d"]<0).sum())
    best3=df.sort_values("ret_1d",ascending=False).head(3)
    worst3=df.sort_values("ret_1d",ascending=True).head(3)
    best3_list=[[i,float(r["ret_1d"])] for i,r in best3.iterrows()]
    worst3_list=[[i,float(r["ret_1d"])] for i,r in worst3.iterrows()]

    kimchi=compute_kimchi_premium(); fbtc,feth=get_funding()

    btc_price=df.at["BTC","current_price"]; eth_price=df.at["ETH","current_price"]
    btc_ret=df.at["BTC","ret_1d"]; eth_ret=df.at["ETH","ret_1d"]

    bar_png=os.path.join(OUT_DIR_DATE,f"bm20_bar_{YMD}.png")
    plot_bar([x[0] for x in best3_list+worst3_list],
             [x[1] for x in best3_list+worst3_list], bar_png)

    d={"asOf":YMD,"bm20Level":bm20_now,"bm20PrevLevel":bm20_prev,
       "bm20ChangePct":bm20_change_pct,"upCount":up_count,"downCount":down_count,
       "best3":best3_list,"worst3":worst3_list,
       "btcPrice":f"${btc_price:,.0f}","btcChangePct":btc_ret,
       "ethPrice":f"${eth_price:,.0f}","ethChangePct":eth_ret,
       "kimchi":kimchi,"funding":{"btc":fbtc,"eth":feth}}
    d["news"]=build_news(d)

    with open(os.path.join(SITE_DIR,"bm20_latest.json"),"w",encoding="utf-8") as f: json.dump(d,f,ensure_ascii=False)

    df_out=df[["current_price","previous_price","ret_1d"]].copy()
    df_out["name"]=df_out.index; df_out["weight_ratio"]=df_out.index.map(W)
    df_out.to_csv(os.path.join(OUT_DIR_DATE,f"bm20_daily_data_{YMD}.csv"),index=False,encoding="utf-8")

    with open(os.path.join(OUT_DIR_DATE,f"bm20_news_{YMD}.txt"),"w",encoding="utf-8") as f: f.write(d["news"])

    print("Saved:", os.path.join(OUT_DIR_DATE,f"bm20_daily_data_{YMD}.csv"),
          bar_png, os.path.join(SITE_DIR,"bm20_latest.json"))

if __name__=="__main__":
    main()
