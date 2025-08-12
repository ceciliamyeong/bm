# ===================== BM20 Daily — Stable Funding + Natural News =====================
# 기능: 데이터 수집 → 카드형 PDF/HTML → 코인별 퍼포먼스 → Top/Bottom → 거래량 증가율 TOP3
#     → BTC/ETH 7일 추세 → 김프/펀딩비 표 → 자연어 뉴스 → (옵션) rclone 업로드

import os, time, json, random, subprocess
from datetime import datetime, timedelta, timezone
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

# ---- HTML (optional) ----
from jinja2 import Template

# ================== 공통 설정 ==================
OUT_DIR = os.getenv("OUT_DIR", "out"); os.makedirs(OUT_DIR, exist_ok=True)
KST = timezone(timedelta(hours=9))
YMD = datetime.now(KST).strftime("%Y-%m-%d")
OUT_DIR_DATE = os.path.join(OUT_DIR, YMD); os.makedirs(OUT_DIR_DATE, exist_ok=True)

# Paths
txt_path  = os.path.join(OUT_DIR_DATE, f"bm20_news_{YMD}.txt")
csv_path  = os.path.join(OUT_DIR_DATE, f"bm20_daily_data_{YMD}.csv")
bar_png   = os.path.join(OUT_DIR_DATE, f"bm20_bar_{YMD}.png")
trend_png = os.path.join(OUT_DIR_DATE, f"bm20_trend_{YMD}.png")
pdf_path  = os.path.join(OUT_DIR_DATE, f"bm20_daily_{YMD}.pdf")
html_path = os.path.join(OUT_DIR_DATE, f"bm20_daily_{YMD}.html")
kp_path   = os.path.join(OUT_DIR_DATE, f"kimchi_{YMD}.json")

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
    else:
        for p in ["/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
                  "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc"]:
            if os.path.exists(p):
                fm.fontManager.addfont(p)
                plt.rcParams["font.family"] = os.path.splitext(os.path.basename(p))[0]
                break
    plt.rcParams["axes.unicode_minus"] = False
except Exception:
    plt.rcParams["axes.unicode_minus"] = False

# ================== Helper ==================
def fmt_pct(v, digits=1):
    try:
        if v is None: return "-"
        return f"{float(v):.{digits}f}%"
    except Exception:
        return "-"

def safe_float(x, d=0.0):
    try: return float(x)
    except: return d

def clamp_list_str(items, n=3):
    items = [str(x) for x in items if str(x)]
    return items[:n]

# ================== Data Layer ==================
CG = "https://api.coingecko.com/api/v3"
BTC_CAP, OTH_CAP = 0.30, 0.15
TOP_UP, TOP_DOWN = 3, 3

BM20_IDS = [
    "bitcoin","ethereum","solana","ripple","binancecoin","toncoin","avalanche-2",
    "chainlink","cardano","polygon","near","polkadot","cosmos","litecoin",
    "arbitrum","optimism","internet-computer","aptos","filecoin","sui","dogecoin"
]

# ---- CoinGecko with backoff ----
def cg_get(path, params=None, retry=8, timeout=20):
    last = None
    api_key = os.getenv("COINGECKO_API_KEY")
    headers = {"User-Agent": "BM20/1.0"}
    if api_key: headers["x-cg-pro-api-key"] = api_key
    for i in range(retry):
        try:
            r = requests.get(f"{CG}{path}", params=params, timeout=timeout, headers=headers)
            if r.status_code == 429:
                ra = float(r.headers.get("Retry-After", 0)) or (1.5 * (i + 1))
                time.sleep(min(ra, 10) + random.random()); continue
            if 500 <= r.status_code < 600:
                time.sleep(1.2 * (i + 1) + random.random()); continue
            r.raise_for_status(); return r.json()
        except Exception as e:
            last = e; time.sleep(0.8 * (i + 1) + random.random())
    raise last

# 1) markets
mkts = cg_get("/coins/markets", {
  "vs_currency":"usd","ids":",".join(BM20_IDS),
  "order":"market_cap_desc","per_page":len(BM20_IDS),"page":1,
  "price_change_percentage":"24h"
})
df = pd.DataFrame([{
  "id":m["id"], "symbol":m["symbol"].upper(), "name":m.get("name", m["symbol"].upper()),
  "current_price":safe_float(m["current_price"]), "market_cap":safe_float(m["market_cap"]),
  "total_volume":safe_float(m.get("total_volume")),
  "chg24":safe_float(m.get("price_change_percentage_24h"),0.0),
} for m in mkts]).sort_values("market_cap", ascending=False).head(20).reset_index(drop=True)

# 2) 전일 종가: 24h 변동률로 역산
df["previous_price"] = df.apply(
    lambda r: (r["current_price"] / (1 + (r["chg24"] or 0) / 100.0)) if r["current_price"] else None,
    axis=1
)

# 3) 가중치(상한→정규화)
df["weight_raw"]=df["market_cap"]/max(df["market_cap"].sum(),1.0)
df["weight_ratio"]=df.apply(lambda r: min(r["weight_raw"], BTC_CAP if r["symbol"]=="BTC" else OTH_CAP), axis=1)
df["weight_ratio"]=df["weight_ratio"]/df["weight_ratio"].sum()

# 4) 김치 프리미엄
def get_kp(df):
    def _req(url, params=None, retry=4, timeout=12):
        last=None
        for i in range(retry):
            try:
                r=requests.get(url, params=params, timeout=timeout, headers={"User-Agent":"BM20/1.0"})
                if r.status_code==429: time.sleep(1.2*(i+1)); continue
                r.raise_for_status(); return r.json()
            except Exception as e:
                last=e; time.sleep(0.6*(i+1))
        raise last
    try:
        u=_req("https://api.upbit.com/v1/ticker", {"markets":"KRW-BTC"})
        btc_krw=float(u[0]["trade_price"]); dom="upbit"
    except Exception:
        try:
            cg=_req(f"{CG}/simple/price", {"ids":"bitcoin","vs_currencies":"krw"})
            btc_krw=float(cg["bitcoin"]["krw"]); dom="cg_krw"
        except Exception:
            return None, {"dom":"fallback0","glb":"df","fx":"fixed1350","btc_krw":None,"btc_usd":None,"usdkrw":1350.0}
    try:
        btc_usd=float(df.loc[df["symbol"]=="BTC","current_price"].iloc[0]); glb="df"
    except Exception:
        btc_usd=None; glb=None
    if btc_usd is None:
        try:
            b=_req("https://api.binance.com/api/v3/ticker/price", {"symbol":"BTCUSDT"})
            btc_usd=float(b["price"]); glb="binance"
        except Exception:
            try:
                cg=_req(f"{CG}/simple/price", {"ids":"bitcoin","vs_currencies":"usd"})
                btc_usd=float(cg["bitcoin"]["usd"]); glb="cg_usd"
            except Exception:
                return None, {"dom":dom,"glb":"fallback0","fx":"fixed1350",
                              "btc_krw":round(btc_krw,2),"btc_usd":None,"usdkrw":1350.0}
    try:
        t=_req(f"{CG}/simple/price", {"ids":"tether","vs_currencies":"krw"})
        usdkrw=float(t["tether"]["krw"]); fx="cg_tether"
        if not (900<=usdkrw<=2000): raise ValueError
    except Exception:
        usdkrw=1350.0; fx="fixed1350"
    kp=((btc_krw/usdkrw)-btc_usd)/btc_usd*100
    return kp, {"dom":dom,"glb":glb,"fx":fx,"btc_krw":round(btc_krw,2),"btc_usd":round(btc_usd,2),"usdkrw":round(usdkrw,2)}

kimchi_pct, kp_meta = get_kp(df)
kp_text = fmt_pct(kimchi_pct, 2) if kimchi_pct is not None else "집계 지연"

# 5) 펀딩비 — 안정 엔드포인트 + 다중 폴백
def get_binance_funding(symbol="BTCUSDT", retry=5):
    urls = [
        "https://fapi.binance.com/fapi/v1/premiumIndex",
        "https://fapi1.binance.com/fapi/v1/premiumIndex",
        "https://fapi2.binance.com/fapi/v1/premiumIndex",
    ]
    for i in range(retry):
        for u in urls:
            try:
                r = requests.get(u, params={"symbol": symbol}, timeout=12,
                                 headers={"User-Agent":"BM20/1.0","Accept":"application/json"})
                if r.status_code == 429: time.sleep(1.0*(i+1)); continue
                r.raise_for_status()
                j = r.json()
                if isinstance(j, dict) and "lastFundingRate" in j:
                    return float(j["lastFundingRate"]) * 100.0
                if isinstance(j, list) and j and "lastFundingRate" in j[0]:
                    return float(j[0]["lastFundingRate"]) * 100.0
            except Exception:
                pass
        time.sleep(0.6*(i+1))
    # 마지막 폴백(히스토리 최신 1개)
    try:
        r = requests.get("https://fapi.binance.com/fapi/v1/fundingRate",
                         params={"symbol":symbol,"limit":1}, timeout=12)
        r.raise_for_status(); d=r.json()
        if d: return float(d[0]["fundingRate"]) * 100.0
    except Exception:
        return None
    return None

def get_bybit_funding(symbol="BTCUSDT", retry=5):
    url = "https://api.bybit.com/v5/market/tickers"
    for i in range(retry):
        try:
            r = requests.get(url, params={"category":"linear","symbol":symbol}, timeout=12,
                             headers={"User-Agent":"BM20/1.0","Accept":"application/json"})
            if r.status_code == 429: time.sleep(1.0*(i+1)); continue
            r.raise_for_status()
            j = r.json()
            lst = j.get("result",{}).get("list",[])
            if lst:
                fr = lst[0].get("fundingRate")
                if fr is not None: return float(fr) * 100.0
            return None
        except Exception:
            time.sleep(0.6*(i+1))
    return None

btc_f_bin = get_binance_funding("BTCUSDT"); time.sleep(0.2)
eth_f_bin = get_binance_funding("ETHUSDT"); time.sleep(0.2)
btc_f_byb = get_bybit_funding("BTCUSDT");   time.sleep(0.2)
eth_f_byb = get_bybit_funding("ETHUSDT")

def fp(v): return "-" if (v is None) else f"{float(v):.4f}%"

# 6) 인덱스/통계
df["price_change_pct"]=(df["current_price"]/df["previous_price"]-1)*100
df["contribution"]=(df["current_price"]-df["previous_price"])*df["weight_ratio"]
bm20_prev=float((df["previous_price"]*df["weight_ratio"]).sum())
bm20_now=float((df["current_price"]*df["weight_ratio"]).sum())
bm20_chg=(bm20_now/bm20_prev-1)*100 if bm20_prev else 0.0
num_up=int((df["price_change_pct"]>0).sum()); num_down=int((df["price_change_pct"]<0).sum())

top_up = df.sort_values("price_change_pct", ascending=False).head(TOP_UP).reset_index(drop=True)
top_dn = df.sort_values("price_change_pct", ascending=True).head(TOP_DOWN).reset_index(drop=True)

# 거래량 증가율 TOP3 (상위 소수만 조회, 근사)
def get_prev_volume_coin(cid, days=2):
    data = cg_get(f"/coins/{cid}/market_chart", {"vs_currency":"usd","days":days})
    vols = data.get("total_volumes", [])
    if not vols: return None
    now_v = float(vols[-1][1])
    past_v = float(vols[len(vols)//2][1]) if len(vols) >= 2 else float(vols[0][1])
    if past_v == 0: return None
    return now_v, past_v

vol_df = df.sort_values("total_volume", ascending=False).head(8).copy()
vol_incr_rows = []
for _, r in vol_df.iterrows():
    try:
        pair = get_prev_volume_coin(r["id"], days=2)
        time.sleep(0.2)
        if pair:
            now_v, past_v = pair
            incr = (now_v/past_v - 1.0) * 100.0
            vol_incr_rows.append({"symbol": r["symbol"], "incr_pct": incr})
    except Exception:
        continue
vol_incr = pd.DataFrame(vol_incr_rows)
if not vol_incr.empty:
    vol_top3 = vol_incr.sort_values("incr_pct", ascending=False).head(3).reset_index(drop=True)
else:
    vol_top3 = pd.DataFrame([{"symbol":"-", "incr_pct": None} for _ in range(3)])

# 7) 뉴스 — 자연어 분기 강화
def build_news():
    def s(v): return f"{float(v):+,.2f}%"
    trend = "상승" if bm20_chg > 0 else ("하락" if bm20_chg < 0 else "보합")
    lead_verb = "오르며" if df.loc[df["symbol"]=="BTC","price_change_pct"].iloc[0] >= 0 else "내리며"

    up_syms  = clamp_list_str(top_up["symbol"].tolist(), 3)
    up_pcts  = [s(p) for p in top_up["price_change_pct"].tolist()[:len(up_syms)]]
    dn_syms  = clamp_list_str(top_dn["symbol"].tolist(), 3)
    dn_pcts  = [s(p) for p in top_dn["price_change_pct"].tolist()[:len(dn_syms)]]

    parts = []
    parts.append(f"비트코인과 이더리움을 포함한 대형주 위주의 BM20 지수는 {YMD} 전일대비 {s(bm20_chg)} {trend}한 {bm20_now:,.0f}포인트를 기록했다.")
    btc_pct = df.loc[df["symbol"]=="BTC","price_change_pct"].iloc[0]
    parts.append(f"시총 1위 비트코인은 {s(btc_pct)} {lead_verb} 시장 방향에 영향을 줬다.")

    if up_syms:
        parts.append(f"상승 종목 가운데 {', '.join(up_syms)}가 각각 {', '.join(up_pcts)} 기록하며 상대적으로 견조했다.")
    if dn_syms:
        parts.append(f"반면 {', '.join(dn_syms)}는 {', '.join(dn_pcts)} 하락해 지수 흐름을 제약했다.")

    balance = ("상승이 우세했다" if num_up>num_down else
               ("하락이 우세했다" if num_down>num_up else "상승·하락이 비슷했다"))
    parts.append(f"시총 상위 20종목 중 상승 {num_up}개, 하락 {num_down}개로 {balance}.")

    kp_side = "한국이 낮은 수준" if (kimchi_pct is not None and kimchi_pct < 0) else "한국이 높은 수준"
    fund_sentence = f"바이낸스 펀딩비는 BTC {fp(btc_f_bin)}, ETH {fp(eth_f_bin)}"
    if (btc_f_byb is not None) or (eth_f_byb is not None):
        fund_sentence += f"; 바이빗은 BTC {fp(btc_f_byb)}, ETH {fp(eth_f_byb)}"
    parts.append(f"국내외 가격 차이를 나타내는 K-BM 프리미엄은 {kp_text}로, {kp_side}에서 거래됐다. "
                 f"{fund_sentence}가 집계됐다.")
    return " ".join(parts)

news = build_news()

# 8) 저장 (TXT/CSV/JSON)
with open(txt_path,"w",encoding="utf-8") as f: f.write(news)
df_out=df[["symbol","name","current_price","previous_price","price_change_pct","market_cap","total_volume","weight_ratio","contribution"]]
df_out.to_csv(csv_path, index=False, encoding="utf-8")
with open(kp_path, "w", encoding="utf-8") as f:
    json.dump({"date":YMD, **(kp_meta or {}), "kimchi_pct": (None if kimchi_pct is None else round(float(kimchi_pct),4))},
              f, ensure_ascii=False)

# ================== Charts ==================
# A) 코인별 퍼포먼스
perf = df.sort_values("price_change_pct", ascending=False)[["symbol","price_change_pct"]].reset_index(drop=True)
plt.figure(figsize=(10.6, 4.6))
x = range(len(perf)); y = perf["price_change_pct"].values
colors_v = ["#2E7D32" if v >= 0 else "#C62828" for v in y]
plt.bar(x, y, color=colors_v, width=0.82)
plt.xticks(x, perf["symbol"], rotation=0, fontsize=10)
for i, v in enumerate(y):
    if v >= 0:
        plt.text(i, v + (max(y)*0.03 if max(y)>0 else 0.25), f"{v:+.2f}%", ha="center", va="bottom", fontsize=10)
    else:
        plt.text(i, v - (abs(min(y))*0.03 if min(y)<0 else 0.25), f"{v:+.2f}%", ha="center", va="top", fontsize=10)
plt.axhline(0, linewidth=1, color="#90A4AE")
plt.title("코인별 퍼포먼스 (1D, USD)", fontsize=13, loc="left", pad=10)
plt.ylabel("%"); plt.tight_layout(); plt.savefig(bar_png, dpi=180); plt.close()

# B) BTC/ETH 7일 추세
def get_pct_series(coin_id, days=8):
    data=cg_get(f"/coins/{coin_id}/market_chart", {"vs_currency":"usd","days":days})
    prices=data.get("prices",[])
    if not prices: return []
    s=[p[1] for p in prices]; base=s[0]
    return [ (v/base-1)*100 for v in s ]
btc7=get_pct_series("bitcoin", 8); time.sleep(0.5)
eth7=get_pct_series("ethereum", 8)
plt.figure(figsize=(10.6, 3.8))
plt.plot(range(len(btc7)), btc7, label="BTC")
plt.plot(range(len(eth7)), eth7, label="ETH")
plt.legend(loc="upper left"); plt.title("BTC & ETH 7일 가격 추세", fontsize=13, loc="left", pad=8)
plt.ylabel("% (from start)"); plt.tight_layout(); plt.savefig(trend_png, dpi=180); plt.close()

# ================== PDF (Clean Card Layout) ==================
styles = getSampleStyleSheet()
title_style    = ParagraphStyle("Title",    fontName=KOREAN_FONT, fontSize=18, alignment=1, spaceAfter=6)
subtitle_style = ParagraphStyle("Subtitle", fontName=KOREAN_FONT, fontSize=12.5, alignment=1,
                                textColor=colors.HexColor("#546E7A"), spaceAfter=12)
section_h      = ParagraphStyle("SectionH", fontName=KOREAN_FONT, fontSize=13,  alignment=0,
                                textColor=colors.HexColor("#1A237E"), spaceBefore=4, spaceAfter=8)
body_style     = ParagraphStyle("Body",     fontName=KOREAN_FONT, fontSize=11,  alignment=0, leading=16)

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

doc = SimpleDocTemplate(pdf_path, pagesize=A4,
                        leftMargin=1.8*cm, rightMargin=1.8*cm,
                        topMargin=1.6*cm, bottomMargin=1.6*cm)

story = []
story += [Paragraph("BM20 데일리 리포트", title_style),
          Paragraph(f"{YMD}", subtitle_style)]

# 메트릭 표
metrics = [
    ["지수",        f"{bm20_now:,.0f} pt"],
    ["일간 변동",   f"{bm20_chg:+.2f}%"],
    ["상승/하락",   f"{num_up} / {num_down}"],
    ["김치 프리미엄", kp_text],
    ["펀딩비(Binance)", f"BTC {fp(btc_f_bin)} / ETH {fp(eth_f_bin)}"],
]
if (btc_f_byb is not None) or (eth_f_byb is not None):
    metrics.append(["펀딩비(Bybit)", f"BTC {fp(btc_f_byb)} / ETH {fp(eth_f_byb)}"])
mt = Table(metrics, colWidths=[3.8*cm, 12.2*cm]); style_table_basic(mt)
story += [card([mt]), Spacer(1, 0.45*cm)]

# 코인별 퍼포먼스
perf_block = [Paragraph("코인별 퍼포먼스 (1D, USD)", section_h)]
if os.path.exists(bar_png): perf_block += [Image(bar_png, width=16.0*cm, height=6.6*cm)]
story += [card(perf_block), Spacer(1, 0.45*cm)]

# 상승/하락 TOP3
tbl_up = [["상승 TOP3","등락률"], *[[r["symbol"], f"{r['price_change_pct']:+.2f}%"] for _,r in top_up.iterrows()]]
tbl_dn = [["하락 TOP3","등락률"], *[[r["symbol"], f"{r['price_change_pct']:+.2f}%"] for _,r in top_dn.iterrows()]]
t_up = Table(tbl_up, colWidths=[8.0*cm, 3.5*cm]); t_dn = Table(tbl_dn, colWidths=[8.0*cm, 3.5*cm])
style_table_basic(t_up); style_table_basic(t_dn)
story += [card([Paragraph("상승/하락 TOP3", section_h), Spacer(1,4), t_up, Spacer(1,6), t_dn]),
          Spacer(1, 0.45*cm)]

# 거래량 증가율 TOP3 (근사)
vol_tbl = [["종목","증가율"], *[[r["symbol"], "-" if r["incr_pct"] is None else f"{r['incr_pct']:+.2f}%"] for _,r in vol_top3.iterrows()]]
t_vol = Table(vol_tbl, colWidths=[8.0*cm, 3.5*cm])
t_vol.setStyle(TableStyle([
    ("FONTNAME",(0,0),(-1,-1), KOREAN_FONT),
    ("FONTSIZE",(0,0),(-1,-1),10.5),
    ("BACKGROUND",(0,0),(-1,0), colors.HexColor("#FFF3E0")),
    ("BOX",(0,0),(-1,-1),0.5, colors.HexColor("#FFCC80")),
    ("INNERGRID",(0,0),(-1,-1),0.25, colors.HexColor("#FFE0B2")),
    ("ALIGN",(0,0),(-1,-1),"LEFT"),
    ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
]))
story += [card([Paragraph("거래량 증가율 TOP3 (근사)", section_h), Spacer(1,4), t_vol]),
          Spacer(1, 0.45*cm)]

# BTC/ETH 7일 추세
trend_block = [Paragraph("BTC & ETH 7일 가격 추세", section_h)]
if os.path.exists(trend_png): trend_block += [Image(trend_png, width=16.0*cm, height=5.2*cm)]
story += [card(trend_block), Spacer(1, 0.45*cm)]

# BM20 데일리 뉴스
story += [card([Paragraph("BM20 데일리 뉴스", section_h), Spacer(1,2), Paragraph(news, body_style)]),
          Spacer(1, 0.45*cm)]

# 푸터
footer = Paragraph("© Blockmedia · Data: CoinGecko, Upbit · Funding: Binance & Bybit",
                   ParagraphStyle("Footer", fontName=KOREAN_FONT, fontSize=9, alignment=1,
                                  textColor=colors.HexColor("#78909C")))
story += [footer]
doc.build(story)

# ================== HTML (옵션) ==================
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
</style></head><body>
<div class="wrap">
  <div class="card">
    <h1>BM20 데일리 리포트</h1>
    <div class="muted">{{ ymd }}</div>
    <table style="margin-top:10px">
      <tr><th>지수</th><td>{{ bm20_now }} pt</td></tr>
      <tr><th>일간 변동</th><td>{{ bm20_chg }}</td></tr>
      <tr><th>상승/하락</th><td>{{ num_up }} / {{ num_down }}</td></tr>
      <tr><th>김치 프리미엄</th><td>{{ kp_text }}</td></tr>
      <tr><th>펀딩비(Binance)</th><td>BTC {{ bf_btc }} / ETH {{ bf_eth }}</td></tr>
      {% if by_btc or by_eth %}<tr><th>펀딩비(Bybit)</th><td>BTC {{ by_btc }} / ETH {{ by_eth }}</td></tr>{% endif %}
    </table>
  </div>
  <div class="card">
    <h2>코인별 퍼포먼스 (1D, USD)</h2>
    {% if bar_png %}<p class="center"><img src="{{ bar_png }}" alt="Performance"></p>{% endif %}
    <h2>상승/하락 TOP3</h2>
    <table><tr><th>상승</th><th style="text-align:right">등락률</th></tr>
      {% for r in top_up %}<tr><td>{{ r.sym }}</td><td style="text-align:right">{{ r.pct }}</td></tr>{% endfor %}
    </table><br>
    <table><tr><th>하락</th><th style="text-align:right">등락률</th></tr>
      {% for r in top_dn %}<tr><td>{{ r.sym }}</td><td style="text-align:right">{{ r.pct }}</td></tr>{% endfor %}
    </table>
  </div>
  <div class="card">
    <h2>거래량 증가율 TOP3 (근사)</h2>
    <table><tr><th>종목</th><th style="text-align:right">증가율</th></tr>
      {% for r in vol3 %}<tr><td>{{ r.sym }}</td><td style="text-align:right">{{ r.pct }}</td></tr>{% endfor %}
    </table>
  </div>
  <div class="card">
    <h2>BTC & ETH 7일 가격 추세</h2>
    {% if trend_png %}<p class="center"><img src="{{ trend_png }}" alt="Trend"></p>{% endif %}
  </div>
  <div class="card"><h2>BM20 데일리 뉴스</h2><p>{{ news }}</p></div>
  <div class="footer">© Blockmedia · Data: CoinGecko, Upbit · Funding: Binance & Bybit</div>
</div></body></html>
""")
html = html_tpl.render(
    ymd=YMD, bm20_now=f"{bm20_now:,.0f}", bm20_chg=f"{bm20_chg:+.2f}%",
    num_up=num_up, num_down=num_down, kp_text=kp_text,
    bf_btc=fmt_pct(btc_f_bin,4), bf_eth=fmt_pct(eth_f_bin,4),
    by_btc=fmt_pct(btc_f_byb,4) if btc_f_byb is not None else "",
    by_eth=fmt_pct(eth_f_byb,4) if eth_f_byb is not None else "",
    top_up=[{"sym":r["symbol"], "pct": f"{r['price_change_pct']:+.2f}%"} for _,r in top_up.iterrows()],
    top_dn=[{"sym":r["symbol"], "pct": f"{r['price_change_pct']:+.2f}%"} for _,r in top_dn.iterrows()],
    vol3=[{"sym":r["symbol"], "pct": ( "-" if r["incr_pct"] is None else f"{r['incr_pct']:+.2f}%")} for _,r in vol_top3.iterrows()],
    bar_png=os.path.basename(bar_png), trend_png=os.path.basename(trend_png), news=news
)
with open(html_path, "w", encoding="utf-8") as f: f.write(html)

print("Saved:", txt_path, csv_path, bar_png, trend_png, pdf_path, html_path, kp_path)

# ================== (옵션) Google Drive 업로드 ==================
GDRIVE_DEST = os.getenv("GDRIVE_DEST")
AUTO_UPLOAD = os.getenv("AUTO_UPLOAD_TO_DRIVE", "0")

def run(cmd):
    print("[cmd]", " ".join(cmd))
    try:
        subprocess.check_call(cmd); return True
    except subprocess.CalledProcessError as e:
        print("[rclone] error:", e); return False

if AUTO_UPLOAD == "1" and GDRIVE_DEST:
    run(["rclone", "copy", OUT_DIR, f"gd:{GDRIVE_DEST}", "--create-empty-src-dirs", "--transfers=4", "--checkers=8"])
else:
    print("[rclone] skipped (set AUTO_UPLOAD_TO_DRIVE=1 and GDRIVE_DEST to enable)")

