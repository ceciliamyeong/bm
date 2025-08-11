# ===================== BM20 Daily — Centered Edition (Funding + Stable API) =====================
# 기능: 데이터 수집 → 중앙정렬 헤더(지수/상하락/김프/펀딩비) → 퍼포먼스 차트(상/하) → Top/Bottom 표 → 뉴스 → PDF/HTML → (옵션) GDrive 업로드
# 안정화: CoinGecko 429 회피(지수 백오프), per-coin market_chart 제거(24h 변동률로 전일가 역산)

import os, time, json, random, subprocess
from datetime import datetime, timedelta, timezone
import requests
import pandas as pd

# ---- Matplotlib (charts) ----
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm

# ---- ReportLab (PDF) ----
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.cidfonts import UnicodeCIDFont
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image

# ---- HTML (optional preview) ----
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
trend_png = os.path.join(OUT_DIR_DATE, f"bm20_trend_{YMD}.png")  # 유지(선택)
heat_png  = os.path.join(OUT_DIR_DATE, f"bm20_heat_{YMD}.png")   # 유지(선택)
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
        for p in [
            "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
            "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
        ]:
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

# ================== Data Layer ==================
CG = "https://api.coingecko.com/api/v3"
BTC_CAP, OTH_CAP = 0.30, 0.15
TOP_UP, TOP_DOWN = 6, 4

BM20_IDS = [
    "bitcoin","ethereum","solana","ripple","binancecoin","toncoin","avalanche-2",
    "chainlink","cardano","polygon","near","polkadot","cosmos","litecoin",
    "arbitrum","optimism","internet-computer","aptos","filecoin","sui","dogecoin"
]

def safe_float(x, d=0.0):
    try: return float(x)
    except: return d

# ---- CoinGecko with backoff ----
def cg_get(path, params=None, retry=8, timeout=20):
    last = None
    api_key = os.getenv("COINGECKO_API_KEY")  # (선택) Pro 키 있으면 사용
    headers = {"User-Agent": "BM20/1.0"}
    if api_key:
        headers["x-cg-pro-api-key"] = api_key
    for i in range(retry):
        try:
            r = requests.get(f"{CG}{path}", params=params, timeout=timeout, headers=headers)
            if r.status_code == 429:
                ra = float(r.headers.get("Retry-After", 0)) or (1.5 * (i + 1))
                time.sleep(min(ra, 10) + random.random())
                continue
            if 500 <= r.status_code < 600:
                time.sleep(1.2 * (i + 1) + random.random()); continue
            r.raise_for_status(); return r.json()
        except Exception as e:
            last = e; time.sleep(0.8 * (i + 1) + random.random())
    raise last

# 1) markets (시세/시총/거래량)
mkts = cg_get("/coins/markets", {
  "vs_currency":"usd","ids":",".join(BM20_IDS),
  "order":"market_cap_desc","per_page":len(BM20_IDS),"page":1,
  "price_change_percentage":"24h"
})
df = pd.DataFrame([{
  "id":m["id"],
  "symbol":m["symbol"].upper(),
  "name":m.get("name", m["symbol"].upper()),
  "current_price":safe_float(m["current_price"]),
  "market_cap":safe_float(m["market_cap"]),
  "total_volume":safe_float(m.get("total_volume")),
  "chg24":safe_float(m.get("price_change_percentage_24h"),0.0),
} for m in mkts]).sort_values("market_cap", ascending=False).head(20).reset_index(drop=True)

# 2) 전일 종가: 24h 변동률로 역산 (대량 호출 제거)
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

# 5) 펀딩비 (Binance Futures)
def get_binance_funding(symbol="BTCUSDT", retry=4):
    url = "https://fapi.binance.com/fapi/v1/fundingRate"
    last=None
    for i in range(retry):
        try:
            r=requests.get(url, params={"symbol":symbol, "limit":1}, timeout=12, headers={"User-Agent":"BM20/1.0"})
            if r.status_code==429: time.sleep(1.0*(i+1)); continue
            r.raise_for_status()
            data=r.json()
            return float(data[0]["fundingRate"])*100.0
        except Exception as e:
            last=e; time.sleep(0.6*(i+1))
    raise last

try:
    btc_funding = get_binance_funding("BTCUSDT")
    time.sleep(0.4)
    eth_funding = get_binance_funding("ETHUSDT")
except Exception:
    btc_funding = None; eth_funding = None

# 6) 인덱스/통계
df["price_change_pct"]=(df["current_price"]/df["previous_price"]-1)*100
df["contribution"]=(df["current_price"]-df["previous_price"])*df["weight_ratio"]
bm20_prev=float((df["previous_price"]*df["weight_ratio"]).sum())
bm20_now=float((df["current_price"]*df["weight_ratio"]).sum())
bm20_chg=(bm20_now/bm20_prev-1)*100 if bm20_prev else 0.0
num_up=int((df["price_change_pct"]>0).sum()); num_down=int((df["price_change_pct"]<0).sum())

top3=df.sort_values("price_change_pct", ascending=False).head(3).reset_index(drop=True)
bot3=df.sort_values("price_change_pct", ascending=True).head(3).reset_index(drop=True)
top_vol=df.sort_values("total_volume", ascending=False).head(5).reset_index(drop=True)

# 7) 뉴스 텍스트
btc_row=df.loc[df["symbol"]=="BTC"].iloc[0]; btc_pct=btc_row["price_change_pct"]
trend_word="상승" if bm20_chg>=0 else "하락"
verb_btc="오르며" if btc_pct>=0 else "내리며"
limit_phrase="지수 상승을 제한했다." if bm20_chg>=0 else "지수 하락을 키웠다."
dominance="상승이 우세했다." if num_up>num_down else ("하락이 우세했다." if num_down>num_up else "보합세를 보였다.")
kp_text = fmt_pct(kimchi_pct, 2) if kimchi_pct is not None else "집계 지연"
funding_text = f"BTC {fmt_pct(btc_funding,4)} / ETH {fmt_pct(eth_funding,4)}"

news_lines=[
  f"BM20 지수는 전일대비 {bm20_chg:+.2f}% {trend_word}한 {bm20_now:,.0f}pt를 기록했다.",
  f"비트코인(BTC)은 {btc_pct:+.2f}% {verb_btc} 전체 흐름에 영향을 줬고, 상위 상승 종목 {top3.loc[0,'symbol']}, {top3.loc[1,'symbol']}, {top3.loc[2,'symbol']}가 시장을 견인했다.",
  f"반면 하락 종목은 {bot3.loc[0,'symbol']}, {bot3.loc[1,'symbol']}, {bot3.loc[2,'symbol']}로, {limit_phrase}",
  f"상승 {num_up} / 하락 {num_down}로 {dominance} 한편, 김치 프리미엄은 {kp_text}이며, 선물시장 펀딩비는 {funding_text}로 집계됐다."
]
news=" ".join(news_lines)

# 8) 저장 (TXT/CSV/JSON)
with open(txt_path,"w",encoding="utf-8") as f: f.write(news)
df_out=df[["symbol","name","current_price","previous_price","price_change_pct","market_cap","total_volume","weight_ratio","contribution"]]
df_out.to_csv(csv_path, index=False, encoding="utf-8")
with open(kp_path, "w", encoding="utf-8") as f:
    json.dump({"date":YMD, **(kp_meta or {}), "kimchi_pct": (None if kimchi_pct is None else round(float(kimchi_pct),4))},
              f, ensure_ascii=False)

# ================== Charts ==================
# A) 1-Day Relative Performance (Vertical, diverging)
perf = df.sort_values("price_change_pct", ascending=False)[["symbol","price_change_pct"]].reset_index(drop=True)
plt.figure(figsize=(10, 3.6))
x = range(len(perf)); y = perf["price_change_pct"].values
colors_v = ["#2E7D32" if v >= 0 else "#C62828" for v in y]
plt.bar(x, y, color=colors_v, width=0.8)
plt.xticks(x, perf["symbol"], rotation=0, fontsize=9)
for i, v in enumerate(y):
    if v >= 0:
        plt.text(i, v + (max(y)*0.03 if max(y)>0 else 0.2), f"{v:+.2f}%", ha="center", va="bottom", fontsize=8)
    else:
        plt.text(i, v - (abs(min(y))*0.03 if min(y)<0 else 0.2), f"{v:+.2f}%", ha="center", va="top", fontsize=8)
plt.axhline(0, linewidth=1, color="#90A4AE")
plt.title("1 DAY RELATIVE PERFORMANCE  [USD]", fontsize=12, pad=10)
plt.ylabel("%")
plt.tight_layout(); plt.savefig(bar_png, dpi=180); plt.close()

# (선택) B, C 차트는 원하면 계속 사용 가능
# ================== PDF (Centered Layout) ==================
styles=getSampleStyleSheet()
title_style    = ParagraphStyle("Title",    fontName=KOREAN_FONT, fontSize=18, alignment=1, spaceAfter=10)
subtitle_style = ParagraphStyle("Subtitle", fontName=KOREAN_FONT, fontSize=13, alignment=1, textColor=colors.HexColor("#546E7A"), spaceAfter=10)
metric_style   = ParagraphStyle("Metric",   fontName=KOREAN_FONT, fontSize=12, alignment=1, spaceAfter=6)
section_style  = ParagraphStyle("Section",  fontName=KOREAN_FONT, fontSize=12, alignment=1, textColor=colors.HexColor("#1A237E"), spaceBefore=10, spaceAfter=6)
body_style     = ParagraphStyle("Body",     fontName=KOREAN_FONT, fontSize=11, alignment=1, leading=16)

doc = SimpleDocTemplate(pdf_path, pagesize=A4,
                        leftMargin=1.8*cm, rightMargin=1.8*cm,
                        topMargin=1.6*cm, bottomMargin=1.6*cm)

story = []
# 상단 중앙 헤더
story += [
    Paragraph("BM20 데일리 리포트", title_style),
    Paragraph(f"{YMD}", subtitle_style),
    Paragraph(f"지수 {bm20_now:,.0f}pt  ({bm20_chg:+.2f}%)", metric_style),
    Paragraph(f"상승 {num_up} · 하락 {num_down}", metric_style),
    Paragraph(f"김치프리미엄 {kp_text}", metric_style),
    Paragraph(f"펀딩비  {funding_text}", metric_style),
    Spacer(1, 0.5*cm)
]

# 퍼포먼스 차트
if os.path.exists(bar_png):
    story += [Paragraph("1일 상대 퍼포먼스", section_style),
              Image(bar_png, width=16.0*cm, height=5.2*cm), Spacer(1, 0.3*cm)]

# Top/Bottom 표 (등락률 기준)
tbl_up = [["상승 Top 3","등락률"], *[[r["symbol"], f"{r['price_change_pct']:+.2f}%"] for _,r in top3.iterrows()]]
tbl_dn = [["하락 Top 3","등락률"], *[[r["symbol"], f"{r['price_change_pct']:+.2f}%"] for _,r in bot3.iterrows()]]
def style_tbl(t):
    t.setStyle(TableStyle([
        ("FONTNAME",(0,0),(-1,-1), KOREAN_FONT),
        ("FONTSIZE",(0,0),(-1,-1),10.5),
        ("BACKGROUND",(0,0),(-1,0), colors.HexColor("#E3F2FD")),
        ("BOX",(0,0),(-1,-1),0.5,colors.HexColor("#90CAF9")),
        ("INNERGRID",(0,0),(-1,-1),0.25,colors.HexColor("#BBDEFB")),
        ("ALIGN",(1,1),(1,-1),"RIGHT"),
    ]))
t_up = Table(tbl_up, colWidths=[8.0*cm, 3.5*cm]); style_tbl(t_up)
t_dn = Table(tbl_dn, colWidths=[8.0*cm, 3.5*cm]); style_tbl(t_dn)
story += [t_up, Spacer(1,0.2*cm), t_dn, Spacer(1,0.4*cm)]

# 뉴스 해설
story += [Paragraph("요약 해설", section_style),
          Paragraph(news, body_style)]

# 푸터
story += [Spacer(1,0.5*cm),
          Paragraph("© Blockmedia · Data: CoinGecko, Upbit · Funding: Binance Futures", ParagraphStyle("Footer", fontName=KOREAN_FONT, fontSize=9, alignment=1, textColor=colors.HexColor("#78909C")))]

doc.build(story)

# ================== HTML (옵션, 미리보기) ==================
html_tpl = Template(r"""
<!doctype html><html lang="ko"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>BM20 데일리 {{ ymd }}</title>
<style>
body{font-family:-apple-system,BlinkMacSystemFont,"NanumGothic","Noto Sans CJK","Malgun Gothic",Arial,sans-serif;background:#fafbfc;color:#111;margin:0}
.wrap{max-width:720px;margin:0 auto;padding:20px}
.card{background:#fff;border:1px solid #e5e9f0;border-radius:12px;padding:20px;margin-bottom:16px}
h1{font-size:22px;margin:0 0 8px 0} h2{font-size:15px;margin:16px 0 8px 0;color:#1A237E}
.muted{color:#555} .center{text-align:center} img{max-width:100%;height:auto;border-radius:10px;border:1px solid #e5e9f0}
table{width:100%;border-collapse:collapse;font-size:14px} th,td{border:1px solid #e5e9f0;padding:8px} th{background:#eef4ff}
.footer{font-size:12px;color:#666;text-align:center;margin-top:16px}
</style></head><body>
<div class="wrap">
  <div class="card center">
    <h1>BM20 데일리 리포트</h1>
    <div class="muted">{{ ymd }}</div>
    <div>지수 {{ bm20_now }}pt ({{ bm20_chg }})</div>
    <div>상승 {{ num_up }} · 하락 {{ num_down }}</div>
    <div>김치프리미엄 {{ kp_text }}</div>
    <div>펀딩비 {{ funding_text }}</div>
  </div>
  <div class="card">
    <h2>1일 상대 퍼포먼스</h2>
    {% if bar_png %}<p class="center"><img src="{{ bar_png }}" alt="Performance"></p>{% endif %}
    <h2>상승/하락 Top 3</h2>
    <table>
      <tr><th>상승</th><th>등락률</th></tr>
      {% for r in top_up %}<tr><td>{{ r.sym }}</td><td style="text-align:right">{{ r.pct }}</td></tr>{% endfor %}
    </table><br>
    <table>
      <tr><th>하락</th><th>등락률</th></tr>
      {% for r in top_dn %}<tr><td>{{ r.sym }}</td><td style="text-align:right">{{ r.pct }}</td></tr>{% endfor %}
    </table>
  </div>
  <div class="card"><h2>요약 해설</h2><p>{{ news }}</p></div>
  <div class="footer">© Blockmedia · Data: CoinGecko, Upbit · Funding: Binance Futures</div>
</div></body></html>
""")
html = html_tpl.render(
    ymd=YMD, bm20_now=f"{bm20_now:,.0f}", bm20_chg=f"{bm20_chg:+.2f}%",
    num_up=num_up, num_down=num_down, kp_text=kp_text, funding_text=funding_text,
    top_up=[{"sym":r["symbol"], "pct": f"{r['price_change_pct']:+.2f}%"} for _,r in top3.iterrows()],
    top_dn=[{"sym":r["symbol"], "pct": f"{r['price_change_pct']:+.2f}%"} for _,r in bot3.iterrows()],
    bar_png=os.path.basename(bar_png)
)
with open(html_path, "w", encoding="utf-8") as f: f.write(html)

print("Saved:", txt_path, csv_path, bar_png, pdf_path, html_path, kp_path)

# ================== (옵션) Google Drive 업로드 (rclone) ==================
GDRIVE_DEST = os.getenv("GDRIVE_DEST")  # 예: "BM20/daily"
AUTO_UPLOAD = os.getenv("AUTO_UPLOAD_TO_DRIVE", "0")  # "1"이면 업로드 시도

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
