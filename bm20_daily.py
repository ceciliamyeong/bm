# ===================== BM20 Daily — Stable Edition (Backoff, Low-API) =====================
# 기능: 데이터 수집 → 표/차트 → 2단 PDF/HTML 저장 → (옵션) rclone로 GDrive 업로드
# 안정화: CoinGecko 429 회피 (지수백오프), per-coin market_chart 제거(24h 변동률로 전일가 역산)

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
from reportlab.platypus import (
    BaseDocTemplate, Frame, PageTemplate, Paragraph, Spacer, Table, TableStyle, Image
)

# ---- HTML (preview/newsletter) ----
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
heat_png  = os.path.join(OUT_DIR_DATE, f"bm20_heat_{YMD}.png")
pdf_path  = os.path.join(OUT_DIR_DATE, f"bm20_daily_{YMD}.pdf")
html_path = os.path.join(OUT_DIR_DATE, f"bm20_daily_{YMD}.html")
kp_path   = os.path.join(OUT_DIR_DATE, f"kimchi_{YMD}.json")

# ================== Fonts (Nanum 우선, 실패시 CID) ==================
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
        cands = [
            "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
            "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
        ]
        for p in cands:
            if os.path.exists(p):
                fm.fontManager.addfont(p)
                plt.rcParams["font.family"] = os.path.splitext(os.path.basename(p))[0]
                break
    plt.rcParams["axes.unicode_minus"] = False
except Exception:
    plt.rcParams["axes.unicode_minus"] = False

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

# ---- 지수백오프 + API Key(있으면) 헤더 포함 ----
def cg_get(path, params=None, retry=8, timeout=20):
    last = None
    api_key = os.getenv("COINGECKO_API_KEY")  # (선택) pro 키가 있으면 한도 넉넉
    headers = {"User-Agent": "BM20/1.0"}
    if api_key:
        headers["x-cg-pro-api-key"] = api_key
    for i in range(retry):
        try:
            r = requests.get(f"{CG}{path}", params=params, timeout=timeout, headers=headers)
            if r.status_code == 429:  # Too Many Requests
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

# 2) 전일 종가: 24h 변동률로 역산 (대량 market_chart 호출 제거)
df["previous_price"] = df.apply(
    lambda r: (r["current_price"] / (1 + (r["chg24"] or 0) / 100.0)) if r["current_price"] else None,
    axis=1
)

# 3) 가중치(상한→정규화)
df["weight_raw"]=df["market_cap"]/max(df["market_cap"].sum(),1.0)
df["weight_ratio"]=df.apply(
    lambda r: min(r["weight_raw"], BTC_CAP if r["symbol"]=="BTC" else OTH_CAP),
    axis=1
)
df["weight_ratio"]=df["weight_ratio"]/df["weight_ratio"].sum()

# 4) 김치 프리미엄 (df 가격 재활용 + 보수적 우회)
def get_kp(df):
    def _req(url, params=None, retry=4, timeout=12):
        last=None
        for i in range(retry):
            try:
                r=requests.get(url, params=params, timeout=timeout, headers={"User-Agent":"BM20/1.0"})
                if r.status_code==429:
                    time.sleep(1.2*(i+1)); continue
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
            return None, {"dom":"fallback0","glb":"df","fx":"fixed1350",
                          "btc_krw":None,"btc_usd":None,"usdkrw":1350.0}
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
    return kp, {"dom":dom,"glb":glb,"fx":fx,
                "btc_krw":round(btc_krw,2),"btc_usd":round(btc_usd,2),"usdkrw":round(usdkrw,2)}

kimchi_pct, kp_meta = get_kp(df)

# 5) 인덱스/통계
df["price_change_pct"]=(df["current_price"]/df["previous_price"]-1)*100
df["contribution"]=(df["current_price"]-df["previous_price"])*df["weight_ratio"]
bm20_prev=float((df["previous_price"]*df["weight_ratio"]).sum())
bm20_now=float((df["current_price"]*df["weight_ratio"]).sum())
bm20_chg=(bm20_now/bm20_prev-1)*100 if bm20_prev else 0.0
num_up=int((df["price_change_pct"]>0).sum()); num_down=int((df["price_change_pct"]<0).sum())
btc_weight=float(df.loc[df["symbol"]=="BTC","weight_ratio"].iloc[0]) if "BTC" in df["symbol"].values else 0.0
eth_weight=float(df.loc[df["symbol"]=="ETH","weight_ratio"].iloc[0]) if "ETH" in df["symbol"].values else 0.0

top3=df.sort_values("contribution", ascending=False).head(3).reset_index(drop=True)
bot3=df.sort_values("contribution", ascending=True).head(3).reset_index(drop=True)
top_vol=df.sort_values("total_volume", ascending=False).head(5).reset_index(drop=True)

# 6) 뉴스 텍스트
btc_row=df.loc[df["symbol"]=="BTC"].iloc[0]; btc_pct=btc_row["price_change_pct"]
trend_word="상승" if bm20_chg>=0 else "하락"
verb_btc="오르며" if btc_pct>=0 else "내리며"
limit_phrase="지수 상승을 제한했다." if bm20_chg>=0 else "지수 하락을 키웠다."
dominance="상승이 우세했다." if num_up>num_down else ("하락이 우세했다." if num_down>num_up else "보합세를 보였다.")
kp_text = f"{kimchi_pct:.2f}%" if kimchi_pct is not None else "집계 지연"

news_lines=[
  f"BM20 지수는 전일대비 {bm20_chg:+.2f}% {trend_word}한 {bm20_now:,.0f}pt를 기록했다.",
  f"비트코인(BTC)은 {btc_pct:+.2f}% {verb_btc} 지수 흐름에 영향을 줬고, 상위 기여 종목 {top3.loc[0,'symbol']}, {top3.loc[1,'symbol']}, {top3.loc[2,'symbol']}가 주도했다.",
  f"반면 기여도가 낮았던 종목은 {bot3.loc[0,'symbol']}, {bot3.loc[1,'symbol']}, {bot3.loc[2,'symbol']}로, {limit_phrase}",
  f"상승 {num_up} / 하락 {num_down}로 {dominance} BTC/ETH 비중 {btc_weight*100:.1f}% / {eth_weight*100:.1f}%.",
  f"국내-해외 가격 차이를 나타내는 김치 프리미엄은 {kp_text}."
]
news=" ".join(news_lines)

# 7) 저장 (TXT/CSV/JSON)
with open(txt_path,"w",encoding="utf-8") as f: f.write(news)
df_out=df[["symbol","name","current_price","previous_price","price_change_pct","market_cap","total_volume","weight_ratio","contribution"]]
df_out.to_csv(csv_path, index=False, encoding="utf-8")
with open(kp_path, "w", encoding="utf-8") as f:
    json.dump({"date":YMD, **(kp_meta or {}), "kimchi_pct": (None if kimchi_pct is None else round(float(kimchi_pct),4))},
              f, ensure_ascii=False)

# ================== Charts ==================
# A) Top/Bottom 바차트
winners=df.sort_values("price_change_pct", ascending=False).head(TOP_UP)
losers=df.sort_values("price_change_pct", ascending=True).head(TOP_DOWN)
bar=pd.concat([winners[["symbol","price_change_pct"]], losers[["symbol","price_change_pct"]]])
plt.figure()
colors_bar=["tab:green" if v>=0 else "tab:red" for v in bar["price_change_pct"]]
plt.barh(bar["symbol"], bar["price_change_pct"], color=colors_bar)
plt.axvline(0, linewidth=1)
plt.title(f"BM20 Daily Performance  ({YMD})"); plt.xlabel("Daily Change (%)")
plt.tight_layout(); plt.savefig(bar_png, dpi=180); plt.close()

# B) BTC/ETH 7D 추세 (% from first) — 두 번 호출 사이 살짝 대기
def get_pct_series(coin_id, days=8):
    data=cg_get(f"/coins/{coin_id}/market_chart", {"vs_currency":"usd","days":days})
    prices=data.get("prices",[])
    if not prices: return []
    s=[p[1] for p in prices]; base=s[0]
    return [ (v/base-1)*100 for v in s ]
btc7=get_pct_series("bitcoin", 8); time.sleep(0.8)
eth7=get_pct_series("ethereum", 8)
plt.figure()
plt.plot(range(len(btc7)), btc7, label="BTC 7D")
plt.plot(range(len(eth7)), eth7, label="ETH 7D")
plt.legend(); plt.title(f"BTC/ETH 7D Trend ({YMD})"); plt.ylabel("% Change from Start")
plt.tight_layout(); plt.savefig(trend_png, dpi=180); plt.close()

# C) Weights 바차트
plt.figure()
weights_sorted=df.sort_values("weight_ratio", ascending=True)
plt.barh(weights_sorted["symbol"], weights_sorted["weight_ratio"]*100)
plt.title("BM20 Weights (%)"); plt.xlabel("Weight (%)")
plt.tight_layout(); plt.savefig(heat_png, dpi=180); plt.close()

# ================== PDF (Two-column) ==================
styles=getSampleStyleSheet()
styles.add(ParagraphStyle(name="K-Title",  fontName=KOREAN_FONT, fontSize=20, leading=24))
styles.add(ParagraphStyle(name="K-Strong", fontName=KOREAN_FONT, fontSize=12, leading=16, textColor=colors.HexColor("#1A237E")))
styles.add(ParagraphStyle(name="K-Body",   fontName=KOREAN_FONT, fontSize=10.5, leading=15))

doc = BaseDocTemplate(pdf_path, pagesize=A4,
                      leftMargin=1.4*cm, rightMargin=1.4*cm,
                      topMargin=1.3*cm, bottomMargin=1.3*cm)
frame_w = (A4[0] - doc.leftMargin - doc.rightMargin)
col_gap = 0.8*cm
col_w = (frame_w - col_gap)/2.0
col_h = A4[1] - doc.topMargin - doc.bottomMargin
left  = Frame(doc.leftMargin, doc.bottomMargin, col_w, col_h, id='left')
right = Frame(doc.leftMargin + col_w + col_gap, doc.bottomMargin, col_w, col_h, id='right')
doc.addPageTemplates([PageTemplate(id='TwoCol', frames=[left, right])])

story_left, story_right = [], []
story_left += [Paragraph(f"BM20 데일리 리포트  {YMD}", styles["K-Title"]), Spacer(1,0.25*cm)]

summary_tbl = Table([
    ["BM20 지수", f"{bm20_now:,.0f} pt"],
    ["일간 변동", f"{bm20_chg:+.2f}%"],
    ["상승/하락", f"{num_up} / {num_down}"],
    ["BTC/ETH 비중", f"{btc_weight*100:.1f}% / {eth_weight*100:.1f}%"],
    ["김치 프리미엄", f"{kp_text}"],
], colWidths=[3.6*cm, col_w-3.6*cm-0.2*cm])
summary_tbl.setStyle(TableStyle([
    ("FONTNAME",(0,0),(-1,-1), KOREAN_FONT),
    ("FONTSIZE",(0,0),(-1,-1),10.5),
    ("BACKGROUND",(0,0),(-1,0), colors.HexColor("#ECEFF1")),
    ("TEXTCOLOR",(0,0),(0,-1), colors.HexColor("#1A237E")),
    ("BOX",(0,0),(-1,-1),0.5,colors.HexColor("#B0BEC5")),
    ("INNERGRID",(0,0),(-1,-1),0.25,colors.HexColor("#CFD8DC")),
    ("ALIGN",(1,0),(1,-1),"RIGHT"),
]))
story_left += [summary_tbl, Spacer(1, 0.35*cm)]

for ln in news_lines:
    story_left += [Paragraph(ln, styles["K-Body"]), Spacer(1, 0.12*cm)]

story_left += [Spacer(1, 0.3*cm), Paragraph("Top 기여 / 하락 기여", styles["K-Strong"]), Spacer(1, 0.2*cm)]
tbl_contrib = [["종목","기여(가중)"], *[[r["symbol"], f"{r['contribution']:+.6f}"] for _,r in top3.iterrows()]]
tbl_drag    = [["종목","기여(가중)"], *[[r["symbol"], f"{r['contribution']:+.6f}"] for _,r in bot3.iterrows()]]
def style_table(t):
    t.setStyle(TableStyle([
        ("FONTNAME",(0,0),(-1,-1), KOREAN_FONT),
        ("FONTSIZE",(0,0),(-1,-1),10),
        ("BACKGROUND",(0,0),(-1,0), colors.HexColor("#E3F2FD")),
        ("BOX",(0,0),(-1,-1),0.5,colors.HexColor("#90CAF9")),
        ("INNERGRID",(0,0),(-1,-1),0.25,colors.HexColor("#BBDEFB")),
        ("ALIGN",(1,1),(1,-1),"RIGHT"),
    ]))
t1=Table(tbl_contrib, colWidths=[col_w*0.5, col_w*0.4]); style_table(t1)
t2=Table(tbl_drag,    colWidths=[col_w*0.5, col_w*0.4]); style_table(t2)
story_left += [t1, Spacer(1,0.2*cm), t2]

if os.path.exists(bar_png):
    story_right += [Image(bar_png, width=col_w, height=col_w*0.55), Spacer(1, 0.25*cm)]
if os.path.exists(trend_png):
    story_right += [Image(trend_png, width=col_w, height=col_w*0.60), Spacer(1, 0.25*cm)]
if os.path.exists(heat_png):
    story_right += [Image(heat_png, width=col_w, height=col_w*0.55), Spacer(1, 0.25*cm)]

doc.build(story_left + story_right)

# ================== HTML (옵션) ==================
html_tpl = Template(r"""
<!doctype html><html lang="ko"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>BM20 데일리 {{ ymd }}</title>
<style>
body{font-family:-apple-system,BlinkMacSystemFont,"NanumGothic","Noto Sans CJK","Malgun Gothic",Arial,sans-serif;background:#fafbfc;color:#111;margin:0}
.wrap{max-width:720px;margin:0 auto;padding:20px}
.card{background:#fff;border:1px solid #e5e9f0;border-radius:12px;padding:20px;margin-bottom:16px}
h1{font-size:24px;margin:0 0 10px 0} h2{font-size:16px;margin:18px 0 8px 0;color:#1A237E}
.muted{color:#555} table{width:100%;border-collapse:collapse;font-size:14px}
th,td{border:1px solid #e5e9f0;padding:8px;text-align:left} th{background:#eef4ff}
.metric{display:flex;flex-wrap:wrap;gap:10px}
.metric div{flex:1 1 45%;background:#f7f9fc;border:1px solid #e1e6ef;border-radius:10px;padding:10px}
.right{text-align:right} img{max-width:100%;height:auto;border-radius:10px;border:1px solid #e5e9f0}
.footer{font-size:12px;color:#666;text-align:center;margin-top:16px}
</style></head><body>
<div class="wrap">
  <div class="card">
    <h1>BM20 데일리 리포트 <span class="muted">{{ ymd }}</span></h1>
    <div class="metric">
      <div><b>BM20 지수</b><br>{{ bm20_now }} pt</div>
      <div><b>일간 변동</b><br>{{ bm20_chg }}</div>
      <div><b>상승/하락</b><br>{{ num_up }}/{{ num_down }}</div>
      <div><b>BTC/ETH 비중</b><br>{{ btc_w }}% / {{ eth_w }}%</div>
      <div><b>김치 프리미엄</b><br>{{ kp_text }}</div>
    </div>
    <h2>요약</h2>
    <p>{{ news }}</p>
  </div>
  <div class="card">
    <h2>Top/Bottom & Weights</h2>
    {% if bar_png %}<p><img src="{{ bar_png }}" alt="Top/Bottom"></p>{% endif %}
    {% if heat_png %}<p><img src="{{ heat_png }}" alt="Weights"></p>{% endif %}
  </div>
  <div class="card">
    <h2>BTC/ETH 7D</h2>
    {% if trend_png %}<p><img src="{{ trend_png }}" alt="Trend"></p>{% endif %}
  </div>
  <div class="card">
    <h2>거래량 Top5</h2>
    <table><tr><th>종목</th><th class="right">거래량(USD)</th></tr>
      {% for row in vol_rows %}<tr><td>{{ row.name }}</td><td class="right">{{ row.vol }}</td></tr>{% endfor %}
    </table>
  </div>
  <div class="footer">Data: CoinGecko, Upbit · © Blockmedia BM20</div>
</div></body></html>
""")
vol_rows=[{"name":r["symbol"], "vol": f"{r['total_volume']:,.0f}"} for _,r in top_vol.iterrows()]
html = html_tpl.render(
    ymd=YMD, bm20_now=f"{bm20_now:,.0f}", bm20_chg=f"{bm20_chg:+.2f}%",
    num_up=num_up, num_down=num_down, kp_text=kp_text, news=news,
    btc_w=f"{btc_weight*100:.1f}", eth_w=f"{eth_weight*100:.1f}",
    vol_rows=vol_rows,
    bar_png=os.path.basename(bar_png), trend_png=os.path.basename(trend_png), heat_png=os.path.basename(heat_png)
)
with open(html_path, "w", encoding="utf-8") as f: f.write(html)

print("Saved:", txt_path, csv_path, bar_png, trend_png, heat_png, pdf_path, html_path, kp_path)

# ================== (옵션) Google Drive 업로드 (rclone) ==================
GDRIVE_DEST = os.getenv("GDRIVE_DEST")  # 예: "BM20/daily"
AUTO_UPLOAD = os.getenv("AUTO_UPLOAD_TO_DRIVE", "0")  # "1"이면 업로드 시도

def run(cmd):
    print("[cmd]", " ".join(cmd))
    try:
        subprocess.check_call(cmd)
        return True
    except subprocess.CalledProcessError as e:
        print("[rclone] error:", e); return False

if AUTO_UPLOAD == "1" and GDRIVE_DEST:
    run(["rclone", "copy", OUT_DIR, f"gd:{GDRIVE_DEST}", "--create-empty-src-dirs", "--transfers=4", "--checkers=8"])
else:
    print("[rclone] skipped (set AUTO_UPLOAD_TO_DRIVE=1 and GDRIVE_DEST to enable)")
