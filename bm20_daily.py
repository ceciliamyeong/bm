# ===================== BM20 Daily — 2-Column PDF + HTML + Gmail =====================
import os, time, json, random, smtplib, ssl
from email.message import EmailMessage
from email.utils import formataddr
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
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.platypus import (
    BaseDocTemplate, Frame, PageTemplate, Paragraph, Spacer, Table, TableStyle, Image
)

# ---- HTML (Newsletter) ----
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

# ================== Fonts ==================
KOREAN_FONT = "HYSMyeongJo-Medium"   # ReportLab 내장 CJK
pdfmetrics.registerFont(UnicodeCIDFont(KOREAN_FONT))

try:
    CANDS = [
        "/usr/share/fonts/truetype/nanum/NanumGothic.ttf",
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
    ]
    for p in CANDS:
        if os.path.exists(p):
            fm.fontManager.addfont(p)
            plt.rcParams["font.family"] = os.path.splitext(os.path.basename(p))[0].replace(".ttf","")
            break
    plt.rcParams["axes.unicode_minus"] = False
except Exception:
    plt.rcParams["axes.unicode_minus"] = False

# ================== Data ==================
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

def cg_get(path, params=None, retry=3, timeout=15):
    last=None
    for _ in range(retry):
        try:
            r=requests.get(f"{CG}{path}", params=params, timeout=timeout,
                           headers={"User-Agent":"Mozilla/5.0"})
            r.raise_for_status(); return r.json()
        except Exception as e:
            last=e; time.sleep(0.6+0.4*random.random())
    raise last

# 1) markets
mkts = cg_get("/coins/markets", {
  "vs_currency":"usd","ids":",".join(BM20_IDS),
  "order":"market_cap_desc","per_page":len(BM20_IDS),"page":1,
  "price_change_percentage":"24h"
})
df = pd.DataFrame([{
  "id":m["id"], "name":m["symbol"].upper(),
  "current_price":safe_float(m["current_price"]),
  "market_cap":safe_float(m["market_cap"]),
  "chg24":safe_float(m.get("price_change_percentage_24h"),0.0),
} for m in mkts]).sort_values("market_cap", ascending=False).reset_index(drop=True)
df = df.head(20).reset_index(drop=True)

# 2) yday close (KST)
def get_yday_close(cid):
    data = cg_get(f"/coins/{cid}/market_chart", {"vs_currency":"usd","days":2})
    prices = data.get("prices", [])
    if not prices: return None
    yday = (datetime.now(KST) - timedelta(days=1)).date()
    series = [(datetime.fromtimestamp(p[0]/1000, timezone.utc), p[1]) for p in prices]
    yvals = [p for (t,p) in series if t.astimezone(KST).date()==yday]
    if yvals: return float(yvals[-1])
    return float(series[-2][1]) if len(series)>=2 else float(series[-1][1])

prevs=[]
for cid in df["id"]:
    try: prevs.append(get_yday_close(cid))
    except Exception: prevs.append(None)
    time.sleep(0.2)
for i,r in df.iterrows():
    if prevs[i] in (None,0):
        prevs[i] = r["current_price"]/(1+(r["chg24"] or 0)/100.0)
df["previous_price"]=prevs

# 3) weights
df["weight_raw"]=df["market_cap"]/max(df["market_cap"].sum(),1.0)
df["weight_ratio"]=df.apply(
    lambda r: min(r["weight_raw"], BTC_CAP if r["name"]=="BTC" else OTH_CAP),
    axis=1
)
df["weight_ratio"]=df["weight_ratio"]/df["weight_ratio"].sum()

# 4) Kimchi premium
def get_kp(df):
    def _req(url, params=None, retry=3, timeout=12):
        last=None
        for i in range(retry):
            try:
                r=requests.get(url, params=params, timeout=timeout,
                               headers={"User-Agent":"Mozilla/5.0"})
                r.raise_for_status(); return r.json()
            except Exception as e:
                last=e; time.sleep(0.6*(i+1))
        raise last
    # KRW
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
    # USD
    try:
        btc_usd=float(df.loc[df["name"]=="BTC","current_price"].iloc[0]); glb="df"
    except Exception:
        btc_usd=None; glb=None
    if btc_usd is None:
        for url,pr in [
          ("https://api.binance.com/api/v3/ticker/price", {"symbol":"BTCUSDT"}),
          ("https://api1.binance.com/api/v3/ticker/price", {"symbol":"BTCUSDT"}),
        ]:
            try:
                j=_req(url, pr); btc_usd=float(j["price"]); glb=url; break
            except Exception:
                continue
        if btc_usd is None:
            try:
                cg=_req(f"{CG}/simple/price", {"ids":"bitcoin","vs_currencies":"usd"})
                btc_usd=float(cg["bitcoin"]["usd"]); glb="cg_usd"
            except Exception:
                return None, {"dom":dom,"glb":"fallback0","fx":"fixed1350",
                              "btc_krw":round(btc_krw,2),"btc_usd":None,"usdkrw":1350.0}
    # FX
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

# 5) index stats
df["price_change_pct"]=(df["current_price"]/df["previous_price"]-1)*100
df["contribution"]=(df["current_price"]-df["previous_price"])*df["weight_ratio"]
bm20_prev=float((df["previous_price"]*df["weight_ratio"]).sum())
bm20_now=float((df["current_price"]*df["weight_ratio"]).sum())
bm20_chg=(bm20_now/bm20_prev-1)*100 if bm20_prev else 0.0
num_up=int((df["price_change_pct"]>0).sum()); num_down=int((df["price_change_pct"]<0).sum())

top3=df.sort_values("contribution", ascending=False).head(3).reset_index(drop=True)
bot2=df.sort_values("contribution", ascending=True).head(2).reset_index(drop=True)

btc_row=df.loc[df["name"]=="BTC"].iloc[0]; btc_pct=btc_row["price_change_pct"]
lead2,lead3=top3.iloc[1], top3.iloc[2]; lag1,lag2=bot2.iloc[0], bot2.iloc[1]
trend_word="상승" if bm20_chg>=0 else "하락"
verb_btc="오르며" if btc_pct>=0 else "내리며"
limit_phrase="지수 상승을 제한했다." if bm20_chg>=0 else "지수 하락을 키웠다."
dominance="상승이 압도적으로 많았다." if num_up>(num_down+2) else ("상승이 우세했다." if num_up>num_down else "하락이 우세했다.")
kp_text = f"{kimchi_pct:.2f}%" if kimchi_pct is not None else "집계 지연"

news_lines=[
  f"비트코인과 이더리움을 포함한 대형코인 위주의 BM20 지수는 전일대비 {bm20_chg:+.2f}% {trend_word}한 {bm20_now:,.0f}pt를 기록했다.",
  f"이 가운데 비트코인(BTC)이 {btc_pct:+.2f}% {verb_btc} 지수 {('상승' if bm20_chg>=0 else '하락')}을 견인했고, "
  f"{lead2['name']}({lead2['price_change_pct']:+.2f}%), {lead3['name']}({lead3['price_change_pct']:+.2f}%)도 긍정적으로 기여했다.",
  f"반면, {lag1['name']}({lag1['price_change_pct']:+.2f}%), {lag2['name']}({lag2['price_change_pct']:+.2f}%)는 하락, {limit_phrase}",
  f"이날 대형 코인 20개 중 상승한 자산은 {num_up}개였고, 하락한 코인은 {num_down}개로 {dominance}",
  f"한편, 업비트와 빗썸 등 한국 주요 거래소와 바이낸스 등 해외거래소와의 비트코인 가격 차이를 나타내는 k-bm 프리미엄(김치프리미엄)은 {kp_text}로 집계됐다."
]
news=" ".join(news_lines)

# Save text/csv
with open(txt_path,"w",encoding="utf-8") as f: f.write(news)
df[["name","current_price","previous_price","weight_ratio","price_change_pct"]].to_csv(csv_path, index=False, encoding="utf-8")

# ================== Charts ==================
# A) top/bottom bar
winners=df.sort_values("price_change_pct", ascending=False).head(TOP_UP)
losers=df.sort_values("price_change_pct", ascending=True).head(TOP_DOWN)
bar=pd.concat([winners[["name","price_change_pct"]], losers[["name","price_change_pct"]]])
plt.figure()
colors_bar=["tab:green" if v>=0 else "tab:red" for v in bar["price_change_pct"]]
plt.barh(bar["name"], bar["price_change_pct"], color=colors_bar)
plt.axvline(0, linewidth=1)
plt.title(f"BM20 Daily Performance  ({YMD})"); plt.xlabel("Daily Change (%)")
plt.tight_layout(); plt.savefig(bar_png, dpi=180); plt.close()

# B) BTC/ETH 7D trend (pct from start)
def get_pct_series(coin_id, days=8):
    data=cg_get(f"/coins/{coin_id}/market_chart", {"vs_currency":"usd","days":days})
    prices=data.get("prices",[])
    if not prices: return []
    s=[p[1] for p in prices]; base=s[0]
    return [ (v/base-1)*100 for v in s ]

btc7=get_pct_series("bitcoin", 8); eth7=get_pct_series("ethereum", 8)
x=range(max(len(btc7), len(eth7)))
plt.figure()
plt.plot(range(len(btc7)), btc7, label="BTC 7D")
plt.plot(range(len(eth7)), eth7, label="ETH 7D")
plt.legend(); plt.title(f"BTC/ETH 7D Trend ({YMD})"); plt.ylabel("% Change from Start")
plt.tight_layout(); plt.savefig(trend_png, dpi=180); plt.close()

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
left = Frame(doc.leftMargin, doc.bottomMargin, col_w, col_h, id='left')
right= Frame(doc.leftMargin + col_w + col_gap, doc.bottomMargin, col_w, col_h, id='right')
doc.addPageTemplates([PageTemplate(id='TwoCol', frames=[left, right])])

story_left, story_right = [], []
story_left += [Paragraph(f"BM20 데일리 리포트  {YMD}", styles["K-Title"]), Spacer(1,0.25*cm)]

summary_tbl = Table([
    ["BM20 지수", f"{bm20_now:,.0f} pt"],
    ["일간 변동", f"{bm20_chg:+.2f}%"],
    ["상승/하락", f"{num_up} / {num_down}"],
    ["김치 프리미엄", f"{kp_text}"],
], colWidths=[3.2*cm, col_w-3.2*cm-0.2*cm])
summary_tbl.setStyle(TableStyle([
    ("FONTNAME",(0,0),(-1,-1), KOREAN_FONT),
    ("FONTSIZE",(0,0),(-1,-1),10.5),
    ("BACKGROUND",(0,0),(-1,0), colors.HexColor("#ECEFF1")),
    ("TEXTCOLOR",(0,0),(0,-1), colors.HexColor("#1A237E")),
    ("BOX",(0,0),(-1,-1),0.5,colors.HexColor("#B0BEC5")),
    ("INNERGRID",(0,0),(-1,-1),0.25,colors.HexColor("#CFD8DC")),
    ("ALIGN",(1,0),(1,-1),"RIGHT"),
    ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
]))
story_left += [summary_tbl, Spacer(1, 0.35*cm)]

for ln in news_lines:
    story_left += [Paragraph(ln, styles["K-Body"]), Spacer(1, 0.12*cm)]
story_left += [Spacer(1, 0.3*cm), Paragraph("Top 상승/하락", styles["K-Strong"]), Spacer(1, 0.2*cm)]

tbl_data = [["종목","일간 등락(%)"]]
for _,r in winners.iterrows(): tbl_data.append([r["name"], f"{r['price_change_pct']:+.2f}"])
for _,r in losers.iterrows():  tbl_data.append([r["name"], f"{r['price_change_pct']:+.2f}"])
tbl = Table(tbl_data, colWidths=[col_w*0.45, col_w*0.45])
tbl.setStyle(TableStyle([
    ("FONTNAME",(0,0),(-1,-1), KOREAN_FONT),
    ("FONTSIZE",(0,0),(-1,-1),10),
    ("BACKGROUND",(0,0),(-1,0), colors.HexColor("#E3F2FD")),
    ("BOX",(0,0),(-1,-1),0.5,colors.HexColor("#90CAF9")),
    ("INNERGRID",(0,0),(-1,-1),0.25,colors.HexColor("#BBDEFB")),
    ("ALIGN",(1,1),(1,-1),"RIGHT"),
]))
story_left += [tbl]

if os.path.exists(bar_png):
    story_right += [Image(bar_png, width=col_w, height=col_w*0.55), Spacer(1,0.3*cm)]
if os.path.exists(trend_png):
    story_right += [Image(trend_png, width=col_w, height=col_w*0.60)]

doc.build(story_left + story_right)

# ================== HTML Newsletter ==================
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
      <div><b>김치 프리미엄</b><br>{{ kp_text }}</div>
    </div>
    <h2>요약</h2>
    <p>{{ news }}</p>
  </div>
  <div class="card">
    <h2>Top 상승/하락</h2>
    <table><tr><th>종목</th><th class="right">일간 등락(%)</th></tr>
      {% for row in table_rows %}<tr><td>{{ row.name }}</td><td class="right">{{ row.chg }}</td></tr>{% endfor %}
    </table>
  </div>
  <div class="card">
    <h2>차트</h2>
    {% if bar_png %}<p><img src="{{ bar_png }}" alt="BM20 Top/Bottom"></p>{% endif %}
    {% if trend_png %}<p><img src="{{ trend_png }}" alt="BTC/ETH 7D Trend"></p>{% endif %}
  </div>
  <div class="footer">Data: CoinGecko, Upbit · © Blockmedia BM20</div>
</div></body></html>
""")

table_rows=[]
for _,r in winners.iterrows(): table_rows.append({"name": r["name"], "chg": f"{r['price_change_pct']:+.2f}%"})
for _,r in losers.iterrows():  table_rows.append({"name": r["name"], "chg": f"{r['price_change_pct']:+.2f}%"})

html = html_tpl.render(
    ymd=YMD, bm20_now=f"{bm20_now:,.0f}", bm20_chg=f"{bm20_chg:+.2f}%",
    num_up=num_up, num_down=num_down, kp_text=kp_text, news=news,
    table_rows=table_rows, bar_png=os.path.basename(bar_png), trend_png=os.path.basename(trend_png)
)
with open(html_path, "w", encoding="utf-8") as f: f.write(html)

# ================== Gmail Send ==================
def send_email_gmail(subject: str, html_body: str, attachments: list):
    user = os.getenv("GMAIL_USER")
    app_pw = os.getenv("GMAIL_APP_PASS")
    to_raw = os.getenv("MAIL_TO", "")
    from_name = os.getenv("MAIL_FROM_NAME", "BM20 Bot")
    if not user or not app_pw or not to_raw:
        print("[mail] GMAIL_USER/GMAIL_APP_PASS/MAIL_TO not set → skip"); return
    to_list = [t.strip() for t in to_raw.split(",") if t.strip()]
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = formataddr((from_name, user))
    msg["To"] = ", ".join(to_list)
    plain = f"""[BM20 데일리] {YMD}
BM20 지수: {bm20_now:,.0f}pt
일간 변동: {bm20_chg:+.2f}%
상승/하락: {num_up}/{num_down}
김치 프리미엄: {kp_text}

요약: {news}
"""
    msg.set_content(plain)
    msg.add_alternative(html_body, subtype="html")
    for path in attachments:
        if not path or not os.path.exists(path): continue
        with open(path, "rb") as f: data=f.read()
        fname=os.path.basename(path); lower=fname.lower()
        if   lower.endswith(".pdf"):  mt,st="application","pdf"
        elif lower.endswith(".png"):  mt,st="image","png"
        elif lower.endswith(".csv"):  mt,st="text","csv"
        elif lower.endswith(".txt"):  mt,st="text","plain"
        elif lower.endswith(".html"): mt,st="text","html"
        else:                         mt,st="application","octet-stream"
        msg.add_attachment(data, maintype=mt, subtype=st, filename=fname)
    with smtplib.SMTP("smtp.gmail.com", 587, timeout=60) as smtp:
        smtp.ehlo(); smtp.starttls(context=ssl.create_default_context())
        smtp.login(user, app_pw); smtp.send_message(msg)
    print(f"[mail] sent to: {msg['To']}")

mail_subject = f"[BM20 Daily] {YMD}  BM20 {bm20_chg:+.2f}% ({bm20_now:,.0f}pt)"
mail_html = f"""
<h2>BM20 데일리 리포트 <span style='color:#666'>{YMD}</span></h2>
<ul>
  <li><b>BM20 지수</b>: {bm20_now:,.0f} pt</li>
  <li><b>일간 변동</b>: {bm20_chg:+.2f}%</li>
  <li><b>상승/하락</b>: {num_up}/{num_down}</li>
  <li><b>김치 프리미엄</b>: {kp_text}</li>
</ul>
<p style="line-height:1.6">{news}</p>
<p>첨부: PDF 리포트, 차트 PNG, CSV/TXT</p>
"""
send_email_gmail(mail_subject, mail_html, [pdf_path, bar_png, trend_png, csv_path, txt_path, html_path])

# ================== Kimchi meta log ==================
with open(kp_path, "w", encoding="utf-8") as f:
    json.dump({"date":YMD, **(kp_meta or {}), "kimchi_pct": (None if kimchi_pct is None else round(float(kimchi_pct),4))},
              f, ensure_ascii=False)

print("Saved:", txt_path, csv_path, bar_png, trend_png, pdf_path, html_path, kp_path)

