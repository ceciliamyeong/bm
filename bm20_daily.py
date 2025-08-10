import os, time, json, random
from datetime import datetime, timedelta, timezone
import requests, pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib.units import cm

# === 기본 설정 ===
OUT_DIR = os.getenv("OUT_DIR", "out")
os.makedirs(OUT_DIR, exist_ok=True)

today = datetime.now()
YMD = today.strftime("%Y-%m-%d")
CG = "https://api.coingecko.com/api/v3"
BTC_CAP, OTH_CAP = 0.30, 0.15
TOP_UP, TOP_DOWN = 6, 4

# BM20 = 기존 20 + DOGE 후보 → 시총 Top20 유지
BM20_IDS = [
  "bitcoin","ethereum","solana","ripple","binancecoin","toncoin","avalanche-2",
  "chainlink","cardano","polygon","near","polkadot","cosmos","litecoin",
  "arbitrum","optimism","internet-computer","aptos","filecoin","sui","dogecoin"
]

def cg_get(path, params=None, retry=3, timeout=15):
    last=None
    for i in range(retry):
        try:
            r=requests.get(f"{CG}{path}", params=params, timeout=timeout,
                           headers={"User-Agent":"Mozilla/5.0"})
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last=e; time.sleep(0.6+0.4*random.random())
    raise last

def safe_float(x, d=0.0):
    try: return float(x)
    except: return d

# 1) 실시간 시세/시총 (CG)
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

# → 21 후보 중 상위 20개만 유지(최하위 자동 탈락)
df = df.head(20).reset_index(drop=True)

# 2) 전일 종가 근사 (/market_chart 어제 마지막 값)
def get_yday_close(cid):
    data = cg_get(f"/coins/{cid}/market_chart", {"vs_currency":"usd","days":2})
    prices = data.get("prices", [])
    if not prices: return None
    tz = timezone.utc
    yday = datetime.now(tz).date() - timedelta(days=1)
    series = [(datetime.fromtimestamp(p[0]/1000,tz), p[1]) for p in prices]
    yvals = [p for (t,p) in series if t.date()==yday]
    if yvals: return float(yvals[-1])
    return float(series[-2][1]) if len(series)>=2 else float(series[-1][1])

prevs=[]
for cid in df["id"]:
    try: prevs.append(get_yday_close(cid))
    except Exception: prevs.append(None); time.sleep(0.25)
for i,r in df.iterrows():
    if prevs[i] in (None,0):
        prevs[i] = r["current_price"]/(1+(r["chg24"] or 0)/100.0)
df["previous_price"]=prevs

# 3) 가중치(상한→정규화)
df["weight_raw"]=df["market_cap"]/max(df["market_cap"].sum(),1.0)
df["weight_ratio"]=df.apply(lambda r: min(r["weight_raw"], BTC_CAP if r["name"]=="BTC" else OTH_CAP), axis=1)
df["weight_ratio"]=df["weight_ratio"]/df["weight_ratio"].sum()

# 4) 김치 프리미엄 (df 재활용 + 여러 우회 경로)
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
    # KRW (Upbit → CG KRW → 실패 시 0)
    try:
        u=_req("https://api.upbit.com/v1/ticker", {"markets":"KRW-BTC"})
        btc_krw=float(u[0]["trade_price"]); dom="upbit"
    except Exception:
        try:
            cg=_req(f"{CG}/simple/price", {"ids":"bitcoin","vs_currencies":"krw"})
            btc_krw=float(cg["bitcoin"]["krw"]); dom="cg_krw"
        except Exception:
            return 0.0, {"dom":"fallback0","glb":"df","fx":"fixed1350",
                         "btc_krw":None,"btc_usd":None,"usdkrw":1350.0}
    # USD (df의 BTC 가격 재활용)
    try:
        btc_usd=float(df.loc[df["name"]=="BTC","current_price"].iloc[0]); glb="df"
    except Exception:
        btc_usd=None; glb=None
    if btc_usd is None:
        for url,pr in [
          ("https://api.binance.com/api/v3/ticker/price", {"symbol":"BTCUSDT"}),
          ("https://api1.binance.com/api/v3/ticker/price", {"symbol":"BTCUSDT"}),
          ("https://api.exchange.coinbase.com/products/BTC-USD/ticker", None),
          ("https://www.okx.com/api/v5/market/ticker", {"instId":"BTC-USDT"}),
          ("https://api.kraken.com/0/public/Ticker", {"pair":"XBTUSD"}),
        ]:
            try:
                j=_req(url, pr)
                if isinstance(j,dict) and "price" in j: btc_usd=float(j["price"]); glb=url; break
                if "data" in j and isinstance(j["data"],list): btc_usd=float(j["data"][0]["last"]); glb=url; break
                if "result" in j and "XXBTZUSD" in j["result"]: btc_usd=float(j["result"]["XXBTZUSD"]["c"][0]); glb=url; break
            except Exception: continue
        if btc_usd is None:
            try:
                cg=_req(f"{CG}/simple/price", {"ids":"bitcoin","vs_currencies":"usd"})
                btc_usd=float(cg["bitcoin"]["usd"]); glb="cg_usd"
            except Exception:
                return 0.0, {"dom":dom,"glb":"fallback0","fx":"fixed1350",
                             "btc_krw":round(btc_krw,2),"btc_usd":None,"usdkrw":1350.0}
    # FX (테더 KRW → 고정 1350)
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

# 5) BM20 계산
df["price_change_pct"]=(df["current_price"]/df["previous_price"]-1)*100
df["contribution"]=(df["current_price"]-df["previous_price"])*df["weight_ratio"]
bm20_prev=float((df["previous_price"]*df["weight_ratio"]).sum())
bm20_now=float((df["current_price"]*df["weight_ratio"]).sum())
bm20_chg=(bm20_now/bm20_prev-1)*100 if bm20_prev else 0.0
num_up=int((df["price_change_pct"]>0).sum())
num_down=int((df["price_change_pct"]<0).sum())

top3=df.sort_values("contribution", ascending=False).head(3).reset_index(drop=True)
bot2=df.sort_values("contribution", ascending=True).head(2).reset_index(drop=True)

# 6) 뉴스(요청 톤 그대로)
btc_row=df.loc[df["name"]=="BTC"].iloc[0]; btc_pct=btc_row["price_change_pct"]
lead2,lead3=top3.iloc[1], top3.iloc[2]; lag1,lag2=bot2.iloc[0], bot2.iloc[1]
trend_word="상승" if bm20_chg>=0 else "하락"
verb_btc="오르며" if btc_pct>=0 else "내리며"
limit_phrase="지수 상승을 제한했다." if bm20_chg>=0 else "지수 하락을 키웠다."
dominance="상승이 압도적으로 많았다." if num_up>(num_down+2) else ("상승이 우세했다." if num_up>num_down else "하락이 우세했다.")

news_lines=[
 f"비트코인과 이더리움을 포함한 대형코인 위주의 BM20 지수는 전일대비 {bm20_chg:+.2f}% {trend_word}한 {bm20_now:,.0f}pt를 기록했다.",
 f"이 가운데 비트코인(BTC)이 {btc_pct:+.2f}% {verb_btc} 지수 {('상승' if bm20_chg>=0 else '하락')}을 견인했고, "
 f"{lead2['name']}({lead2['price_change_pct']:+.2f}%), {lead3['name']}({lead3['price_change_pct']:+.2f}%)도 긍정적으로 기여했다.",
 f"반면, {lag1['name']}({lag1['price_change_pct']:+.2f}%), {lag2['name']}({lag2['price_change_pct']:+.2f}%)는 하락, {limit_phrase}",
 f"이날 대형 코인 20개 중 상승한 자산은 {num_up}개였고, 하락한 코인은 {num_down}개로 {dominance}",
 f"한편, 업비트와 빗썸 등 한국 주요 거래소와 바이낸스 등 해외거래소와의 비트코인 가격 차이를 나타내는 k-bm 프리미엄(김치프리미엄)은 {kimchi_pct:.2f}%로 집계됐다."
]
news=" ".join(news_lines)

# 7) 저장 (TXT/CSV)
txt_path=os.path.join(OUT_DIR, f"bm20_news_{YMD}.txt")
csv_path=os.path.join(OUT_DIR, f"bm20_daily_data_{YMD}.csv")
with open(txt_path,"w",encoding="utf-8") as f: f.write(news)
df[["name","current_price","previous_price","weight_ratio"]].to_csv(csv_path, index=False, encoding="utf-8")

# 8) 차트 → PNG
winners=df.sort_values("price_change_pct", ascending=False).head(TOP_UP)
losers=df.sort_values("price_change_pct", ascending=True).head(TOP_DOWN)
bar=pd.concat([winners[["name","price_change_pct"]], losers[["name","price_change_pct"]]])
colors=["tab:green" if v>=0 else "tab:red" for v in bar["price_change_pct"]]
plt.figure()
plt.barh(bar["name"], bar["price_change_pct"], color=colors)
plt.axvline(0, linewidth=1, color="steelblue")
plt.title(f"BM20 Daily Performance  ({YMD})")
plt.xlabel("Daily Change (%)")
plt.tight_layout()
png_path=os.path.join(OUT_DIR, f"bm20_chart_{YMD}.png")
plt.savefig(png_path, dpi=180); plt.close()

# 9) 한 페이지 PDF
pdf_path=os.path.join(OUT_DIR, f"bm20_daily_{YMD}.pdf")
c=canvas.Canvas(pdf_path, pagesize=A4)
w, h = A4
margin = 1.5*cm
y = h - margin
c.setFont("Helvetica-Bold", 14); c.drawString(margin, y, f"BM20 데일리 리포트  {YMD}")
y -= 0.8*cm; c.setFont("Helvetica", 10)
for line in news_lines:
    for seg in [line[i:i+68] for i in range(0, len(line), 68)]:
        c.drawString(margin, y, seg); y -= 0.5*cm
y -= 0.3*cm
if os.path.exists(png_path):
    img_w = w - 2*margin
    img_h = img_w * 0.5
    c.drawImage(png_path, margin, margin, width=img_w, height=img_h, preserveAspectRatio=True, anchor='sw')
c.showPage(); c.save()

print("Saved:", txt_path, csv_path, png_path, pdf_path)
