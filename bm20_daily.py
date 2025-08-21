# ===================== BM20 Daily — Final (KST, date-folder, robust KP) =====================
import os, time, json, random
from datetime import datetime, timedelta, timezone
import requests
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib.units import cm

def plot_bm20_history(history_csv="out/history/bm20_index_history.csv",
                      out_png="bm20_history_latest.png"):
    if not os.path.exists(history_csv):
        return False
    try:
        hist = pd.read_csv(history_csv)
        # 예상 컬럼: date (YYYY-MM-DD), bm20_level (float)
        hist["date"] = pd.to_datetime(hist["date"])
        hist = hist.sort_values("date").dropna(subset=["bm20_level"])
        if hist.empty:
            return False
    except Exception:
        return False

    # 다크 테마
    plt.rcParams.update({
        "figure.facecolor":"#0b1020",
        "axes.facecolor":"#121831",
        "axes.edgecolor":"#28324d",
        "axes.labelcolor":"#e6ebff",
        "xtick.color":"#cfd6ff",
        "ytick.color":"#cfd6ff",
        "text.color":"#e6ebff",
        "savefig.facecolor":"#0b1020",
        "savefig.bbox":"tight",
        "font.size":11,
    })

    fig, ax = plt.subplots(figsize=(10,5), dpi=150)
    ax.plot(hist["date"], hist["bm20_level"], linewidth=1.6)
    ax.axhline(100, color="#3a4569", lw=1, linestyle="--")  # 2018-01-01=100 기준선
    ax.set_title("BM20 Index (2018-01-01 = 100)")
    ax.set_xlabel("Date")
    ax.set_ylabel("Index Level")
    # 마지막 값 주석
    last_date = hist["date"].iloc[-1]
    last_val  = float(hist["bm20_level"].iloc[-1])
    ax.annotate(f"{last_val:,.0f}",
                xy=(last_date, last_val),
                xytext=(10, 0), textcoords="offset points",
                va="center")
    fig.autofmt_xdate()
    fig.savefig(out_png)
    plt.close(fig)
    return True

# ===== 날짜/폴더 설정 (맨 위에서 정의) =====
OUT_DIR = os.getenv("OUT_DIR", "out")
os.makedirs(OUT_DIR, exist_ok=True)

KST = timezone(timedelta(hours=9))                 # 한국 시간
YMD = datetime.now(KST).strftime("%Y-%m-%d")       # 오늘 날짜(YYYY-MM-DD)

OUT_DIR_DATE = os.path.join(OUT_DIR, YMD)          # 날짜별 하위 폴더
os.makedirs(OUT_DIR_DATE, exist_ok=True)

# ===== 상수 =====
CG = "https://api.coingecko.com/api/v3"
BTC_CAP, OTH_CAP = 0.30, 0.15
TOP_UP, TOP_DOWN = 6, 4

# BM20 = 기존 20 + DOGE 후보 → 시총 Top20 유지
BM20_IDS = [
    "bitcoin","ethereum","solana","ripple","binancecoin","toncoin","avalanche-2",
    "chainlink","cardano","polygon","near","polkadot","cosmos","litecoin",
    "arbitrum","optimism","internet-computer","aptos","filecoin","sui","dogecoin"
]

# ===== 유틸 =====
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

# ===== 1) 실시간 시세/시총 =====
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

# 21 후보 → 상위 20개만 유지
df = df.head(20).reset_index(drop=True)

# ===== 2) 전일 종가 (KST 기준 전날) 근사 =====
def get_yday_close(cid):
    data = cg_get(f"/coins/{cid}/market_chart", {"vs_currency":"usd","days":2})
    prices = data.get("prices", [])
    if not prices: return None
    yday = (datetime.now(KST) - timedelta(days=1)).date()
    # CG 타임스탬프는 UNIX ms (UTC). KST로 변환해 같은 날짜의 마지막 값 선택
    series = [(datetime.fromtimestamp(p[0]/1000, timezone.utc), p[1]) for p in prices]
    yvals = [p for (t,p) in series if t.astimezone(KST).date()==yday]
    if yvals: return float(yvals[-1])
    return float(series[-2][1]) if len(series)>=2 else float(series[-1][1])

prevs=[]
for cid in df["id"]:
    try:
        prevs.append(get_yday_close(cid))
    except Exception:
        prevs.append(None)
    time.sleep(0.25)
# 보정: 누락 시 24h 변동률로 역산
for i,r in df.iterrows():
    if prevs[i] in (None,0):
        prevs[i] = r["current_price"]/(1+(r["chg24"] or 0)/100.0)
df["previous_price"]=prevs

# ===== 3) 가중치 (상한→정규화) =====
df["weight_raw"]=df["market_cap"]/max(df["market_cap"].sum(),1.0)
df["weight_ratio"]=df.apply(
    lambda r: min(r["weight_raw"], BTC_CAP if r["name"]=="BTC" else OTH_CAP),
    axis=1
)
df["weight_ratio"]=df["weight_ratio"]/df["weight_ratio"].sum()

# ===== 4) 김치 프리미엄 (안정화·우회 포함) =====
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
    # KRW (Upbit → CG KRW → 실패 시 None)
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
    # USD (df 가격 재활용 → 필요 시 거래소 우회 → 최후 CG USD)
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
            except Exception:
                continue
        if btc_usd is None:
            try:
                cg=_req(f"{CG}/simple/price", {"ids":"bitcoin","vs_currencies":"usd"})
                btc_usd=float(cg["bitcoin"]["usd"]); glb="cg_usd"
            except Exception:
                return None, {"dom":dom,"glb":"fallback0","fx":"fixed1350",
                              "btc_krw":round(btc_krw,2),"btc_usd":None,"usdkrw":1350.0}
    # FX (USDT→KRW, 비정상시 1350 고정)
    try:
        t=_req(f"{CG}/simple/price", {"ids":"tether","vs_currencies":"krw"})
        usdkrw=float(t["tether"]["krw"]); fx="cg_tether"
        if not (900<=usdkrw<=2000): raise ValueError
    except Exception:
        usdkrw=1350.0; fx="fixed1350"
    kp=((btc_krw/usdkrw)-btc_usd)/btc_usd*100
    return kp, {"dom":dom,"glb":glb,"fx":fx,
                "btc_krw":round(btc_krw,2),"btc_usd":round(btc_usd,2),"usdkrw":round(usdkrw,2)}

kimchi_pct, kp_meta = get_kp(df)  # kimchi_pct: float 또는 None

# ===== 5) BM20 계산 =====
df["price_change_pct"]=(df["current_price"]/df["previous_price"]-1)*100
df["contribution"]=(df["current_price"]-df["previous_price"])*df["weight_ratio"]
bm20_prev=float((df["previous_price"]*df["weight_ratio"]).sum())
bm20_now=float((df["current_price"]*df["weight_ratio"]).sum())
bm20_chg=(bm20_now/bm20_prev-1)*100 if bm20_prev else 0.0
num_up=int((df["price_change_pct"]>0).sum()); num_down=int((df["price_change_pct"]<0).sum())

top3=df.sort_values("contribution", ascending=False).head(3).reset_index(drop=True)
bot2=df.sort_values("contribution", ascending=True).head(2).reset_index(drop=True)

# ===== 6) 뉴스(해석형 톤) =====
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

# ===== 7) 저장 경로 =====
txt_path = os.path.join(OUT_DIR_DATE, f"bm20_news_{YMD}.txt")
csv_path = os.path.join(OUT_DIR_DATE, f"bm20_daily_data_{YMD}.csv")
png_path = os.path.join(OUT_DIR_DATE, f"bm20_chart_{YMD}.png")
pdf_path = os.path.join(OUT_DIR_DATE, f"bm20_daily_{YMD}.pdf")
kp_path  = os.path.join(OUT_DIR_DATE, f"kimchi_{YMD}.json")

# ===== 8) 저장 (TXT/CSV) =====
with open(txt_path,"w",encoding="utf-8") as f: f.write(news)
df[["name","current_price","previous_price","weight_ratio"]].to_csv(csv_path, index=False, encoding="utf-8")

# ===== 9) 차트 (상승=초록/하락=빨강) =====
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
plt.savefig(png_path, dpi=180); plt.close()

# ===== 10) PDF(1페이지) =====
c=canvas.Canvas(pdf_path, pagesize=A4)
w, h = A4; margin = 1.5*cm; y = h - margin
c.setFont("Helvetica-Bold", 14); c.drawString(margin, y, f"BM20 데일리 리포트  {YMD}")
y -= 0.8*cm; c.setFont("Helvetica", 10)
for line in news_lines:
    for seg in [line[i:i+68] for i in range(0, len(line), 68)]:
        c.drawString(margin, y, seg); y -= 0.5*cm
y -= 0.3*cm
if os.path.exists(png_path):
    img_w = w - 2*margin; img_h = img_w * 0.5
    c.drawImage(png_path, margin, margin, width=img_w, height=img_h, preserveAspectRatio=True, anchor='sw')
c.showPage(); c.save()

# ===== 11) 김프 메타 로그 =====
with open(kp_path, "w", encoding="utf-8") as f:
    json.dump({"date":YMD, **(kp_meta or {}), "kimchi_pct": (None if kimchi_pct is None else round(float(kimchi_pct),4))},
              f, ensure_ascii=False)

print("Saved:", txt_path, csv_path, png_path, pdf_path, kp_path)
