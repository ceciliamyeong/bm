# scripts/generate_report.py
# BM20 데일리 리포트 (index 최신 + /archive/날짜.html)
# - 김프/펀딩비 항상 표기 (Upbit, Binance, 환율)
# - 뉴스(조선비즈 톤) 자동 생성
# - 차트 overflow 방지 / 이미지 내 한글 제목 제거(HTML 제목으로 대체)
# - BM20: 대형 코인 위주 + 국내상장 1.3× 가중치, 랩트/파생 토큰 제외

import os, io, time, random, base64, datetime, requests
import pandas as pd
import matplotlib.pyplot as plt

# ========= 공통 설정 =========
KST = datetime.timezone(datetime.timedelta(hours=9))
TODAY = datetime.datetime.now(KST).strftime("%Y-%m-%d")
ARCHIVE_DIR = "archive"
os.makedirs(ARCHIVE_DIR, exist_ok=True)

RATE_SLEEP = float(os.getenv("BM20_RATE_SLEEP", "1.2"))
MAX_RETRY  = int(os.getenv("BM20_MAX_RETRY", "5"))
CG_API_KEY = os.getenv("CG_API_KEY")
HDR = {"User-Agent": "bm20-bot/1.0"}
if CG_API_KEY:
    HDR["x-cg-pro-api-key"] = CG_API_KEY

def get(url, params=None, headers=None, timeout=30, max_retry=MAX_RETRY, sleep=RATE_SLEEP):
    """재시도/백오프 GET"""
    h = dict(HDR); 
    if headers: h.update(headers)
    for i in range(max_retry):
        r = requests.get(url, params=params, headers=h, timeout=timeout)
        if r.status_code == 200:
            time.sleep(sleep); return r.json()
        if r.status_code in (429, 503):
            time.sleep(sleep*(2**i)+random.uniform(0,0.6)); continue
        time.sleep(sleep)
    r.raise_for_status()

# ========= 유니버스/필터 =========
STABLES = {"USDT","USDC","FDUSD","BUSD","TUSD","DAI","USDP","UST","EURS","PYUSD"}
WRAPPED = {  # 랩트/파생 토큰 제외
    "WBTC","WETH","WBETH","WSTETH","STETH","RETH","CBETH","SDAI","USDe","USDT.e","USDC.e",
}
# 대형 코인 후보(상황에 따라 일부 빠질 수 있음)
MAJOR_POOL = {
    "BTC","ETH","XRP","ADA","SOL","DOGE","TRX","DOT","LINK","MATIC","LTC","BCH",
    "ATOM","AVAX","ARB","OP","NEAR","APT","SUI","ICP","FIL","ETC","HBAR","STX",
    "ALGO","IMX","INJ","TIA","RNDR","TON","XLM","PEPE","SHIB","AAVE","UNI","SEI"
}

# ========= 외부 데이터: 김프/펀딩 =========
def kimchi_premium_btc():
    try:
        up = get("https://api.upbit.com/v1/ticker", {"markets":"KRW-BTC"})[0]["trade_price"]
        fx = get("https://api.exchangerate.host/latest", {"base":"USD","symbols":"KRW"})["rates"]["KRW"]
        binance = float(get("https://api.binance.com/api/v3/ticker/price", {"symbol":"BTCUSDT"})["price"])
        up_usd = up / fx
        prem = (up_usd / binance - 1.0) * 100.0
        meta = {"upbit_krw": up, "usdk_rw": fx, "binance": binance}
        return prem, meta
    except Exception:
        return None, None

def funding(symbol):
    try:
        j = get("https://fapi.binance.com/fapi/v1/fundingRate", {"symbol":symbol, "limit":1}, timeout=15)
        if j: return float(j[0]["fundingRate"]) * 100.0
    except Exception:
        pass
    return None

# ========= CoinGecko 마켓 =========
def cg(path, **params):
    return get(f"https://api.coingecko.com/api/v3/{path}", params)

markets = cg("coins/markets", vs_currency="usd", order="market_cap_desc",
             per_page=100, page=1, price_change_percentage="24h")

df_all = pd.DataFrame([{
    "id": c["id"],
    "symbol": c["symbol"].upper(),
    "name": c["name"],
    "mcap": c["market_cap"] or 0,
    "pct":  c["price_change_percentage_24h"] or 0.0,
    "vol":  c["total_volume"] or 0.0,
    "price":c["current_price"] or 0.0,
} for c in markets])

# ========= 국내상장 판별(Upbit KRW 마켓) =========
def upbit_krw_symbols():
    try:
        j = get("https://api.upbit.com/v1/market/all", {"isDetails":"false"})
        return {x["market"].split("-")[1].upper() for x in j if x["market"].startswith("KRW-")}
    except Exception:
        return set()

KRW_LISTED = upbit_krw_symbols()

# ========= BM20 구성: 대형 + 국내상장 가중치 1.3 =========
df = df_all.copy()
df = df[(~df["symbol"].isin(STABLES)) & (~df["symbol"].isin(WRAPPED))].copy()
# 대형 후보 우선
df = df[df["symbol"].isin(MAJOR_POOL) | (df["mcap"].rank(ascending=False)<=60)].copy()
df["kr_listed"] = df["symbol"].isin(KRW_LISTED)
df["adj_mcap"]  = df["mcap"] * df["kr_listed"].apply(lambda x: 1.3 if x else 1.0)
# 가중치 기준은 원래 시총(리포트 표기 자연스러움), 선별은 adj_mcap 상위 20
bm20 = df.sort_values("adj_mcap", ascending=False).head(20).copy()
bm20["weight"] = bm20["mcap"] / max(1.0, bm20["mcap"].sum())

# ========= BM20 지수 =========
bm20["ret"] = bm20["pct"] / 100.0
bm20_return     = float((bm20["weight"] * bm20["ret"]).sum())
bm20_index      = 100.0 * (1.0 + bm20_return)
bm20_change_pct = bm20_return * 100.0
up_count        = int((bm20["pct"] >  0).sum())
down_count      = int((bm20["pct"] <= 0).sum())

# ========= 거래량 증가율 TOP3(근사) =========
def volume_spike_top3():
    out = []
    for cid, sym in zip(bm20["id"], bm20["symbol"]):
        try:
            j = cg(f"coins/{cid}/market_chart", vs_currency="usd", days=7)
            vol = pd.DataFrame(j["total_volumes"], columns=["ts","v"])
            med = vol["v"].median()
            last = float(vol["v"].iloc[-1])
            ratio = (last/med) if med else 0.0
            out.append((sym, ratio))
        except Exception:
            out.append((sym, 0.0))
    out.sort(key=lambda x:x[1], reverse=True)
    return out[:3]
vol_top3 = volume_spike_top3()

# ========= BTC/ETH 7일 수익률 =========
def hist7d_ret(coin_id):
    j = cg(f"coins/{coin_id}/market_chart", vs_currency="usd", days=7)
    p = pd.DataFrame(j["prices"], columns=["ts","price"])
    p["price"] = p["price"].astype(float)
    p["ret"]   = (p["price"]/p["price"].iloc[0]-1)*100.0
    return p["ret"]
btc_ret = hist7d_ret("bitcoin")
eth_ret = hist7d_ret("ethereum")

# ========= 김프/펀딩 =========
kimchi_pct, kp_meta = kimchi_premium_btc()
fund_btc = funding("BTCUSDT")
fund_eth = funding("ETHUSDT")

# ========= 차트 (이미지 내 한글 제목 제거) =========
def fig_to_png_b64(fig):
    buf = io.BytesIO(); fig.tight_layout(); fig.savefig(buf, format="png", dpi=160); plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode()

# 1D 바차트
disp = bm20.sort_values("pct", ascending=False)
fig1 = plt.figure(figsize=(10,3))
plt.bar(disp["symbol"], disp["pct"]); plt.ylabel("%")
for x, y in enumerate(disp["pct"]):
    plt.text(x, y+(0.5 if y>=0 else -0.5), f"{y:+.2f}%", ha="center",
             va="bottom" if y>=0 else "top", fontsize=8)
img_bar = fig_to_png_b64(fig1)

# BTC/ETH 7일
fig2 = plt.figure(figsize=(10,3))
plt.plot(btc_ret, label="BTC"); plt.plot(eth_ret, label="ETH")
plt.legend(); plt.ylabel("% (from start)")
img_line = fig_to_png_b64(fig2)

# ========= 뉴스(조선비즈 톤) =========
def fmt_pct(v, n=2):
    if v is None: return "-"
    try: return f"{float(v):+.{n}f}%"
    except: return "-"

def pct(v):
    try: return f"{float(v):+,.2f}%"
    except: return "-"

def pct_abs(v):
    try: return f"{abs(float(v)):.2f}%"
    except: return "-"

def num(v):
    try:
        s = f"{float(v):,.2f}"
        return s.rstrip("0").rstrip(".")
    except: return "-"

def fp(v):
    if v is None: return "-"
    try: return f"{float(v):+.3f}%"
    except: return "-"

top_up  = disp.head(3)
top_dn  = disp.tail(3)
btc_row = df_all[df_all["symbol"].str.upper()=="BTC"].head(1)
eth_row = df_all[df_all["symbol"].str.upper()=="ETH"].head(1)

def build_news():
    trend = "상승" if bm20_change_pct>0 else ("하락" if bm20_change_pct<0 else "보합")
    breadth = f"상승 {up_count}·하락 {down_count}"
    ups = ", ".join(f"{r.symbol}(+{r.pct:.2f}%)" for r in top_up.itertuples())
    dns = ", ".join(f"{r.symbol}({r.pct:.2f}%)" for r in top_dn.itertuples())
    btc_line = ""
    if len(btc_row):
        br = btc_row.iloc[0]
        btc_line = f"비트코인(BTC) {pct(br['price_change_percentage_24h'])}, {num(br['current_price'])}달러."
    eth_line = ""
    if len(eth_row):
        er = eth_row.iloc[0]
        eth_line = f"이더리움(ETH) {pct(er['price_change_percentage_24h'])}, {num(er['current_price'])}달러."
    kp_line = f"김치 프리미엄 {fmt_pct(kimchi_pct,2)}" if kimchi_pct is not None else "김치 프리미엄 -"
    fund_line = f"펀딩비 BTC {fp(fund_btc)} · ETH {fp(fund_eth)}"
    title = f"BM20 {pct_abs(bm20_change_pct)} {'상승' if bm20_change_pct>0 else ('하락' if bm20_change_pct<0 else '보합')}… 지수 {num(bm20_index)}pt, {kp_line}"
    body = " ".join([
        f"{TODAY} BM20 지수 {pct(bm20_change_pct)} {trend}.",
        f"시장 체력은 {breadth}.",
        (f"상승 상위 {ups}." if bm20_change_pct>=0 else f"하락 상위 {dns}."),
        (f"대비 {dns} 약세." if bm20_change_pct>=0 else f"반면 {ups} 강세."),
        btc_line, eth_line, f"{kp_line}, {fund_line}."
    ])
    return title, body

news_title, news_body = build_news()

# ========= HTML =========
def esc(s): return str(s).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
def metric_row(k, v): return f"<tr><td class='k'>{esc(k)}</td><td class='v'>{esc(v)}</td></tr>"

metrics_html = "".join([
    metric_row("지수", f"{bm20_index:,.2f} pt"),
    metric_row("일간 변동", f"{bm20_change_pct:+.2f}%"),
    metric_row("상승/하락", f"{up_count} / {down_count}"),
    metric_row("김치 프리미엄", fmt_pct(kimchi_pct,2)),
    metric_row("펀딩비(Binance)", f"BTC {fp(fund_btc)} / ETH {fp(fund_eth)}"),
])

rows_weight = "\n".join(
    f"<tr><td>{esc(r.symbol)}</td>"
    f"<td style='text-align:right'>{'예' if r.kr_listed else '아니오'}</td>"
    f"<td style='text-align:right'>{r.weight*100:,.2f}%</td>"
    f"<td style='text-align:right'>{r.pct:+.2f}%</td></tr>"
    for r in bm20.itertuples()
)

vol_rows = "\n".join(
    f"<tr><td>{sym}</td><td style='text-align:right'>+{ratio*100:,.2f}%</td></tr>"
    for sym, ratio in vol_top3
)

top_up_rows = "\n".join(
    f"<tr><td>{esc(r.symbol)}</td><td style='text-align:right'>+{r.pct:.2f}%</td></tr>"
    for r in top_up.itertuples()
)
top_dn_rows = "\n".join(
    f"<tr><td>{esc(r.symbol)}</td><td style='text-align:right'>{r.pct:.2f}%</td></tr>"
    for r in top_dn.itertuples()
)

# 외부 뉴스 본문(선택)
news_html_extra = ""
try:
    if os.path.exists("news/today.md"):
        with open("news/today.md","r",encoding="utf-8") as f:
            md = f.read().strip()
        news_html_extra = "<p>" + md.replace("\n\n","</p><p>").replace("\n"," ") + "</p>"
except Exception:
    pass

HTML = f"""<!doctype html><html lang="ko"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>BM20 데일리 리포트 — {TODAY}</title>
<style>
body{{font-family:-apple-system,BlinkMacSystemFont,'Malgun Gothic',Arial,sans-serif;background:#f3f6fb;color:#111;margin:0}}
.wrap{{max-width:1000px;margin:0 auto;padding:16px}}
.card{{background:#fff;border:1px solid #e2e8f3;border-radius:12px;padding:14px;margin:12px 0;box-shadow:0 1px 0 #eef2f7}}
h1{{font-size:28px;margin:0 0 6px 0;text-align:center}} .muted{{text-align:center;color:#667}}
h2{{font-size:18px;color:#123b8a;margin:8px 0}}
h3{{font-size:20px;margin:6px 0}}
table.meta{{width:100%;border-collapse:collapse}} table.meta td{{border:1px solid #e5eaf3;padding:10px}}
table.meta td.k{{width:160px;background:#f6f9ff}}
table.std{{width:100%;border-collapse:collapse}} table.std th,table.std td{{border:1px solid #e5eaf3;padding:8px}} table.std th{{background:#eef4ff}}
.grid2{{display:grid;grid-template-columns:1fr 1fr;gap:12px}}
.chart img{{max-width:100%;height:auto;display:block;margin:0 auto}}
.badge{{display:inline-block;background:#f0f4ff;border:1px solid #dbe7ff;padding:3px 8px;border-radius:999px;margin-right:6px}}
</style></head><body><div class="wrap">

<div class="card">
  <h1>BM20 데일리 리포트</h1>
  <div class="muted">{TODAY}</div>
  <div style="margin:8px 0">
    <span class="badge">구성: 대형 20</span>
    <span class="badge">국내상장 1.3× 가중치</span>
  </div>
  <table class="meta">{metrics_html}</table>
</div>

<div class="card">
  <h2>BM20 데일리 뉴스</h2>
  <h3>{esc(news_title)}</h3>
  <p>{esc(news_body)}</p>
  {news_html_extra}
</div>

<div class="card">
  <h2>코인별 퍼포먼스 (1D, USD)</h2>
  <div class="chart"><img alt="1D Performance" src="data:image/png;base64,{img_bar}"></div>
</div>

<div class="grid2">
  <div class="card">
    <h2>상승 TOP3</h2>
    <table class="std"><tr><th>종목</th><th>등락률</th></tr>{top_up_rows}</table>
  </div>
  <div class="card">
    <h2>하락 TOP3</h2>
    <table class="std"><tr><th>종목</th><th>등락률</th></tr>{top_dn_rows}</table>
  </div>
</div>

<div class="card">
  <h2>거래량 증가율 TOP3 (근사)</h2>
  <table class="std"><tr><th>종목</th><th>증가율</th></tr>{vol_rows}</table>
</div>

<div class="card">
  <h2>BTC & ETH 7일 가격 추세</h2>
  <div class="chart"><img alt="7D Trend" src="data:image/png;base64,{img_line}"></div>
</div>

<div class="card">
  <h2>구성 & 가중치</h2>
  <table class="std">
    <tr><th>심볼</th><th>국내 상장</th><th>가중치</th><th>1D 등락</th></tr>
    {rows_weight}
  </table>
</div>

<div class="muted" style="font-size:12px;margin:8px 0;text-align:center">
  © Blockmedia · Data: CoinGecko / Upbit / Binance · FX: exchangerate.host
</div>
</div></body></html>
"""

# ========= 저장 =========
os.makedirs(ARCHIVE_DIR, exist_ok=True)
with open(f"{ARCHIVE_DIR}/bm20_daily_{TODAY}.html","w",encoding="utf-8") as f: f.write(HTML)
with open("index.html","w",encoding="utf-8") as f: f.write(HTML)
with open(".nojekyll","w",encoding="utf-8") as f: f.write("")

# 아카이브 인덱스
items = sorted([p for p in os.listdir(ARCHIVE_DIR) if p.startswith("bm20_daily_") and p.endswith(".html")])
links = "\n".join([f'<li><a href="{p}">{p.replace("bm20_daily_","").replace(".html","")}</a></li>' for p in items])
with open(f"{ARCHIVE_DIR}/index.html","w",encoding="utf-8") as f:
    f.write(f'<!doctype html><meta charset="UTF-8"><title>BM20 Archive</title><h1>BM20 데일리 아카이브</h1><ul>{links}</ul>')

print("Saved index + archive. Kimchi:", kimchi_pct, "Funding:", fund_btc, fund_eth)

