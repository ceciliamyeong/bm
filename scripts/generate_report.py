# scripts/generate_report.py
# BM20 데일리 HTML 자동 생성 (index 최신 + /archive/날짜.html)
# - 조선비즈 톤 뉴스(제목+본문) 자동 생성
# - PDF처럼: 헤더 메트릭 / 1D 바차트 / 상승·하락 TOP3 / 거래량 증가율 TOP3
#            / BTC·ETH 7일 추세 / 구성&가중치 표 / (옵션) 외부 뉴스 본문
# - 이미지 Base64 포함, UTF-8 저장
# - CoinGecko 429 백오프, Binance funding 사용
# - (선택) news/today.md 있으면 본문 추가

import requests, datetime, base64, io, os, time, random
import pandas as pd
import matplotlib.pyplot as plt

# ===== 시간/경로 =====
KST = datetime.timezone(datetime.timedelta(hours=9))
TODAY = datetime.datetime.now(KST).strftime("%Y-%m-%d")
ARCHIVE_DIR = "archive"
os.makedirs(ARCHIVE_DIR, exist_ok=True)

# ===== CoinGecko 레이트리밋 대응 =====
RATE_SLEEP = float(os.getenv("BM20_RATE_SLEEP", "1.2"))
MAX_RETRY  = int(os.getenv("BM20_MAX_RETRY", "5"))
CG_API_KEY = os.getenv("CG_API_KEY")
DEFAULT_HEADERS = {"User-Agent": "bm20-bot/1.0"}
if CG_API_KEY:
    DEFAULT_HEADERS["x-cg-pro-api-key"] = CG_API_KEY

def cg(path, **params):
    """CoinGecko GET (재시도/백오프)"""
    url = f"https://api.coingecko.com/api/v3/{path}"
    for i in range(MAX_RETRY):
        r = requests.get(url, params=params, timeout=30, headers=DEFAULT_HEADERS)
        if r.status_code == 200:
            time.sleep(RATE_SLEEP); return r.json()
        if r.status_code == 429:
            time.sleep(RATE_SLEEP*(2**i)+random.uniform(0,0.6)); continue
        time.sleep(RATE_SLEEP)
    r.raise_for_status()

# ===== 데이터: 상위 코인(스테이블 제외) =====
STABLES = {"USDT","USDC","FDUSD","BUSD","TUSD","DAI","USDP","UST","EURS","PYUSD"}

markets = cg("coins/markets", vs_currency="usd", order="market_cap_desc",
             per_page=40, page=1, price_change_percentage="24h")

df = pd.DataFrame([{
    "id":     c["id"],
    "symbol": c["symbol"].upper(),
    "name":   c["name"],
    "mcap":   c["market_cap"] or 0,
    "pct":    (c["price_change_percentage_24h"] or 0.0),
    "vol":    c["total_volume"] or 0.0,
    "price":  c["current_price"] or 0.0,
} for c in markets])
df = df[~df["symbol"].isin(STABLES)].reset_index(drop=True)

# BM20: 시총 상위 20
bm20 = df.sort_values("mcap", ascending=False).head(20).copy()
bm20["weight"] = bm20["mcap"] / bm20["mcap"].sum()

# 지수(전일=100 가정)
bm20["ret"] = bm20["pct"] / 100.0
bm20_return     = float((bm20["weight"] * bm20["ret"]).sum())
bm20_index      = 100.0 * (1.0 + bm20_return)
bm20_change_pct = bm20_return * 100.0

# 상승/하락 개수
up_count   = int((bm20["pct"] >  0).sum())
down_count = int((bm20["pct"] <= 0).sum())

# ===== 거래량 증가율 TOP3(근사) =====
def volume_spike_top3():
    out = []
    for cid, sym in zip(bm20["id"], bm20["symbol"]):
        try:
            j = cg(f"coins/{cid}/market_chart", vs_currency="usd", days=7)
            vol = pd.DataFrame(j["total_volumes"], columns=["ts","v"])
            median7    = vol["v"].median()
            latest_vol = float(vol["v"].iloc[-1])
            ratio = (latest_vol/median7) if median7 else 0.0
            out.append((sym, ratio))
        except Exception:
            out.append((sym, 0.0))
    out.sort(key=lambda x:x[1], reverse=True)
    return out[:3]

vol_top3 = volume_spike_top3()

# ===== BTC/ETH 7일 수익률 =====
def hist7d_ret(coin_id):
    j = cg(f"coins/{coin_id}/market_chart", vs_currency="usd", days=7)
    p = pd.DataFrame(j["prices"], columns=["ts","price"])
    p["price"] = p["price"].astype(float)
    p["ret"]   = (p["price"]/p["price"].iloc[0]-1)*100.0
    return p["ret"]

btc_ret = hist7d_ret("bitcoin")
eth_ret = hist7d_ret("ethereum")

# ===== 펀딩비(Binance) =====
def funding_rate(symbol="BTCUSDT"):
    try:
        r = requests.get("https://fapi.binance.com/fapi/v1/fundingRate",
                         params={"symbol": symbol, "limit": 1}, timeout=15)
        if r.status_code==200:
            d = r.json()
            if d: return float(d[0]["fundingRate"])*100.0
    except Exception:
        pass
    return None

k_prem  = None  # 김프는 공용 API 한계로 빈칸(나중에 별도 소스 연결 가능)
fund_btc = funding_rate("BTCUSDT")
fund_eth = funding_rate("ETHUSDT")

# ===== 차트(Base64) =====
def fig_to_png_base64(fig):
    buf = io.BytesIO()
    fig.tight_layout()
    fig.savefig(buf, format="png", dpi=160)
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode()

# 1D 퍼포먼스 바차트
disp = bm20.sort_values("pct", ascending=False)
fig_bar = plt.figure(figsize=(10,3))
plt.bar(disp["symbol"], disp["pct"])
plt.title("코인별 퍼포먼스 (1D, USD)"); plt.ylabel("%")
for x,y in enumerate(disp["pct"]):
    plt.text(x, y+(0.5 if y>=0 else -0.5), f"{y:+.2f}%", ha="center",
             va="bottom" if y>=0 else "top", fontsize=8)
img_bar = fig_to_png_base64(fig_bar)

# BTC & ETH 7일 추세
fig_line = plt.figure(figsize=(10,3))
plt.plot(btc_ret, label="BTC"); plt.plot(eth_ret, label="ETH")
plt.legend(); plt.title("BTC & ETH 7일 가격 추세"); plt.ylabel("% (from start)")
img_line = fig_to_png_base64(fig_line)

# TOP3
top3_up = disp.head(3)[["symbol","pct"]]
top3_dn = disp.tail(3)[["symbol","pct"]]

# ===== 조선비즈 톤 뉴스(제목+본문) =====
YMD = TODAY

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

def fmt_pct(v, n=2):
    if v is None: return "-"
    try: return f"{float(v):+.{n}f}%"
    except: return "-"

def fp(v):
    if v is None: return "-"
    try: return f"{float(v):+.3f}%"
    except: return "-"

bm20_chg = bm20_change_pct; bm20_now = bm20_index
num_up = up_count; num_down = down_count
kimchi_pct = k_prem
btc_row = df.loc[df["symbol"]=="BTC"].head(1)
eth_row = df.loc[df["symbol"]=="ETH"].head(1)

def build_news_v2():
    trend = "상승" if bm20_chg>0 else ("하락" if bm20_chg<0 else "보합")
    breadth_word = "강세 우위" if num_up>num_down else ("약세 우위" if num_down>num_up else "중립")
    breadth = f"상승 {num_up}·하락 {num_down}"

    ups = [f"{r['symbol']}({pct(r['pct'])})" for _, r in top3_up.iterrows()]
    dns = [f"{r['symbol']}({pct(r['pct'])})" for _, r in top3_dn.iterrows()]

    # BTC/ETH 한 줄
    btc_line = ""
    if len(btc_row):
        br = btc_row.iloc[0]
        btc_line = f"비트코인(BTC)은 {pct(br['pct'])} "
        btc_line += ("하락" if br["pct"]<0 else ("상승" if br["pct"]>0 else "보합"))
        btc_line += f"해 {num(br['price'])}달러선."
    eth_line = ""
    if len(eth_row):
        er = eth_row.iloc[0]
        eth_line = f"이더리움(ETH)은 {pct(er['pct'])} "
        eth_line += ("하락" if er["pct"]<0 else ("상승" if er["pct"]>0 else "보합"))
        eth_line += f"해 {num(er['price'])}달러에 거래됐다."

    # 김치 프리미엄/펀딩비
    if kimchi_pct is None:
        kp_line = "김치 프리미엄은 보합권."
    else:
        kp_side = "할인" if kimchi_pct<0 else "할증"
        kp_line = f"김치 프리미엄 {fmt_pct(kimchi_pct,2)}({kp_side})."
    fund_line = f"바이낸스 펀딩비는 BTC {fp(fund_btc)}, ETH {fp(fund_eth)}."

    # 제목(간결·사실), 본문(숏문장 위주)
    title = f"BM20 {pct_abs(bm20_chg)} {'상승' if bm20_chg>0 else ('하락' if bm20_chg<0 else '보합')}… 지수 {num(bm20_now)}pt"
    if kimchi_pct is not None:
        title += f", 김프 {fmt_pct(kimchi_pct,2)}"
    body = " ".join([
        f"{YMD} BM20 지수 {pct(bm20_chg)} {trend}.",
        f"시장 폭은 {breadth}({breadth_word}).",
        (f"하락 상위 {', '.join(dns)}." if num_down>=num_up else f"상승 상위 {', '.join(ups)}."),
        (f"반면 상승 상위 {', '.join(ups)}." if num_down>=num_up else f"반면 하락 상위 {', '.join(dns)}."),
        btc_line, eth_line, kp_line, fund_line
    ])
    return title, body

news_title, news_body = build_news_v2()

# 선택형 외부 본문(news/today.md)
news_html_extra = ""
try:
    if os.path.exists("news/today.md"):
        with open("news/today.md","r",encoding="utf-8") as f:
            md = f.read().strip()
        news_html_extra = "<p>" + md.replace("\n\n","</p><p>").replace("\n","<br>") + "</p>"
except Exception:
    pass

# ===== HTML =====
def esc(s): return str(s).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
def metric_row(k, v): return f"<tr><td class='k'>{esc(k)}</td><td class='v'>{esc(v)}</td></tr>"

metrics_html = "".join([
    metric_row("지수", f"{bm20_index:,.2f} pt"),
    metric_row("일간 변동", f"{bm20_change_pct:+.2f}%"),
    metric_row("상승/하락", f"{up_count} / {down_count}"),
    metric_row("김치 프리미엄", f"{fmt_pct(k_prem)}"),
    metric_row("펀딩비(Binance)", f"BTC {fp(fund_btc)} / ETH {fp(fund_eth)}"),
])

rows_weight = "\n".join(
    f"<tr><td>{esc(r.symbol)}</td><td style='text-align:right'>{r.weight*100:,.2f}%</td><td style='text-align:right'>{r.pct:+.2f}%</td></tr>"
    for r in bm20.itertuples()
)

vol_rows = "\n".join(
    f"<tr><td>{sym}</td><td style='text-align:right'>+{ratio*100:,.2f}%</td></tr>"
    for sym, ratio in vol_top3
)

top_up_rows = "\n".join(
    f"<tr><td>{esc(sym)}</td><td style='text-align:right'>+{pct:.2f}%</td></tr>"
    for sym, pct in zip(top3_up["symbol"], top3_up["pct"])
)
top_dn_rows = "\n".join(
    f"<tr><td>{esc(sym)}</td><td style='text-align:right'>{pct:.2f}%</td></tr>"
    for sym, pct in zip(top3_dn["symbol"], top3_dn["pct"])
)

HTML = f"""<!doctype html><html lang="ko"><head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>BM20 데일리 리포트 — {TODAY}</title>
<style>
body{{font-family:-apple-system,BlinkMacSystemFont,'Malgun Gothic',Arial,sans-serif;background:#f7f9fc;color:#111;margin:0}}
.wrap{{max-width:960px;margin:0 auto;padding:16px}}
.card{{background:#fff;border:1px solid #dde3ee;border-radius:10px;padding:14px;margin:12px 0;box-shadow:0 1px 0 #eef2f7}}
h1{{font-size:26px;margin:0 0 4px 0;text-align:center}} .muted{{text-align:center;color:#667}} h2{{font-size:16px;color:#143c9a;margin:10px 0}}
table.meta{{width:100%;border-collapse:collapse}} table.meta td{{border:1px solid #e5eaf3;padding:8px}}
table.meta td.k{{width:160px;background:#f1f6ff}}
table.std{{width:100%;border-collapse:collapse}} table.std th, table.std td{{border:1px solid #e5eaf3;padding:8px}} table.std th{{background:#eef4ff}}
.grid{{display:grid;grid-template-columns:1fr 1fr;gap:12px}}
.center{{text-align:center}}
h3{{font-size:18px;margin:6px 0}}
</style></head><body><div class="wrap">

<div class="card">
  <h1>BM20 데일리 리포트</h1>
  <div class="muted">{TODAY}</div>
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
  <p class="center"><img alt="Perf" src="data:image/png;base64,{img_bar}"></p>
</div>

<div class="grid">
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
  <p class="center"><img alt="BTC&ETH" src="data:image/png;base64,{img_line}"></p>
</div>

<div class="card">
  <h2>구성 & 가중치</h2>
  <table class="std">
    <tr><th>심볼</th><th>가중치</th><th>1D 등락</th></tr>
    {rows_weight}
  </table>
</div>

<div class="muted" style="font-size:12px;margin:8px 0;text-align:center">© Blockmedia · Data: CoinGecko; Funding: Binance</div>
</div></body></html>
"""

# ===== 저장 (최신본 + 아카이브 + 아카이브 인덱스) =====
arc_path = f"{ARCHIVE_DIR}/bm20_daily_{TODAY}.html"
with open(arc_path, "w", encoding="utf-8") as f: f.write(HTML)
with open("index.html", "w", encoding="utf-8") as f: f.write(HTML)

# Jekyll 처리/캐시 이슈 방지(페이지 배포용)
with open(".nojekyll", "w", encoding="utf-8") as f:
    f.write("")

items = sorted([p for p in os.listdir(ARCHIVE_DIR) if p.startswith("bm20_daily_") and p.endswith(".html")])
links = "\n".join([f'<li><a href="{p}">{p.replace("bm20_daily_","").replace(".html","")}</a></li>' for p in items])
arc_index = f"""<!doctype html><meta charset="UTF-8"><title>BM20 Archive</title>
<h1>BM20 데일리 아카이브</h1><ul>{links}</ul>"""
with open(f"{ARCHIVE_DIR}/index.html", "w", encoding="utf-8") as f: f.write(arc_index)

print("Saved:", arc_path, "index.html", "archive/index.html")

