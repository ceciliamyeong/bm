import requests, datetime, base64, io, os
import pandas as pd
import matplotlib.pyplot as plt

# ----- Timezone & paths -----
TZ = datetime.timezone(datetime.timedelta(hours=9))  # KST
today = datetime.datetime.now(TZ).strftime("%Y-%m-%d")
ARCHIVE_DIR = "archive"
os.makedirs(ARCHIVE_DIR, exist_ok=True)

# ----- Helpers -----
def cg(path, **params):
    url = f"https://api.coingecko.com/api/v3/{path}"
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def fig_to_base64(fig):
    buf = io.BytesIO()
    fig.tight_layout()
    fig.savefig(buf, format="png")
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode()

# ----- Data (sample using CoinGecko public API) -----
market = cg("coins/markets", vs_currency="usd", order="market_cap_desc",
            per_page=20, page=1, price_change_percentage="24h")
df = pd.DataFrame([{
    "symbol": c["symbol"].upper(),
    "name": c["name"],
    "pct": c["price_change_percentage_24h"]
} for c in market]).dropna()

def hist7d(coin):
    j = cg(f"coins/{coin}/market_chart", vs_currency="usd", days=7)
    p = pd.DataFrame(j["prices"], columns=["ts","price"])
    p["price"] = p["price"].astype(float)
    p["ret"] = (p["price"]/p["price"].iloc[0]-1)*100
    return p

btc = hist7d("bitcoin")
eth = hist7d("ethereum")

# ----- Charts -----
# Chart 1: BTC & ETH 7-day trend (% from start)
fig1 = plt.figure(figsize=(10,3))
plt.plot(btc["ret"], label="BTC")
plt.plot(eth["ret"], label="ETH")
plt.legend()
plt.title("BTC & ETH 7일 가격 추세")
plt.ylabel("% (from start)")
img_trend = fig_to_base64(fig1)

# Chart 2: 1D performance bar chart for top 20 by mktcap
df_sorted = df.sort_values("pct", ascending=False)
fig2 = plt.figure(figsize=(10,3))
plt.bar(df_sorted["symbol"], df_sorted["pct"])
plt.title("코인별 퍼포먼스 (1D, USD)")
plt.ylabel("%")
for x, y in zip(range(len(df_sorted)), df_sorted["pct"]):
    plt.text(x, y + (0.5 if y>=0 else -0.5), f"{y:+.2f}%", ha="center",
             va="bottom" if y>=0 else "top", fontsize=8)
img_bar = fig_to_base64(fig2)

# ----- Simple headline -----
headline = f"BM20 데일리 {today} — 상승 {(df['pct']>0).sum()}개 / 하락 {(df['pct']<=0).sum()}개"

# ----- HTML template (single-file, images embedded) -----
html = '''<!doctype html><html lang="ko"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>BM20 데일리 {today}</title>
<style>
body{{font-family:-apple-system,BlinkMacSystemFont,'Malgun Gothic',Arial,sans-serif;background:#fafbfc;color:#111;margin:0}}
.wrap{{max-width:760px;margin:0 auto;padding:20px}}
.card{{background:#fff;border:1px solid #e5e9f0;border-radius:12px;padding:20px;margin-bottom:16px}}
h1{{font-size:22px;margin:0 0 8px 0;text-align:center}} h2{{font-size:15px;margin:16px 0 8px 0;color:#1A237E}}
.muted{{color:#555;text-align:center}} .center{{text-align:center}} .footer{{font-size:12px;color:#666;text-align:center;margin-top:16px}}
table{{width:100%;border-collapse:collapse;font-size:14px}} th,td{{border:1px solid #e5e9f0;padding:8px}} th{{background:#eef4ff}}
a{{color:#1A237E;text-decoration:none}}
</style></head><body><div class="wrap">
<div class="card"><h1>BM20 데일리 리포트</h1><div class="muted">{today}</div></div>
<div class="card"><h2>코인별 퍼포먼스 (1D, USD)</h2>
<p class="center"><img alt="Performance" src="data:image/png;base64,{img_bar}"></p></div>
<div class="card"><h2>BTC & ETH 7일 가격 추세</h2>
<p class="center"><img alt="Trend" src="data:image/png;base64,{img_trend}"></p></div>
<div class="card"><h2>데일리 뉴스</h2><p>{headline}</p>
<p class="center"><a href="archive/">아카이브 보기</a></p></div>
<div class="footer">© Blockmedia · Data: CoinGecko</div></div></body></html>'''

html = html.format(today=today, img_bar=img_bar, img_trend=img_trend, headline=headline)

# ----- Save: archive + latest -----
arc_path = f"{ARCHIVE_DIR}/bm20_daily_{today}.html"
with open(arc_path, "w", encoding="utf-8") as f:
    f.write(html)
with open("index.html", "w", encoding="utf-8") as f:
    f.write(html)

# ----- Archive index (relative links) -----
items = sorted([p for p in os.listdir(ARCHIVE_DIR)
                if p.startswith("bm20_daily_") and p.endswith(".html")])
links = "\n".join([f'<li><a href="{p}">{p.replace("bm20_daily_","").replace(".html","")}</a></li>' for p in items])
archive_index = f'''<!doctype html><meta charset="utf-8">
<title>BM20 Archive</title>
<h1>BM20 데일리 아카이브</h1>
<ul>{links}</ul>'''
with open(f"{ARCHIVE_DIR}/index.html", "w", encoding="utf-8") as f:
    f.write(archive_index)

print("Saved:", arc_path, "and index.html, archive/index.html")
