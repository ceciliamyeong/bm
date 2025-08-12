# scripts/generate_report.py
# BM20 데일리 자동 생성 (최신본 index.html + 아카이브 /archive/날짜.html)
# - CoinGecko 공개 API 사용
# - 429(Too Many Requests) 대비: 재시도/백오프/슬립
# - 국내상장 가중치: 정적 심볼 리스트로 우선 판정(빠름) + 필요 시 API 확인(안정)
# - 이미지 Base64 포함 단일 HTML 출력

import requests, datetime, base64, io, os, time, random, sys
import pandas as pd
import matplotlib.pyplot as plt

# ===== 기본 설정 =====
KST = datetime.timezone(datetime.timedelta(hours=9))
TODAY = datetime.datetime.now(KST).strftime("%Y-%m-%d")
ARCHIVE_DIR = "archive"
os.makedirs(ARCHIVE_DIR, exist_ok=True)

# ===== Rate Limit 대응 =====
RATE_SLEEP = float(os.getenv("BM20_RATE_SLEEP", "1.2"))  # 호출 간 대기(초)
MAX_RETRY  = int(os.getenv("BM20_MAX_RETRY", "5"))
CG_API_KEY = os.getenv("CG_API_KEY")  # 있으면 레이트리밋 완화(Pro)
DEFAULT_HEADERS = {"User-Agent": "bm20-bot/1.0"}
if CG_API_KEY:
    DEFAULT_HEADERS["x-cg-pro-api-key"] = CG_API_KEY

def cg(path, **params):
    """CoinGecko 호출 (재시도/백오프)"""
    url = f"https://api.coingecko.com/api/v3/{path}"
    for i in range(MAX_RETRY):
        r = requests.get(url, params=params, timeout=30, headers=DEFAULT_HEADERS)
        if r.status_code == 200:
            time.sleep(RATE_SLEEP)
            return r.json()
        if r.status_code == 429:
            wait = RATE_SLEEP * (2 ** i) + random.uniform(0, 0.6)
            time.sleep(wait)
            continue
        # 기타 오류도 한 번씩 쉰 뒤 재시도
        time.sleep(RATE_SLEEP)
    # 마지막 실패
    r.raise_for_status()

# ===== BM20 구성 규칙 =====
STABLE_TICKERS = {"USDT","USDC","FDUSD","BUSD","TUSD","DAI","USDP","UST","EURS","PYUSD"}
KOREAN_EXCHANGES = ["Upbit","Bithumb","Coinone"]

# 국내 3대 거래소 상장 여부: 우선 정적 심볼로 빠르게 True 판단
KR_LISTED_SYMBOLS = {
    "BTC","ETH","XRP","ADA","SOL","DOGE","TRX","DOT","LINK","MATIC",
    "LTC","ATOM","BCH","APT","ARB","AVAX","SUI","NEAR","ICP","FIL",
    "OP","PEPE","ETC","HBAR","STX","ALGO","IMX","INJ","TIA","RNDR"
}

def listed_on_korea(coin_id, symbol):
    if symbol.upper() in KR_LISTED_SYMBOLS:
        return True
    # 필요할 때만 API 확인 (429 방지)
    try:
        j = cg(f"coins/{coin_id}/tickers", include_exchange_logo=False)
        for t in j.get("tickers", []):
            market = (t.get("market") or {}).get("name","")
            if any(ex in market for ex in KOREAN_EXCHANGES):
                return True
    except Exception:
        return False
    return False

# ===== 상위 코인 가져오기 (스테이블 제외) =====
markets = cg("coins/markets",
             vs_currency="usd",
             order="market_cap_desc",
             per_page=40, page=1,
             price_change_percentage="24h")

m = pd.DataFrame([{
    "id": c["id"],
    "symbol": c["symbol"].upper(),
    "name": c["name"],
    "mcap": c["market_cap"] or 0,
    "pct24h": c["price_change_percentage_24h"] or 0.0
} for c in markets])

m = m[~m["symbol"].isin(STABLE_TICKERS)].reset_index(drop=True)

# 후보 20개만 사용(호출 최소화)
cands = m.head(20).copy()
cands["kr_listed"] = [listed_on_korea(cid, sym) for cid, sym in zip(cands["id"], cands["symbol"])]

# 국내상장 가중치 1.3x
cands["weight_base"] = cands["mcap"] * cands["kr_listed"].apply(lambda x: 1.3 if x else 1.0)
total = cands["weight_base"].sum()
cands["weight"] = cands["weight_base"] / total

bm20 = cands.copy()

# BM20 일간 수익률(가중합)
bm20["ret"] = bm20["pct24h"] / 100.0
bm20_return = (bm20["weight"] * bm20["ret"]).sum()
bm20_change_pct = bm20_return * 100.0
bm20_index = 100.0 * (1.0 + bm20_return)  # 전일=100 가정 지수 레벨

# ===== 차트용 데이터 =====
def hist7d_ret(coin_id):
    j = cg(f"coins/{coin_id}/market_chart", vs_currency="usd", days=7)
    p = pd.DataFrame(j["prices"], columns=["ts","price"])
    p["price"] = p["price"].astype(float)
    p["ret"] = (p["price"]/p["price"].iloc[0]-1)*100.0
    return p["ret"]

btc_ret = hist7d_ret("bitcoin")
eth_ret = hist7d_ret("ethereum")

def fig_to_png_base64(fig):
    buf = io.BytesIO()
    fig.tight_layout()
    fig.savefig(buf, format="png")
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode()

# 차트 1: BTC/ETH 7일 추세
fig1 = plt.figure(figsize=(10,3))
plt.plot(btc_ret, label="BTC")
plt.plot(eth_ret, label="ETH")
plt.legend(); plt.title("BTC & ETH 7일 가격 추세"); plt.ylabel("% (from start)")
img_trend = fig_to_png_base64(fig1)

# 차트 2: BM20 구성 코인 1D 퍼포먼스
disp = bm20.sort_values("pct24h", ascending=False)
fig2 = plt.figure(figsize=(10,3))
plt.bar(disp["symbol"], disp["pct24h"])
plt.title("BM20 구성 코인 퍼포먼스 (1D, USD)"); plt.ylabel("%")
for x, y in enumerate(disp["pct24h"]):
    plt.text(x, y + (0.5 if y>=0 else -0.5), f"{y:+.2f}%", ha="center",
             va="bottom" if y>=0 else "top", fontsize=8)
img_bar = fig_to_png_base64(fig2)

# ===== HTML 생성 =====
def esc(s): return str(s).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

rows = "\n".join(
    f"<tr><td>{esc(r.symbol)}</td>"
    f"<td style='text-align:right'>{'예' if r.kr_listed else '아니오'}</td>"
    f"<td style='text-align:right'>{r.weight*100:,.2f}%</td>"
    f"<td style='text-align:right'>{r.pct24h:+.2f}%</td></tr>"
    for r in bm20.itertuples()
)

HEADLINE = f"BM20 {TODAY} — 지수 {bm20_index:,.2f} (일간 {bm20_change_pct:+.2f}%) · 국내상장 1.3× 가중치"

HTML = f"""<!doctype html><html lang="ko"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>BM20 데일리 {TODAY}</title>
<style>
body{{font-family:-apple-system,BlinkMacSystemFont,'Malgun Gothic',Arial,sans-serif;background:#fafbfc;color:#111;margin:0}}
.wrap{{max-width:860px;margin:0 auto;padding:20px}}
.card{{background:#fff;border:1px solid #e5e9f0;border-radius:12px;padding:20px;margin-bottom:16px}}
h1{{font-size:22px;margin:0 0 8px 0;text-align:center}} h2{{font-size:15px;margin:16px 0 8px 0;color:#1A237E}}
.muted{{color:#555;text-align:center}} .center{{text-align:center}}
table{{width:100%;border-collapse:collapse;font-size:14px}} th,td{{border:1px solid #e5e9f0;padding:8px}} th{{background:#eef4ff}}
.footer{{font-size:12px;color:#666;text-align:center;margin-top:16px}}
.badge{{display:inline-block;background:#eef4ff;border:1px solid #d6e0ff;padding:4px 8px;border-radius:8px;margin:2px}}
</style></head><body><div class="wrap">

<div class="card">
  <h1>BM20 데일리 리포트</h1>
  <div class="muted">{TODAY}</div>
  <p class="center">
    <span class="badge">지수 {bm20_index:,.2f}</span>
    <span class="badge">일간 {bm20_change_pct:+.2f}%</span>
    <span class="badge">구성: 상위 20 (스테이블 제외)</span>
    <span class="badge">국내상장 1.3× 가중치</span>
  </p>
</div>

<div class="card">
  <h2>BM20 구성 코인 & 가중치</h2>
  <table>
    <tr><th>심볼</th><th>국내 상장</th><th>가중치</th><th>1D 등락</th></tr>
    {rows}
  </table>
</div>

<div class="card">
  <h2>BM20 구성 코인 퍼포먼스 (1D)</h2>
  <p class="center"><img alt="BM20 Bar" src="data:image/png;base64,{img_bar}"></p>
</div>

<div class="card">
  <h2>BTC & ETH 7일 가격 추세</h2>
  <p class="center"><img alt="Trend" src="data:image/png;base64,{img_trend}"></p>
</div>

<div class="card">
  <h2>데일리 뉴스</h2>
  <p>{esc(HEADLINE)}</p>
  <p class="center"><a href="archive/">아카이브 보기</a></p>
</div>

<div class="footer">© Blockmedia · Source: CoinGecko public API</div>
</div></body></html>
"""

# ===== 저장 (아카이브 + 최신본 + 아카이브 인덱스) =====
arc_path = f"{ARCHIVE_DIR}/bm20_daily_{TODAY}.html"
with open(arc_path, "w", encoding="utf-8") as f: f.write(HTML)
with open("index.html", "w", encoding="utf-8") as f: f.write(HTML)

items = sorted([p for p in os.listdir(ARCHIVE_DIR) if p.startswith("bm20_daily_") and p.endswith(".html")])
links = "\n".join([f'<li><a href="{p}">{p.replace("bm20_daily_","").replace(".html","")}</a></li>' for p in items])
archive_index = f"""<!doctype html><meta charset="utf-8"><title>BM20 Archive</title>
<h1>BM20 데일리 아카이브</h1><ul>{links}</ul>"""
with open(f"{ARCHIVE_DIR}/index.html", "w", encoding="utf-8") as f: f.write(archive_index)

print("Saved:", arc_path, "index.html", "archive/index.html")
