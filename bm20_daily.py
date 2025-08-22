# ===================== BM20 Daily — 0821 FINAL (KST, Rebase 2018-01-01=100) =====================
# 기능: 유니버스/가중치(확정) → 시세 수집 → 전일/금일 포트폴리오 가치 → 리베이스 지수 산출
#      김치 프리미엄 · 펀딩비 → Best/Worst → 뉴스 해석형 문장 → 차트(가로막대/7D) → JSON/TXT/CSV/PDF 저장
# 산출물: out/YYYY-MM-DD/  (csv/txt/png/pdf),  site/bm20_latest.json
# 주의: CoinGecko 호출이 실패하면 폴백 경로를 사용합니다. (Upbit/Binance/Yahoo/Bybit 등)

import os, json, time, random, math
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Optional

import requests
import pandas as pd

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

try:
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas
    from reportlab.lib.units import cm
    REPORTLAB_AVAILABLE = True
except Exception:
    REPORTLAB_AVAILABLE = False

# ------------------------- Paths / Dates -------------------------
OUT_DIR = os.getenv("OUT_DIR", "out")
os.makedirs(OUT_DIR, exist_ok=True)
KST = timezone(timedelta(hours=9))
TODAY = datetime.now(KST)
YMD = TODAY.strftime("%Y-%m-%d")
OUT_DIR_DATE = os.path.join(OUT_DIR, YMD)
os.makedirs(OUT_DIR_DATE, exist_ok=True)

SITE_DIR = "site"
os.makedirs(SITE_DIR, exist_ok=True)
BASE_CACHE_PATH = os.path.join(OUT_DIR, "base_cache.json")  # 2018-01-01 기준 포트폴리오 가치 캐시

# ------------------------- Constants ----------------------------
CG = "https://api.coingecko.com/api/v3"
REQ_UA = {"User-Agent":"Mozilla/5.0"}
REQ_TIMEOUT = 15

# BM20 Universe (20) — 0821 확정
UNIVERSE: List[str] = [
    "BTC","ETH","XRP","USDT","BNB",
    "DOGE","TON","SUI","SOL","ADA","AVAX","DOT","MATIC","LINK","LTC","ATOM","NEAR","APT","FIL","ICP"
]
# CoinGecko id 매핑
CG_ID: Dict[str,str] = {
    "BTC":"bitcoin","ETH":"ethereum","XRP":"ripple","USDT":"tether","BNB":"binancecoin",
    "DOGE":"dogecoin","TON":"toncoin","SUI":"sui","SOL":"solana","ADA":"cardano","AVAX":"avalanche-2",
    "DOT":"polkadot","MATIC":"polygon","LINK":"chainlink","LTC":"litecoin","ATOM":"cosmos",
    "NEAR":"near","APT":"aptos","FIL":"filecoin","ICP":"internet-computer"
}

# 고정 가중치 (BTC 30 / ETH 20 / XRP·USDT·BNB 각 5 / 나머지 15종 = 35% 균등)
W: Dict[str,float] = {"BTC":0.30, "ETH":0.20, "XRP":0.05, "USDT":0.05, "BNB":0.05}
OTHERS = [s for s in UNIVERSE if s not in W]
eq = 0.35 / len(OTHERS)
for s in OTHERS:
    W[s] = round(eq, 6)

BASE_DATE_STR = "2018-01-01"  # 지수 기준일

# ------------------------- HTTP Utils ---------------------------
def _get(url: str, params: dict=None, timeout: int=REQ_TIMEOUT, headers: dict=REQ_UA, retry: int=3, sleep: float=0.6):
    last = None
    for i in range(retry):
        try:
            r = requests.get(url, params=params, timeout=timeout, headers=headers)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            time.sleep(sleep*(i+1) + 0.2*random.random())
    raise last

def _safe_float(x, d: float=0.0) -> float:
    try:
        return float(x)
    except Exception:
        return d

# ------------------------- Market Snapshot ----------------------
def fetch_markets(symbols: List[str]) -> pd.DataFrame:
    ids = ",".join([CG_ID[s] for s in symbols])
    j = _get(f"{CG}/coins/markets", params={
        "vs_currency":"usd","ids":ids,
        "order":"market_cap_desc","per_page":len(symbols),"page":1,
        "price_change_percentage":"24h"
    })
    rows = []
    for m in j:
        # id → 심볼 역매핑 보정
        sym = None
        for k,v in CG_ID.items():
            if v == m.get("id"):
                sym = k; break
        if not sym:
            sym = m.get("symbol","?").upper()
        rows.append({
            "symbol": sym,
            "current_price": _safe_float(m.get("current_price")),
            "market_cap": _safe_float(m.get("market_cap")),
            "chg24": _safe_float(m.get("price_change_percentage_24h")),
        })
    df = pd.DataFrame(rows).set_index("symbol").reindex(symbols)
    return df


def fetch_yday_close(symbol: str) -> Optional[float]:
    cid = CG_ID[symbol]
    j = _get(f"{CG}/coins/{cid}/market_chart", params={"vs_currency":"usd","days":2})
    prices = j.get("prices", [])
    if not prices:
        return None
    yday = (datetime.now(KST) - timedelta(days=1)).date()
    series = [(datetime.fromtimestamp(p[0]/1000, timezone.utc), p[1]) for p in prices]
    yvals = [p for (t,p) in series if t.astimezone(KST).date()==yday]
    if yvals:
        return float(yvals[-1])
    return float(series[-2][1]) if len(series)>=2 else float(series[-1][1])


def fill_previous_prices(df: pd.DataFrame) -> pd.DataFrame:
    prevs = []
    for s in df.index:
        v = None
        try:
            v = fetch_yday_close(s)
        except Exception:
            v = None
        if v in (None, 0):
            cur = df.at[s, "current_price"]
            ch = (df.at[s, "chg24"] or 0.0)/100.0
            v = cur/(1+ch) if (cur and not math.isclose(ch, -1.0)) else cur
        prevs.append(v)
        time.sleep(0.12)
    df["previous_price"] = prevs
    return df

# ------------------------- Rebase (2018-01-01=100) --------------

def fetch_price_on_2018(symbol: str) -> Optional[float]:
    # CoinGecko history는 dd-mm-yyyy 포맷 요구
    cid = CG_ID[symbol]
    j = _get(f"{CG}/coins/{cid}/history", params={"date":"01-01-2018","localization":"false"})
    try:
        return float(j["market_data"]["current_price"]["usd"])
    except Exception:
        return None


def get_base_value() -> float:
    # 캐시 우선
    if os.path.exists(BASE_CACHE_PATH):
        try:
            with open(BASE_CACHE_PATH, "r", encoding="utf-8") as f:
                c = json.load(f)
            if c.get("base_date") == BASE_DATE_STR and set(c.get("universe",[])) == set(UNIVERSE):
                return float(c["portfolio_value_usd"])
        except Exception:
            pass
    base_val = 0.0
    for s in UNIVERSE:
        p = fetch_price_on_2018(s)
        if p is None:
            continue
        base_val += p * W[s]
        time.sleep(0.12)
    with open(BASE_CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump({
            "base_date": BASE_DATE_STR,
            "universe": UNIVERSE,
            "weights": W,
            "portfolio_value_usd": round(base_val, 10)
        }, f, ensure_ascii=False)
    return base_val

# ------------------------- Kimchi Premium -----------------------

def _fetch_usdkrw_yahoo() -> Optional[float]:
    try:
        j = _get("https://query1.finance.yahoo.com/v7/finance/quote", params={"symbols":"USDKRW=X"})
        q = j["quoteResponse"]["result"][0]
        return float(q["regularMarketPrice"])
    except Exception:
        return None


def _fetch_btc_usd_binance() -> Optional[float]:
    try:
        j = _get("https://api.binance.com/api/v3/ticker/price", params={"symbol":"BTCUSDT"})
        return float(j["price"])
    except Exception:
        return None


def _fetch_btc_krw_upbit() -> Optional[float]:
    try:
        j = _get("https://api.upbit.com/v1/ticker", params={"markets":"KRW-BTC"})
        return float(j[0]["trade_price"])
    except Exception:
        return None


def compute_kimchi_premium(df: pd.DataFrame) -> Tuple[str, Dict]:
    btc_usd = _fetch_btc_usd_binance()
    btc_krw = _fetch_btc_krw_upbit()
    usdkrw = _fetch_usdkrw_yahoo()

    # 폴백: CG simple price
    if btc_usd is None or btc_krw is None or usdkrw is None:
        try:
            headers = {}
            if os.getenv("COINGECKO_API_KEY"):
                headers["x-cg-pro-api-key"] = os.getenv("COINGECKO_API_KEY")
            j = _get(f"{CG}/simple/price", params={"ids":"bitcoin,tether","vs_currencies":"usd,krw"}, headers=headers)
            btc_usd = btc_usd if btc_usd is not None else float(j["bitcoin"]["usd"])
            # KRW는 업비트가 더 적합하지만 실패 시 CG 사용
            btc_krw = btc_krw if btc_krw is not None else float(j["bitcoin"].get("krw", 0)) or None
            if usdkrw is None:
                # tether→KRW를 proxy FX로 사용
                usdkrw = float(j["tether"]["krw"]) if (900 <= float(j["tether"]["krw"]) <= 2000) else None
        except Exception:
            pass

    # 마지막 시도: df에서 btc usd 가격 재사용
    if btc_usd is None:
        try:
            btc_usd = float(df.loc["BTC","current_price"]) if not pd.isna(df.loc["BTC","current_price"]) else None
        except Exception:
            btc_usd = None

    # 환율이 끝까지 없으면 1350 고정
    if usdkrw is None:
        usdkrw = 1350.0

    if not all([btc_usd, btc_krw, usdkrw]) or usdkrw <= 0:
        return "—", {"btc_usd": btc_usd, "btc_krw": btc_krw, "usdkrw": usdkrw, "note":"incomplete"}

    prem = (btc_krw / (btc_usd * usdkrw)) - 1.0
    return f"{prem*100:+.2f}%", {"btc_usd": round(btc_usd,2), "btc_krw": round(btc_krw,0), "usdkrw": round(usdkrw,2)}

# ------------------------- Funding Rates ------------------------

def _funding_binance(symbol: str) -> Optional[float]:
    try:
        j = _get("https://fapi.binance.com/fapi/v1/fundingRate", params={"symbol":symbol, "limit":1})
        if j and isinstance(j, list) and "fundingRate" in j[0]:
            return float(j[0]["fundingRate"]) * 100
    except Exception:
        return None
    return None


def _funding_bybit(symbol: str) -> Optional[float]:
    try:
        j = _get("https://api.bybit.com/v5/market/funding/history", params={"category":"linear","symbol":symbol,"limit":1})
        if j.get("retCode") == 0 and j.get("result",{}).get("list"):
            return float(j["result"]["list"][0]["fundingRate"]) * 100
    except Exception:
        return None
    return None


def get_funding_display_pair() -> Tuple[str,str]:
    def fmt(x: Optional[float]) -> str:
        if x is None:
            return "중립권"
        return f"{x:+.4f}%"
    btc = _funding_binance("BTCUSDT")
    eth = _funding_binance("ETHUSDT")
    if btc is None:
        btc = _funding_bybit("BTCUSDT")
    if eth is None:
        eth = _funding_bybit("ETHUSDT")
    return fmt(btc), fmt(eth)

# ------------------------- Charts (Dark) ------------------------

def _set_dark_style():
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


def plot_perf_barh(names: List[str], rets_pct: List[float], out_png: str):
    _set_dark_style()
    import numpy as np
    y = np.arange(len(names))
    vals = np.array(rets_pct, dtype=float)
    colors = np.where(vals>=0, "#2E7D32", "#C62828")

    fig, ax = plt.subplots(figsize=(10, 6), dpi=150)
    ax.barh(y, vals, color=colors, alpha=0.95)
    ax.axvline(0, color="#3a4569", lw=1)
    ax.set_yticks(y, labels=names)
    ax.set_xlabel("Daily Change (%)")
    ax.invert_yaxis()  # TOP이 위로
    # 라벨
    for yi, v in zip(y, vals):
        s = f"{v:+.2f}%"
        ax.text(v + (0.05 if v>=0 else -0.05), yi, s, va="center", ha=("left" if v>=0 else "right"))
    ax.set_title(f"BM20 Daily Performance  ({YMD})")
    fig.savefig(out_png, dpi=180)
    plt.close(fig)


def plot_btc_eth_trend_7d(out_png: str):
    _set_dark_style()
    # CoinGecko: 7d market_chart
    def mc(id):
        j = _get(f"{CG}/coins/{id}/market_chart", params={"vs_currency":"usd","days":7})
        return [(datetime.fromtimestamp(p[0]/1000, timezone.utc).astimezone(KST), p[1]) for p in j.get("prices", [])]
    b = mc("bitcoin")
    e = mc("ethereum")
    fig, ax = plt.subplots(figsize=(10,5), dpi=150)
    ax.plot([t for t,_ in b], [v for _,v in b], label="BTC")
    ax.plot([t for t,_ in e], [v for _,v in e], label="ETH")
    ax.set_title("BTC · ETH — 7D Trend (USD)")
    ax.set_xlabel("Date (KST)")
    ax.set_ylabel("Price (USD)")
    ax.legend()
    fig.autofmt_xdate()
    fig.savefig(out_png, dpi=180)
    plt.close(fig)

# ------------------------- News Helpers -------------------------

def _word_breadth(up: int, down: int) -> str:
    if up >= down + 3: return "상승 우위"
    if down >= up + 3: return "하락 우위"
    return "혼조"


def _word_funding(btc: str, eth: str) -> str:
    def neutral(x: str) -> bool:
        try:
            return abs(float(str(x).replace('%',''))) < 0.005
        except Exception:
            return True
    return "중립권" if neutral(btc) and neutral(eth) else "약한 편향"


def build_news_long(d: Dict) -> str:
    breadth_word = _word_breadth(d.get("upCount",0), d.get("downCount",0))
    funding = d.get("funding", {}) or {}
    funding_btc = funding.get("btc","중립권"); funding_eth = funding.get("eth","중립권")
    funding_word = _word_funding(funding_btc, funding_eth)

    kimchi = d.get("kimchi","—")
    try:
        kv = float(str(kimchi).replace('%',''))
        kimchi_word = "국내 할인" if kv < 0 else ("국내 할증" if kv > 0 else "중립")
    except Exception:
        kimchi_word = "중립"

    best = d.get("best3", [])[:3] + [["—",0]]*3
    worst = d.get("worst3", [])[:3] + [["—",0]]*3
    best1, best2, best3 = best[0], best[1], best[2]
    worst1, worst2, worst3 = worst[0], worst[1], worst[2]

    majors_word = "방향성 확인"
    btc_ch = d.get("btcChangePct")
    eth_ch = d.get("ethChangePct")
    if isinstance(btc_ch,(int,float)) and isinstance(eth_ch,(int,float)):
        majors_word = "변동성 확대" if (abs(btc_ch) >= 0.2 or abs(eth_ch) >= 0.2) else "방향성 확인"

    text = (
      f"BM20 지수는 {d.get('asOf','')} 전일 대비 {d.get('bm20ChangePct',0):+.2f}% 변동해 "
      f"{d.get('bm20Level',0):,.0f}pt를 기록했습니다. 2018년 1월 1일(=100) 대비 "
      f"{d.get('rebasedMultiple',0):.1f}배 수준입니다. 구성 종목 {d.get('total',20)}개 중 "
      f"상승 {d.get('upCount',0)}·하락 {d.get('downCount',0)}로 {breadth_word}였습니다. "
      f"상승률 상위는 {best1[0]}({best1[1]:+.2f}%), {best2[0]}({best2[1]:+.2f}%), {best3[0]}({best3[1]:+.2f}%)였고, "
      f"하락 상위는 {worst1[0]}({worst1[1]:+.2f}%), {worst2[0]}({worst2[1]:+.2f}%), {worst3[0]}({worst3[1]:+.2f}%)였습니다. "
      f"BTC는 {d.get('btcPrice','')}에서 {d.get('btcChangePct',0):+.2f}%, ETH는 {d.get('ethPrice','')}에서 "
      f"{d.get('ethChangePct',0):+.2f}%로 {majors_word}였습니다. 김치 프리미엄은 {kimchi}({kimchi_word}), "
      f"펀딩비는 BTC {funding_btc} / ETH {funding_eth}로 {funding_word}입니다. Breadth는 {d.get('breadth','—')}였고, "
      f"지수는 전일 {d.get('bm20PrevLevel',0):,.0f}pt에서 금일 {d.get('bm20Level',0):,.0f}pt로 "
      f"{d.get('bm20PointChange',0):+.0f}pt 이동했습니다."
    )
    return text

# ------------------------- Main Pipeline ------------------------

def main():
    # 1) Market snapshot
    df = fetch_markets(UNIVERSE)
    df = fill_previous_prices(df)

    # 2) Portfolio values (prev/today) → Rebase
    base_val = get_base_value()  # USD
    df["weight"] = df.index.map(W)

    # 포트폴리오 가치는 단순 가중 가격 합 (지수 산식에서 상대값이므로 통화단위 무관)
    port_prev = float((df["previous_price"] * df["weight"]).sum())
    port_now  = float((df["current_price"]  * df["weight"]).sum())

    bm20_prev = (port_prev / base_val) * 100 if base_val else 0.0
    bm20_now  = (port_now  / base_val) * 100 if base_val else 0.0
    bm20_point_change = bm20_now - bm20_prev
    bm20_change_pct = ((bm20_now / bm20_prev) - 1) * 100 if bm20_prev else 0.0
    rebased_multiple = (bm20_now / 100.0) if bm20_now else 0.0

    # 3) Per-asset returns & breadth
    df["ret_1d"] = (df["current_price"] / df["previous_price"] - 1) * 100
    up_count = int((df["ret_1d"] > 0).sum())
    down_count = int((df["ret_1d"] < 0).sum())

    # Best/Worst 3
    best3 = df.sort_values("ret_1d", ascending=False).head(3)[["ret_1d"]]
    worst3 = df.sort_values("ret_1d", ascending=True).head(3)[["ret_1d"]]
    best3_list = [[idx, float(r.ret_1d)] for idx, r in best3.itertuples()]
    worst3_list = [[idx, float(r.ret_1d)] for idx, r in worst3.itertuples()]

    # 4) Kimchi & Funding
    kimchi_str, _kp_meta = compute_kimchi_premium(df)
    fund_btc, fund_eth = get_funding_display_pair()

    # 5) BTC/ETH snapshot for text
    btc_price = df.at["BTC","current_price"] if "BTC" in df.index else None
    eth_price = df.at["ETH","current_price"] if "ETH" in df.index else None
    btc_ret = df.at["BTC","ret_1d"] if "BTC" in df.index else 0.0
    eth_ret = df.at["ETH","ret_1d"] if "ETH" in df.index else 0.0

    # 6) Charts
    # (a) Performance barh (TOP/WORST 3 묶음)
    perf_names = [x[0] for x in best3_list] + [x[0] for x in worst3_list]
    perf_vals  = [x[1] for x in best3_list] + [x[1] for x in worst3_list]
    bar_png = os.path.join(OUT_DIR_DATE, f"bm20_bar_{YMD}.png")
    plot_perf_barh(perf_names, perf_vals, bar_png)

    # (b) BTC/ETH 7D trend
    trend_png = os.path.join(OUT_DIR_DATE, f"bm20_trend_{YMD}.png")
    try:
        plot_btc_eth_trend_7d(trend_png)
    except Exception:
        # 실패해도 파이프라인 진행
        pass

    # 7) JSON for site
    d = {
        "asOf": YMD,
        "bm20Level": round(bm20_now, 2),
        "bm20PrevLevel": round(bm20_prev, 2),
        "bm20PointChange": round(bm20_point_change, 2),
        "bm20ChangePct": round(bm20_change_pct, 2),
        "rebasedMultiple": round(rebased_multiple, 2),
        "total": len(UNIVERSE),
        "upCount": up_count,
        "downCount": down_count,
        "breadth": f"{up_count} ↑ / {down_count} ↓",
        "best3": best3_list,
        "worst3": worst3_list,
        "btcPrice": (f"${btc_price:,.0f}" if btc_price else "—"),
        "btcChangePct": round(float(btc_ret), 2) if isinstance(btc_ret,(int,float)) else 0.0,
        "ethPrice": (f"${eth_price:,.0f}" if eth_price else "—"),
        "ethChangePct": round(float(eth_ret), 2) if isinstance(eth_ret,(int,float)) else 0.0,
        "kimchi": kimchi_str,
        "funding": {"btc": fund_btc, "eth": fund_eth},
    }
    d["news"] = build_news_long(d)

    with open(os.path.join(SITE_DIR, "bm20_latest.json"), "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False)

    # 8) CSV / TXT  (generate_report.py 호환: name, current_price, previous_price, weight_ratio [+ret_1d])
    df_out = df[["current_price","previous_price","ret_1d"]].copy()
    df_out["name"] = df_out.index  # 심볼 대문자
    df_out["weight_ratio"] = df_out.index.map(W)
    df_out = df_out[["name","current_price","previous_price","weight_ratio","ret_1d"]]
    df_out.to_csv(os.path.join(OUT_DIR_DATE, f"bm20_daily_data_{YMD}.csv"), index=False, encoding="utf-8")

    with open(os.path.join(OUT_DIR_DATE, f"bm20_news_{YMD}.txt"), "w", encoding="utf-8") as f:
        f.write(d["news"])  # 해석형 장문

    # 9) PDF (선택)
    if REPORTLAB_AVAILABLE:
        pdf_path = os.path.join(OUT_DIR_DATE, f"bm20_daily_{YMD}.pdf")
        c = canvas.Canvas(pdf_path, pagesize=A4)
        w, h = A4
        margin = 1.5*cm
        y = h - margin
        c.setFont("Helvetica-Bold", 14); c.drawString(margin, y, f"BM20 데일리 리포트  {YMD}")
        y -= 0.8*cm; c.setFont("Helvetica", 10)
        # 뉴스 문단 줄바꿈
        text = d["news"]
        line_len = 68
        lines = [text[i:i+line_len] for i in range(0, len(text), line_len)]
        for seg in lines:
            c.drawString(margin, y, seg); y -= 0.5*cm
        # 차트 삽입 (bar)
        bar_png_exists = os.path.exists(bar_png)
        if bar_png_exists:
            y -= 0.3*cm
            img_w = w - 2*margin
            img_h = img_w * 0.5
            c.drawImage(bar_png, margin, margin, width=img_w, height=img_h, preserveAspectRatio=True, anchor='sw')
        c.showPage(); c.save()

    # 10) 콘솔 알림
    print("Saved:",
          os.path.join(OUT_DIR_DATE, f"bm20_daily_data_{YMD}.csv"),
          os.path.join(OUT_DIR_DATE, f"bm20_news_{YMD}.txt"),
          bar_png, trend_png,
          os.path.join(SITE_DIR, "bm20_latest.json"))


if __name__ == "__main__":
    main()
