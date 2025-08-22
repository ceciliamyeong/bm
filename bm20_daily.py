# ===================== BM20 Daily — Yahoo Only (Safe: 401/400 handled) =====================
# 기능: 유니버스/가중치(확정) → Yahoo 시세 수집 → 전일/금일 포트폴리오 가치 → 리베이스 지수 산출
#      김치 프리미엄 · 펀딩비 → Best/Worst → 뉴스 문장 → 차트(가로막대/7D) → JSON/TXT/CSV/PDF 저장
# 산출물: out/YYYY-MM-DD/ (csv/txt/png/pdf), site/bm20_latest.json

import os, json, time, math, random
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
BASE_CACHE_PATH = os.path.join(OUT_DIR, "base_cache.json")  # 2018-01-01 포트폴리오 가치 캐시

# ------------------------- Universe / Weights --------------------
UNIVERSE: List[str] = [
    "BTC","ETH","XRP","USDT","BNB",
    "DOGE","TON","SUI","SOL","ADA","AVAX","DOT","MATIC","LINK","LTC","ATOM","NEAR","APT","FIL","ICP"
]

# 가중치 확정 (BTC 30 / ETH 20 / XRP·USDT·BNB 각 5 / 나머지 15종 = 35% 균등)
W: Dict[str,float] = {"BTC":0.30, "ETH":0.20, "XRP":0.05, "USDT":0.05, "BNB":0.05}
OTHERS = [s for s in UNIVERSE if s not in W]
eq = 0.35 / len(OTHERS)
for s in OTHERS:
    W[s] = round(eq, 6)

BASE_DATE_STR = "2018-01-01"

# ------------------------- Yahoo mapping -------------------------
YF_TICKER: Dict[str, str] = {
    "BTC":  "BTC-USD",
    "ETH":  "ETH-USD",
    "XRP":  "XRP-USD",
    "USDT": "USDT-USD",
    "BNB":  "BNB-USD",
    "DOGE": "DOGE-USD",
    "TON":  "TON11419-USD",  # ✅ Toncoin (Tokamak의 TON-USD 아님)
    "SUI":  "SUI-USD",
    "SOL":  "SOL-USD",
    "ADA":  "ADA-USD",
    "AVAX": "AVAX-USD",
    "DOT":  "DOT-USD",
    "MATIC":"MATIC-USD",
    "LINK": "LINK-USD",
    "LTC":  "LTC-USD",
    "ATOM": "ATOM-USD",
    "NEAR": "NEAR-USD",
    "APT":  "APT-USD",
    "FIL":  "FIL-USD",
    "ICP":  "ICP-USD",
}

# ------------------------- HTTP Utils (robust) -------------------
UA = {"User-Agent":"Mozilla/5.0"}

def _get(url: str, params: dict=None, timeout: int=15, retry: int=3, sleep: float=0.5) -> dict:
    last = None
    for i in range(retry):
        try:
            r = requests.get(url, params=params, timeout=timeout, headers=UA)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            time.sleep(sleep*(i+1) + 0.2*random.random())
    raise last

def _safe_float(x, d: float=None) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return d

# ------------------------- Yahoo helpers (v7→v8 폴백) ----------
def _yf_quote_v7(symbols: List[str]) -> Dict[str, dict]:
    tickers = ",".join([YF_TICKER[s] for s in symbols if s in YF_TICKER])
    if not tickers:
        return {}
    j = _get("https://query1.finance.yahoo.com/v7/finance/quote", {"symbols": tickers})
    out = {}
    for q in j.get("quoteResponse", {}).get("result", []):
        t = q.get("symbol")
        out[t] = {
            "price": q.get("regularMarketPrice"),
            "prev_close": q.get("regularMarketPreviousClose"),
            "market_cap": q.get("marketCap"),
        }
    return out

def _yf_quote_single_via_chart(yf_ticker: str) -> Dict[str, Optional[float]]:
    """
    v8 chart로 2일 일봉을 받아 현재가/전일가 산출 (인증 불필요, 401 회피)
    """
    j = _get(f"https://query2.finance.yahoo.com/v8/finance/chart/{yf_ticker}",
             {"range": "2d", "interval": "1d"})
    try:
        res = j["chart"]["result"][0]
        closes = res["indicators"]["quote"][0]["close"]  # 보통 [전일, 금일]
        if not closes:
            return {"price": None, "prev_close": None, "market_cap": None}
        if len(closes) == 1:
            return {"price": float(closes[0]), "prev_close": None, "market_cap": None}
        return {"price": float(closes[-1]), "prev_close": float(closes[-2]), "market_cap": None}
    except Exception:
        return {"price": None, "prev_close": None, "market_cap": None}

def _yf_quote(symbols: List[str]) -> Dict[str, dict]:
    """
    1) v7 quote 일괄 조회 시도
    2) 실패(특히 401) 시 v8 chart 로 개별 폴백
    """
    try:
        return _yf_quote_v7(symbols)
    except requests.HTTPError as e:
        if getattr(e.response, "status_code", None) != 401:
            raise
    except Exception:
        pass

    out = {}
    for s in symbols:
        t = YF_TICKER[s]
        q = _yf_quote_single_via_chart(t)
        out[t] = q
        time.sleep(0.1)
    return out

# 안전 가격 로더: 날짜 가격 없으면 '최초 가용 종가'로 대체
def _yf_price_on_or_first(yf_ticker: str, date_ymd: str) -> Optional[float]:
    try:
        j = _get(
            f"https://query2.finance.yahoo.com/v8/finance/chart/{yf_ticker}",
            {"range": "max", "interval": "1d"}
        )
        res = j["chart"]["result"][0]
        ts = res.get("timestamp") or []
        cl = (res.get("indicators", {}).get("quote", [{}])[0].get("close")) or []
        if not ts or not cl:
            return None

        target = datetime.fromisoformat(date_ymd).date()
        for t, v in zip(ts, cl):
            if v is None:
                continue
            if datetime.fromtimestamp(t, timezone.utc).date() == target:
                return float(v)

        for v in cl:
            if v is not None:
                return float(v)
        return None
    except Exception:
        return None

# (호환성) 과거 호출이 있어도 안전버전이 동작하도록 동일 이름으로 덮어쓰기
def _yf_price_on_date(yf_ticker: str, date_ymd: str) -> Optional[float]:
    return _yf_price_on_or_first(yf_ticker, date_ymd)

def _yf_history(yf_ticker: str, range_str: str = "7d", interval: str = "1h") -> List[Tuple[datetime, float]]:
    j = _get(f"https://query2.finance.yahoo.com/v8/finance/chart/{yf_ticker}",
             {"range": range_str, "interval": interval})
    try:
        res = j["chart"]["result"][0]
        ts = res["timestamp"]
        cl = res["indicators"]["quote"][0]["close"]
        out = []
        for t, v in zip(ts, cl):
            if v is None:
                continue
            out.append((datetime.fromtimestamp(t, timezone.utc).astimezone(KST), float(v)))
        return out
    except Exception:
        return []

# ------------------------- Market Snapshot ----------------------
def fetch_markets(symbols: List[str]) -> pd.DataFrame:
    q = _yf_quote(symbols)  # v7 or v8(chart) 폴백
    rows = []
    for s in symbols:
        t = YF_TICKER[s]
        qq = q.get(t, {}) or {}
        price = _safe_float(qq.get("price"))
        prev  = _safe_float(qq.get("prev_close"))
        chg24 = ((price/prev - 1) * 100) if (price is not None and prev not in (None, 0)) else None
        rows.append({
            "symbol": s,
            "current_price": price,
            "previous_price": prev,   # 후속 보정 대상
            "market_cap": _safe_float(qq.get("market_cap")),  # chart 폴백이면 None
            "ret_1d": chg24
        })
    return pd.DataFrame(rows).set_index("symbol").reindex(symbols)

def fetch_yday_close(symbol: str) -> Optional[float]:
    t = YF_TICKER[symbol]
    q = _yf_quote_single_via_chart(t)
    return _safe_float(q.get("prev_close"))

def fill_previous_prices(df: pd.DataFrame) -> pd.DataFrame:
    # prev_close가 없거나 0이면 역산
    prevs = []
    for s in df.index:
        v = df.at[s, "previous_price"]
        if v in (None, 0) or (isinstance(v, float) and math.isnan(v)):
            try:
                v = fetch_yday_close(s)
            except Exception:
                v = None
        if v in (None, 0):
            cur = df.at[s, "current_price"]
            ch = df.at[s, "ret_1d"]
            if cur not in (None, 0) and ch not in (None,):
                try:
                    v = cur / (1 + float(ch)/100.0)
                except Exception:
                    v = cur
        prevs.append(v)
        time.sleep(0.05)
    df["previous_price"] = prevs
    return df

# ------------------------- Rebase (2018-01-01=100) --------------
def fetch_price_on_2018(symbol: str) -> Optional[float]:
    return _yf_price_on_or_first(YF_TICKER[symbol], BASE_DATE_STR)

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
        p = fetch_price_on_2018(s)  # 2018-01-01 없으면 최초 가용가 사용
        if p is None:
            continue
        base_val += p * W[s]
        time.sleep(0.05)

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
        j = _get("https://query1.finance.yahoo.com/v7/finance/quote", {"symbols":"USDKRW=X"})
        q = j["quoteResponse"]["result"][0]
        return float(q["regularMarketPrice"])
    except Exception:
        return None

def _fetch_btc_usd_binance() -> Optional[float]:
    try:
        j = _get("https://api.binance.com/api/v3/ticker/price", {"symbol":"BTCUSDT"})
        return float(j["price"])
    except Exception:
        return None

def _fetch_btc_krw_upbit() -> Optional[float]:
    try:
        j = _get("https://api.upbit.com/v1/ticker", {"markets":"KRW-BTC"})
        return float(j[0]["trade_price"])
    except Exception:
        return None

def compute_kimchi_premium(df: pd.DataFrame) -> Tuple[str, Dict]:
    btc_usd = _fetch_btc_usd_binance()
    btc_krw = _fetch_btc_krw_upbit()
    usdkrw = _fetch_usdkrw_yahoo()

    if btc_usd is None:
        try:
            btc_usd = float(df.loc["BTC","current_price"])
        except Exception:
            btc_usd = None
    if usdkrw is None:
        usdkrw = 1350.0

    if not all([btc_usd, btc_krw, usdkrw]) or usdkrw <= 0:
        return "—", {"btc_usd": btc_usd, "btc_krw": btc_krw, "usdkrw": usdkrw, "note":"incomplete"}

    prem = (btc_krw / (btc_usd * usdkrw)) - 1.0
    return f"{prem*100:+.2f}%", {"btc_usd": round(btc_usd,2), "btc_krw": round(btc_krw,0), "usdkrw": round(usdkrw,2)}

# ------------------------- Funding Rates ------------------------
def _funding_binance(symbol: str) -> Optional[float]:
    try:
        j = _get("https://fapi.binance.com/fapi/v1/fundingRate", {"symbol":symbol, "limit":1})
        if j and isinstance(j, list) and "fundingRate" in j[0]:
            return float(j[0]["fundingRate"]) * 100
    except Exception:
        return None
    return None

def _funding_bybit(symbol: str) -> Optional[float]:
    try:
        j = _get("https://api.bybit.com/v5/market/funding/history",
                 {"category":"linear","symbol":symbol,"limit":1})
        if j.get("retCode") == 0 and j.get("result",{}).get("list"):
            return float(j["result"]["list"][0]["fundingRate"]) * 100
    except Exception:
        return None
    return None

def get_funding_display_pair() -> Tuple[str,str]:
    def fmt(x: Optional[float]) -> str:
        if x is None: return "중립권"
        return f"{x:+.4f}%"
    btc = _funding_binance("BTCUSDT") or _funding_bybit("BTCUSDT")
    eth = _funding_binance("ETHUSDT") or _funding_bybit("ETHUSDT")
    return fmt(btc), fmt(eth)

# ------------------------- Charts -------------------------------
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
    ax.invert_yaxis()
    for yi, v in zip(y, vals):
        s = f"{v:+.2f}%"
        ax.text(v + (0.05 if v>=0 else -0.05), yi, s, va="center",
                ha=("left" if v>=0 else "right"))
    ax.set_title(f"BM20 Daily Performance  ({YMD})")
    fig.savefig(out_png, dpi=180)
    plt.close(fig)

def plot_btc_eth_trend_7d(out_png: str):
    _set_dark_style()
    b = _yf_history("BTC-USD", range_str="7d", interval="1h")
    e = _yf_history("ETH-USD", range_str="7d", interval="1h")
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
    # 1) Market snapshot (Yahoo)
    df = fetch_markets(UNIVERSE)
    df = fill_previous_prices(df)

    # 2) Portfolio values (prev/today) → Rebase
    base_val = get_base_value()  # USD
    df["weight"] = df.index.map(W)

    port_prev = float((df["previous_price"] * df["weight"]).sum())
    port_now  = float((df["current_price"]  * df["weight"]).sum())

    bm20_prev = (port_prev / base_val) * 100 if base_val else 0.0
    bm20_now  = (port_now  / base_val) * 100 if base_val else 0.0
    bm20_point_change = bm20_now - bm20_prev
    bm20_change_pct = ((bm20_now / bm20_prev) - 1) * 100 if bm20_prev else 0.0
    rebased_multiple = (bm20_now / 100.0) if bm20_now else 0.0

    # 3) Per-asset returns & breadth
    if "ret_1d" not in df.columns or df["ret_1d"].isna().all():
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
    perf_names = [x[0] for x in best3_list] + [x[0] for x in worst3_list]
    perf_vals  = [x[1] for x in best3_list] + [x[1] for x in worst3_list]
    bar_png = os.path.join(OUT_DIR_DATE, f"bm20_bar_{YMD}.png")
    plot_perf_barh(perf_names, perf_vals, bar_png)

    trend_png = os.path.join(OUT_DIR_DATE, f"bm20_trend_{YMD}.png")
    try:
        plot_btc_eth_trend_7d(trend_png)
    except Exception:
        pass  # 트렌드 실패해도 전체 파이프라인 진행

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

    # 8) CSV / TXT  (generate_report.py 호환)
    df_out = df[["current_price","previous_price","ret_1d"]].copy()
    df_out["name"] = df_out.index
    df_out["weight_ratio"] = df_out.index.map(W)
    df_out = df_out[["name","current_price","previous_price","weight_ratio","ret_1d"]]
    df_out.to_csv(os.path.join(OUT_DIR_DATE, f"bm20_daily_data_{YMD}.csv"), index=False, encoding="utf-8")

    with open(os.path.join(OUT_DIR_DATE, f"bm20_news_{YMD}.txt"), "w", encoding="utf-8") as f:
        f.write(d["news"])

    # 9) PDF (선택)
    if REPORTLAB_AVAILABLE:
        pdf_path = os.path.join(OUT_DIR_DATE, f"bm20_daily_{YMD}.pdf")
        c = canvas.Canvas(pdf_path, pagesize=A4)
        w, h = A4
        margin = 1.5*cm
        y = h - margin
        c.setFont("Helvetica-Bold", 14); c.drawString(margin, y, f"BM20 데일리 리포트  {YMD}")
        y -= 0.8*cm; c.setFont("Helvetica", 10)
        text = d["news"]
        line_len = 68
        lines = [text[i:i+line_len] for i in range(0, len(text), line_len)]
        for seg in lines:
            c.drawString(margin, y, seg); y -= 0.5*cm
        if os.path.exists(bar_png):
            y -= 0.3*cm
            img_w = w - 2*margin
            img_h = img_w * 0.5
            c.drawImage(bar_png, margin, margin, width=img_w, height=img_h, preserveAspectRatio=True, anchor='sw')
        c.showPage(); c.save()

    print("Saved:",
          os.path.join(OUT_DIR_DATE, f"bm20_daily_data_{YMD}.csv"),
          os.path.join(OUT_DIR_DATE, f"bm20_news_{YMD}.txt"),
          bar_png, trend_png,
          os.path.join(SITE_DIR, "bm20_latest.json"))

if __name__ == "__main__":
    main()
