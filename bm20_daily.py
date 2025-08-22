import os, json, time, random, math
from datetime import datetime, timedelta, timezone, date as _date
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

# --- 맨 위 import 근처 교체 ---
import os, json, time, random, math
# ... 생략 ...

try:
    from bm20.price_sources.yahoo import get_price_usd  # (coin_id: str, date: Optional[date]) -> float
except Exception:
    import requests
    from datetime import datetime, timezone, timedelta, date as _date

    _UA = {"User-Agent": "Mozilla/5.0"}
    _Q = "https://query1.finance.yahoo.com/v8/finance/chart/{}"

    # Coingecko ID -> Yahoo 심볼 매핑 (필요한 것만 우선)
    _YH_SYMBOL = {
        "bitcoin": "BTC-USD",
        "ethereum": "ETH-USD",
        "ripple": "XRP-USD",
        "tether": "USDT-USD",
        "binancecoin": "BNB-USD",
        "dogecoin": "DOGE-USD",
        "toncoin": "TON-USD",
        "sui": "SUI-USD",
        "solana": "SOL-USD",
        "cardano": "ADA-USD",
        "avalanche-2": "AVAX-USD",
        "polkadot": "DOT-USD",
        "polygon": "MATIC-USD",
        "chainlink": "LINK-USD",
        "litecoin": "LTC-USD",
        "cosmos": "ATOM-USD",
        "near": "NEAR-USD",
        "aptos": "APT-USD",
        "filecoin": "FIL-USD",
        "internet-computer": "ICP-USD",
    }

    def _epoch(d: _date | None) -> int:
        if d is None:
            # now
            return int(time.time())
        return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())

    def get_price_usd(coin_id: str, day: _date | None) -> float:
        """
        Yahoo Finance에서 당일(실시간 근사) 또는 특정 날짜 종가(UTC 기준 1일 캔들) 반환.
        coin_id는 Coingecko ID 기준 (예: 'bitcoin', 'ethereum').
        """
        sym = _YH_SYMBOL.get(coin_id)
        if not sym:
            raise ValueError(f"Yahoo symbol not mapped for coin_id={coin_id}")

        if day is None:
            # 최근 가격: 1일 범위로 호출 후 마지막 클로즈 사용
            params = {"period1": _epoch(datetime.utcnow().date() - timedelta(days=5)),
                      "period2": _epoch(None), "interval": "1h"}
        else:
            # 해당 날짜의 일간 캔들
            start = _epoch(day - timedelta(days=2))
            end   = _epoch(day + timedelta(days=2))
            params = {"period1": start, "period2": end, "interval": "1d"}

        r = requests.get(_Q.format(sym), params=params, headers=_UA, timeout=15)
        r.raise_for_status()
        j = r.json()
        res = j.get("chart", {}).get("result", [])
        if not res:
            raise RuntimeError(f"Yahoo chart empty for {sym}")
        ind = res[0]["indicators"]["quote"][0]
        closes = ind.get("close") or []
        # 가장 마지막 유효값 반환
        for v in reversed(closes):
            if v is not None:
                return float(v)
        raise RuntimeError(f"No close data for {sym}")


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
CG = "https://api.coingecko.com/api/v3"  # (김프 폴백에서만 사용)
REQ_UA = {"User-Agent":"Mozilla/5.0"}
REQ_TIMEOUT = 15

# BM20 Universe (20)
UNIVERSE: List[str] = [
    "BTC","ETH","XRP","USDT","BNB",
    "DOGE","TON","SUI","SOL","ADA","AVAX","DOT","MATIC","LINK","LTC","ATOM","NEAR","APT","FIL","ICP"
]
CG_ID: Dict[str,str] = {
    "BTC":"bitcoin","ETH":"ethereum","XRP":"ripple","USDT":"tether","BNB":"binancecoin",
    "DOGE":"dogecoin","TON":"toncoin","SUI":"sui","SOL":"solana","ADA":"cardano","AVAX":"avalanche-2",
    "DOT":"polkadot","MATIC":"polygon","LINK":"chainlink","LTC":"litecoin","ATOM":"cosmos",
    "NEAR":"near","APT":"aptos","FIL":"filecoin","ICP":"internet-computer"
}

# 가중치 (BTC 30 / ETH 20 / XRP·USDT·BNB 5 / 나머지 균등 35%)
W: Dict[str,float] = {"BTC":0.30, "ETH":0.20, "XRP":0.05, "USDT":0.05, "BNB":0.05}
OTHERS = [s for s in UNIVERSE if s not in W]
eq = 0.35 / len(OTHERS)
for s in OTHERS:
    W[s] = round(eq, 6)

BASE_DATE_STR = "2018-01-01"

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
    try: return float(x)
    except Exception: return d

# ------------------------- Market Snapshot (Yahoo) --------------
def fetch_markets(symbols: List[str]) -> pd.DataFrame:
    """현재가/24h 변동률 생성. get_price_usd(coin_id, None)와 전일가(한국기준)로 계산.
    변동률 산출 실패 시 보수적으로 0 처리.
    """
    rows = []
    yday = (datetime.now(KST) - timedelta(days=1)).date()
    for s in symbols:
        cid = CG_ID[s]
        cur = None
        prev = None
        # 현재가
        try:
            cur = _safe_float(get_price_usd(cid, None), None)
        except Exception:
            cur = None
        # 전일가 (KST 기준 전일)
        try:
            prev = _safe_float(get_price_usd(cid, yday), None)
        except Exception:
            prev = None
        # 변동률
        if cur is not None and prev not in (None, 0):
            chg24 = (cur / prev - 1.0) * 100.0
        else:
            chg24 = 0.0
        rows.append({"symbol": s, "current_price": float(cur) if cur is not None else None,
                     "market_cap": None, "chg24": float(chg24)})
        time.sleep(0.02)
    df = pd.DataFrame(rows).set_index("symbol").reindex(symbols)
    return df


def fetch_yday_close(symbol: str) -> Optional[float]:
    yday = (datetime.now(KST) - timedelta(days=1)).date()
    try:
        return float(get_price_usd(CG_ID[symbol], yday))
    except Exception:
        return None


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
        time.sleep(0.01)
    df["previous_price"] = prevs
    return df

# ------------------------- Rebase -------------------------------
def fetch_price_on_2018(symbol: str) -> Optional[float]:
    try:
        return float(get_price_usd(CG_ID[symbol], _date(2018, 1, 1)))
    except Exception:
        return None


def get_base_value() -> float:
    """2018-01-01 포트폴리오(가중치 적용) 가치를 캐시하고 반환."""
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
        if p:
            base_val += p * W[s]
        time.sleep(0.01)
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
        return float(j["quoteResponse"]["result"][0]["regularMarketPrice"])
    except Exception:
        return None


def _fetch_btc_usd_binance() -> Optional[float]:
    try:
        return float(_get("https://api.binance.com/api/v3/ticker/price", params={"symbol":"BTCUSDT"})["price"])
    except Exception:
        return None


def _fetch_btc_krw_upbit() -> Optional[float]:
    try:
        return float(_get("https://api.upbit.com/v1/ticker", params={"markets":"KRW-BTC"})[0]["trade_price"])
    except Exception:
        return None


def compute_kimchi_premium(df: pd.DataFrame) -> Tuple[str, Dict]:
    btc_usd, btc_krw, usdkrw = _fetch_btc_usd_binance(), _fetch_btc_krw_upbit(), _fetch_usdkrw_yahoo()
    if btc_usd is None or btc_krw is None or usdkrw is None:
        try:
            j = _get(f"{CG}/simple/price", params={"ids":"bitcoin,tether","vs_currencies":"usd,krw"})
            btc_usd = btc_usd or float(j["bitcoin"]["usd"])
            btc_krw = btc_krw or float(j["bitcoin"].get("krw",0)) or None
            usdkrw = usdkrw or float(j["tether"]["krw"])
        except Exception:
            pass
    if btc_usd is None and "BTC" in df.index:
        btc_usd = float(df.loc["BTC","current_price"]) if pd.notna(df.loc["BTC","current_price"]) else None
    if usdkrw is None:
        usdkrw = 1350.0  # 보수적 기본값
    if not all([btc_usd, btc_krw, usdkrw]) or usdkrw <= 0:
        return "—", {}
    prem = (btc_krw / (btc_usd*usdkrw)) - 1.0
    return f"{prem*100:+.2f}%", {}

# ------------------------- Funding ------------------------------
def _funding_binance(symbol: str) -> Optional[float]:
    try:
        j = _get("https://fapi.binance.com/fapi/v1/fundingRate", params={"symbol":symbol,"limit":1})
        if j and isinstance(j,list):
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
    def fmt(x):
        return "중립권" if x is None else f"{x:+.4f}%"
    btc = _funding_binance("BTCUSDT") or _funding_bybit("BTCUSDT")
    eth = _funding_binance("ETHUSDT") or _funding_bybit("ETHUSDT")
    return fmt(btc), fmt(eth)

# ------------------------- Charts -------------------------------
def _set_dark_style():
    plt.rcParams.update({
        "figure.facecolor":"#0b1020","axes.facecolor":"#121831",
        "axes.edgecolor":"#28324d","axes.labelcolor":"#e6ebff",
        "xtick.color":"#cfd6ff","ytick.color":"#cfd6ff","text.color":"#e6ebff",
        "savefig.facecolor":"#0b1020","savefig.bbox":"tight","font.size":11,
    })


def plot_perf_barh(names: List[str], rets_pct: List[float], out_png: str):
    import numpy as np
    _set_dark_style()
    y = np.arange(len(names))
    vals = np.array(rets_pct, float)
    colors = np.where(vals >= 0, "#2E7D32", "#C62828")
    fig, ax = plt.subplots(figsize=(10,6), dpi=150)
    ax.barh(y, vals, color=colors)
    ax.axvline(0, color="#3a4569", lw=1)
    ax.set_yticks(y, labels=names)
    ax.set_xlabel("Daily Change (%)")
    ax.invert_yaxis()
    for yi, v in zip(y, vals):
        if pd.notna(v):
            ax.text(v + (0.05 if v >= 0 else -0.05), yi, f"{v:+.2f}%", va="center", ha=("left" if v >= 0 else "right"))
    ax.set_title(f"BM20 Daily Performance ({YMD})")
    fig.savefig(out_png, dpi=180)
    plt.close(fig)


def plot_btc_eth_trend_7d(out_png: str):
    _set_dark_style()
    days = [(datetime.now(KST) - timedelta(days=i)).date() for i in range(6,-1,-1)]
    ts = [datetime.combine(d, datetime.min.time(), tzinfo=KST) for d in days]
    b_vals, e_vals = [], []
    for d in days:
        try:
            b_vals.append(float(get_price_usd("bitcoin", d)))
        except Exception:
            b_vals.append(None)
        try:
            e_vals.append(float(get_price_usd("ethereum", d)))
        except Exception:
            e_vals.append(None)
        time.sleep(0.01)
    fig, ax = plt.subplots(figsize=(10,5), dpi=150)
    ax.plot(ts, b_vals, label="BTC")
    ax.plot(ts, e_vals, label="ETH")
    ax.set_title("BTC · ETH — 7D Trend (USD)")
    ax.legend()
    fig.autofmt_xdate()
    fig.savefig(out_png, dpi=180)
    plt.close(fig)

# ------------------------- Main Pipeline ------------------------
def main():
    print(f"[INFO] BM20 daily start: {YMD}")

    df = fetch_markets(UNIVERSE)
    if df["current_price"].isna().all():
        print("::error::현재가 수집에 실패했습니다. 네트워크/소스 확인")
        return 1

    df = fill_previous_prices(df)

    base_val = get_base_value()
    if base_val is None or base_val <= 0:
        print("::error::BASE VALUE 계산 실패")
        return 1

    # 포트폴리오 집계
    df["weight"] = df.index.map(W)
    # 전일/금일 포트폴리오 가치
    port_prev = float((df["previous_price"] * df["weight"]).sum()) if not df["previous_price"].isna().all() else 0.0
    port_now  = float((df["current_price"]  * df["weight"]).sum()) if not df["current_price"].isna().all()  else 0.0

    bm20_prev = (port_prev / base_val) * 100 if base_val else 0.0
    bm20_now  = (port_now  / base_val) * 100 if base_val else 0.0
    bm20_point_change = bm20_now - bm20_prev
    bm20_change_pct   = ((bm20_now / bm20_prev) - 1) * 100 if bm20_prev else 0.0
    rebased_multiple  = (bm20_now / 100.0) if bm20_now else 0.0

    # 1일 수익률
    with pd.option_context('mode.use_inf_as_na', True):
        df["ret_1d"] = ((df["current_price"] / df["previous_price"]) - 1) * 100
        df["ret_1d"] = df["ret_1d"].fillna(0.0)

    up_count  = int((df["ret_1d"] > 0).sum())
    down_count= int((df["ret_1d"] < 0).sum())

    best3 = df.sort_values("ret_1d", ascending=False).head(3)[["ret_1d"]]
    worst3= df.sort_values("ret_1d", ascending=True ).head(3)[["ret_1d"]]
    best3_list  = [[idx, float(r.ret_1d)] for idx, r in best3.itertuples()]
    worst3_list = [[idx, float(r.ret_1d)] for idx, r in worst3.itertuples()]

    kimchi_str, _ = compute_kimchi_premium(df)
    fund_btc, fund_eth = get_funding_display_pair()

    btc_price = df.at["BTC","current_price"] if "BTC" in df.index else None
    eth_price = df.at["ETH","current_price"] if "ETH" in df.index else None
    btc_ret   = df.at["BTC","ret_1d"] if "BTC" in df.index else 0.0
    eth_ret   = df.at["ETH","ret_1d"] if "ETH" in df.index else 0.0

    # Charts
    bar_png = os.path.join(OUT_DIR_DATE, f"bm20_bar_{YMD}.png")
    try:
        plot_perf_barh([x[0] for x in best3_list] + [x[0] for x in worst3_list],
                       [x[1] for x in best3_list] + [x[1] for x in worst3_list],
                       bar_png)
    except Exception as e:
        print(f"::warning::bar chart failed: {e}")

    trend_png = os.path.join(OUT_DIR_DATE, f"bm20_trend_{YMD}.png")
    try:
        plot_btc_eth_trend_7d(trend_png)
    except Exception as e:
        print(f"::warning::trend chart failed: {e}")

    # JSON payload (사이트/허브 공용)
    payload = {
        "asOf": YMD,
        "bm20Level": round(bm20_now, 2),
        "bm20PrevLevel": round(bm20_prev, 2),
        "bm20PointChange": round(bm20_point_change, 2),
        "bm20ChangePct": round(bm20_change_pct, 2),
        "rebasedMultiple": round(rebased_multiple, 2),
        "total": len(UNIVERSE),
        "upCount": up_count,
        "downCount": down_count,
        "breadth": f"{up_count} ↑ / {downCount} ↓",
        "best3": best3_list,
        "worst3": worst3_list,
        "btcPrice": None if btc_price is None else f"${btc_price:,.0f}",
        "btcChangePct": round(float(btc_ret), 2) if btc_ret is not None else 0.0,
        "ethPrice": None if eth_price is None else f"${eth_price:,.0f}",
        "ethChangePct": round(float(eth_ret), 2) if eth_ret is not None else 0.0,
        "kimchi": kimchi_str,
        "funding": {"btc": fund_btc, "eth": fund_eth},
    }

    # 파일 출력
    # 1) 사이트용 최신 JSON (site/)
    latest_site = os.path.join(SITE_DIR, "bm20_latest.json")
    with open(latest_site, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)

    # 2) 허브가 복사해 가는 최신 JSON (out/latest.json) — viewer 스테이지에서 집어감
    latest_out = os.path.join(OUT_DIR, "latest.json")
    with open(latest_out, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)

    # 3) 일자별 CSV/뉴스 텍스트
    df_out = df[["current_price","previous_price","ret_1d"]].copy()
    df_out["name"] = df_out.index
    df_out["weight_ratio"] = df_out.index.map(W)
    csv_path = os.path.join(OUT_DIR_DATE, f"bm20_daily_data_{YMD}.csv")
    df_out.to_csv(csv_path, index=False, encoding="utf-8")

    news_txt_path = os.path.join(OUT_DIR_DATE, f"bm20_news_{YMD}.txt")
    with open(news_txt_path, "w", encoding="utf-8") as f:
        f.write(str(payload))

    # 4) PDF (옵션)
    if REPORTLAB_AVAILABLE:
        try:
            pdf_path = os.path.join(OUT_DIR_DATE, f"bm20_daily_{YMD}.pdf")
            c = canvas.Canvas(pdf_path, pagesize=A4)
            w, h = A4
            margin = 1.5*cm
            y = h - margin
            c.setFont("Helvetica-Bold", 14)
            c.drawString(margin, y, f"BM20 데일리 리포트 {YMD}")
            y -= 0.8*cm
            c.setFont("Helvetica", 10)
            text = json.dumps(payload, ensure_ascii=False)
            lines = [text[i:i+68] for i in range(0, len(text), 68)]
            for seg in lines:
                c.drawString(margin, y, seg)
                y -= 0.5*cm
                if y < margin:
                    c.showPage(); y = h - margin; c.setFont("Helvetica", 10)
            if os.path.exists(bar_png):
                img_w = w - 2*margin
                img_h = img_w * 0.5
                c.drawImage(bar_png, margin, margin, width=img_w, height=img_h, preserveAspectRatio=True, anchor='sw')
            c.showPage(); c.save()
        except Exception as e:
            print(f"::warning::PDF generation failed: {e}")

    print("Saved:", csv_path, news_txt_path, bar_png, trend_png, latest_site, latest_out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
