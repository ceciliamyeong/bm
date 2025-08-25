#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BM20 Daily (stable)
- series.json을 읽어서 level/returns 계산
- 외부 시세 호출: 김치 프리미엄(Upbit KRW-BTC, USDKRW, Binance BTCUSDT) + 펀딩(Binance BTC/ETH)
- latest.json + site/bm20_latest.json 생성
"""

import os, sys, json, time, math, pathlib, logging, datetime as dt
from typing import Optional, List, Dict, Any

import requests
import pandas as pd
from dateutil.relativedelta import relativedelta

# -------------------- config --------------------
ROOT = pathlib.Path(".").resolve()
SITE = ROOT / "site"
OUT  = ROOT / "out" / dt.datetime.now(dt.timezone.utc).astimezone().strftime("%Y-%m-%d")

SERIES_CANDIDATES = [
    ROOT / "series.json",
    ROOT / "bm20_series.json",
    ROOT / "bm" / "series.json",
    ROOT / "bm" / "bm20_series.json",
    SITE / "series.json",
    SITE / "bm20_series.json",
]

LATEST_PATHS = [
    ROOT / "latest.json",
    SITE / "bm20_latest.json",  # 프론트에서 쓰는 경로 호환
]

TIMEOUT = 12
HEADERS = {"User-Agent": "BM20-Daily/1.0 (+https://blockmedia.co.kr)"}

# -------------------- logging --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)

def http_get_json(url: str, params: dict = None, timeout: int = TIMEOUT) -> Optional[dict]:
    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=timeout)
        if r.status_code == 200:
            return r.json()
        logging.warning("GET %s -> %s", url, r.status_code)
    except Exception as e:
        logging.warning("GET %s error: %s", url, e)
    return None

def read_series() -> pd.DataFrame:
    """series.json 읽어서 DataFrame(date, level)로 리턴"""
    for p in SERIES_CANDIDATES:
        if p.is_file():
            try:
                arr = json.loads(p.read_text(encoding="utf-8"))
                # 배열 원소는 {date, level} 또는 [date, level] 가정
                rows = []
                for d in arr:
                    if isinstance(d, dict):
                        date = d.get("date") or d.get("day") or d.get("asof") or d.get("asOf")
                        level = d.get("level") or d.get("index") or d.get("value") or d.get("close") or d.get("bm20Level")
                    elif isinstance(d, list) and len(d) >= 2:
                        date, level = d[0], d[1]
                    else:
                        continue
                    if date is None or level is None:
                        continue
                    rows.append((str(date), float(level)))
                if not rows:
                    raise ValueError("series.json is empty or invalid shape")
                df = pd.DataFrame(rows, columns=["date", "level"])
                df["date"] = pd.to_datetime(df["date"]).dt.date
                df = df.sort_values("date").dropna()
                logging.info("Loaded series from %s (%d rows)", p, len(df))
                return df
            except Exception as e:
                logging.warning("Failed to parse %s: %s", p, e)
    raise FileNotFoundError("No usable series.json found from candidates")

# -------------------- return calc --------------------
def compute_returns(df: pd.DataFrame, today: Optional[dt.date] = None) -> Dict[str, Any]:
    if today is None:
        today = df["date"].max()
    last_row = df.loc[df["date"] == today]
    if last_row.empty:
        # 마지막 날짜가 오늘과 다를 수 있음 → 마지막 행 기준
        last_row = df.iloc[[-1]]
        today = last_row["date"].item()

    level = float(last_row["level"].item())

    def level_at(target_date: dt.date) -> Optional[float]:
        row = df.loc[df["date"] == target_date]
        if not row.empty:
            return float(row["level"].iloc[0])
        # 가장 가까운 과거로 보간
        past = df[df["date"] <= target_date]
        if past.empty: 
            return None
        return float(past.iloc[-1]["level"])

    # 기준일들
    d1   = today - relativedelta(days=1)
    d7   = today - relativedelta(days=7)
    d30  = today - relativedelta(days=30)
    d1y  = today - relativedelta(years=1)
    ytd0 = dt.date(today.year, 1, 1)

    def pct(from_level: Optional[float]) -> Optional[float]:
        if from_level is None or from_level == 0:
            return None
        return (level / from_level) - 1.0

    returns = {
        "1D":  pct(level_at(d1)),
        "7D":  pct(level_at(d7)),
        "30D": pct(level_at(d30)),
        "1Y":  pct(level_at(d1y)),
        "YTD": pct(level_at(ytd0)),
    }

    # 전일/포인트 변화
    prev_level = level_at(d1)
    point_change = level - prev_level if prev_level is not None else None
    change_pct   = pct(prev_level)

    return {
        "asOf": today.isoformat(),
        "bm20Level": level,
        "bm20PrevLevel": prev_level,
        "bm20PointChange": point_change,
        "bm20ChangePct": change_pct,
        "returns": returns,
    }

# -------------------- external: kimchi & funding --------------------
def fetch_upbit_btc_krw() -> Optional[float]:
    # 업비트: https://api.upbit.com/v1/ticker?markets=KRW-BTC
    j = http_get_json("https://api.upbit.com/v1/ticker", params={"markets": "KRW-BTC"})
    try:
        return float(j[0]["trade_price"])
    except Exception:
        return None

def fetch_usd_krw_via_binance() -> Optional[float]:
    # Binance: USDUSDT 가격 ~ 1 근처, KRWUSDT 로 환산 (직접 KRW 페어가 없어 간단 fallback)
    # 더 안정적으로 하려면 한국은행/공개 환율 API를 붙이면 됨.
    # 여기서는 BTCUSDT/BTCKRW로 암묵 환율을 구하는 대체 루트도 마련.
    j = http_get_json("https://api.binance.com/api/v3/ticker/price", params={"symbol": "USDTBUSD"})
    try:
        px = float(j["price"])
        if px > 0:
            return 1.0  # BUSD delist 이후 부정확 → 아래 암묵 환율 사용으로 대체
    except Exception:
        pass
    return None

def fetch_binance_px(symbol: str) -> Optional[float]:
    j = http_get_json("https://api.binance.com/api/v3/ticker/price", params={"symbol": symbol})
    try:
        return float(j["price"])
    except Exception:
        return None

def compute_kimchi() -> Optional[float]:
    """
    kimchi ≈ ((BTC/KRW ÷ USD/KRW) / BTC/USDT) - 1
    - BTC/KRW: Upbit
    - USD/KRW: (없으면 암묵 환율로 대체) = (BTC/KRW) / (BTC/USDT × USDT/USD≈1)
    - BTC/USDT: Binance
    """
    btc_krw = fetch_upbit_btc_krw()
    btc_usdt = fetch_binance_px("BTCUSDT")
    if btc_krw and btc_usdt:
        usd_krw = fetch_usd_krw_via_binance()
        if not usd_krw or usd_krw <= 0:
            # 암묵 환율
            usd_krw = btc_krw / btc_usdt
        try:
            kimchi = ((btc_krw / usd_krw) / btc_usdt) - 1.0
            return float(kimchi)
        except Exception:
            return None
    return None

def fetch_binance_funding() -> Optional[dict]:
    out = {}
    j1 = http_get_json("https://fapi.binance.com/fapi/v1/premiumIndex", params={"symbol": "BTCUSDT"})
    if j1 and "lastFundingRate" in j1:
        try: out["btc"] = float(j1["lastFundingRate"])
        except Exception: pass
    j2 = http_get_json("https://fapi.binance.com/fapi/v1/premiumIndex", params={"symbol": "ETHUSDT"})
    if j2 and "lastFundingRate" in j2:
        try: out["eth"] = float(j2["lastFundingRate"])
        except Exception: pass
    return out or None

# -------------------- io helpers --------------------
def atomic_write_json(path: pathlib.Path, obj: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".tmp.{int(time.time())}")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)

# -------------------- main --------------------
def main():
    try:
        df = read_series()
    except Exception as e:
        logging.error("series.json 로드 실패: %s", e)
        sys.exit(1)

    core = compute_returns(df)

    # 외부 지표(실패해도 전체 파이프라인은 계속)
    kimchi = None
    funding = None

    try:
        kimchi = compute_kimchi()
        logging.info("kimchi: %s", kimchi)
    except Exception as e:
        logging.warning("kimchi 계산 실패: %s", e)

    try:
        funding = fetch_binance_funding()
        logging.info("funding: %s", funding)
    except Exception as e:
        logging.warning("funding 조회 실패: %s", e)

    latest = {**core}
    if kimchi is not None:
        latest["kimchi"] = kimchi
    if funding:
        latest["funding"] = funding

    # 산출물 쓰기 (루트 + site/)
    for p in LATEST_PATHS:
        try:
            atomic_write_json(p, latest)
            logging.info("wrote %s", p)
        except Exception as e:
            logging.warning("failed to write %s: %s", p, e)

    # out/YYYY-MM-DD 폴더에도 보존(감사/추적용)
    try:
        OUT.mkdir(parents=True, exist_ok=True)
        atomic_write_json(OUT / "bm20_latest.json", latest)
    except Exception as e:
        logging.warning("failed to write out folder: %s", e)

    logging.info("DONE.")

if __name__ == "__main__":
    main()

