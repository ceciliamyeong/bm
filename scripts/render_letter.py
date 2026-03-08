#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Render letter.html by replacing placeholders in letter_newsletter_template.html.

Design goals
- Never leave {{PLACEHOLDER}} strings in output: fill with real values or "—"
- Be resilient to small schema changes (missing keys, renamed columns)
- Keep templates mail-friendly: pure string replacement, no JS

Inputs (expected in repo)
- letter_newsletter_template.html
- bm20_latest.json
- bm20_daily_data_latest.csv
- out/history/krw_24h_latest.json
- out/history/btc_usd_series.json (optional; if missing, BTC blocks become "—")
- data/bm20_history.json (for sentiment; optional)
- out/global/k_xrp_share_24h_latest.json (optional)
- out/latest/news_one_liner.txt (optional)
- out/latest/news_one_liner_note.txt (optional)
- out/latest/top_news_latest.json (optional)

Output
- letter.html
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Tuple

import pandas as pd
import requests
from datetime import datetime, timezone, timedelta

ROOT = Path(__file__).resolve().parent.parent

TEMPLATE = ROOT / "newsletter_blockmedia.html"  # 블록미디어 공식 뉴스레터 템플릿

BM20_JSON = ROOT / "bm20_latest.json"
DAILY_CSV = ROOT / "bm20_daily_data_latest.csv"
KRW_JSON = ROOT / "out/history/krw_24h_latest.json"
BTC_JSON = ROOT / "out/history/btc_usd_series.json"  # optional

BM20_HISTORY_JSON = ROOT / "data/bm20_history.json"  # optional
XRP_KR_SHARE_JSON = ROOT / "out/global/k_xrp_share_24h_latest.json"  # optional
ETF_JSON          = ROOT / "data/etf_summary.json"               # optional

NEWS_ONELINER_TXT = ROOT / "out/latest/news_one_liner.txt"
NEWS_ONELINER_NOTE_TXT = ROOT / "out/latest/news_one_liner_note.txt"
TOP_NEWS_JSON = ROOT / "out/latest/top_news_latest.json"

OUT = ROOT / "letter.html"

GREEN = "#16a34a"
RED = "#dc2626"
INK = "#0f172a"
MUTED = "#64748b"

# 1x1 transparent gif to avoid broken image boxes in email clients
TRANSPARENT_GIF = "data:image/gif;base64,R0lGODlhAQABAAAAACH5BAEKAAEALAAAAAABAAEAAAICTAEAOw=="


# ─────────────────────────────────────────────────────────
# 실시간 데이터: CoinGecko 티커 + 업비트 Top/Bottom + 프리미엄
# ─────────────────────────────────────────────────────────

def _kst_now() -> str:
    kst = timezone(timedelta(hours=9))
    return datetime.now(kst).strftime("%Y.%m.%d %H:%M")


def fetch_yahoo_ticker() -> dict[str, str]:
    """BTC·ETH·XRP 현재가 + 24h 변동률
    yfinance 1.2.x: history(period='2d') 방식 사용
    """
    import yfinance as yf

    SYMBOLS = [("BTC-USD", "BTC"), ("ETH-USD", "ETH"), ("XRP-USD", "XRP")]
    fb = {"PRICE": "—", "CHANGE": "—", "COLOR": "ticker-down"}
    fallback = {
        **{f"TICKER_BTC_{k}": v for k, v in fb.items()},
        **{f"TICKER_ETH_{k}": v for k, v in fb.items()},
        **{f"TICKER_XRP_{k}": v for k, v in fb.items()},
        "TICKER_TIME": _kst_now(),
    }

    result = {}
    try:
        syms_str = " ".join(s for s, _ in SYMBOLS)
        # 1.2.x: download() 로 한 번에 가져오기
        df = yf.download(syms_str, period="2d", interval="1d",
                         auto_adjust=True, progress=False, threads=False)

        for yf_sym, sym in SYMBOLS:
            try:
                # multi-ticker download는 컬럼이 (field, ticker) MultiIndex
                close_col = ("Close", yf_sym) if (("Close", yf_sym) in df.columns) else "Close"
                closes = df[close_col].dropna()
                if len(closes) < 2:
                    raise ValueError("not enough data")
                prev  = float(closes.iloc[-2])
                price = float(closes.iloc[-1])
                chg   = (price - prev) / prev * 100

                if price >= 1_000:
                    p_str = f"${price:,.0f}"
                elif price >= 1:
                    p_str = f"${price:,.2f}"
                else:
                    p_str = f"${price:.4f}"

                arrow = "▲" if chg >= 0 else "▼"
                cls   = "ticker-up" if chg >= 0 else "ticker-down"
                result[f"TICKER_{sym}_PRICE"]  = p_str
                result[f"TICKER_{sym}_CHANGE"] = f"{arrow}{abs(chg):.1f}%"
                result[f"TICKER_{sym}_COLOR"]  = cls
                print(f"INFO: {sym} {p_str} {arrow}{abs(chg):.1f}%")
            except Exception as e:
                print(f"WARN: Yahoo ticker {yf_sym} parse failed: {e}")
                result[f"TICKER_{sym}_PRICE"]  = "—"
                result[f"TICKER_{sym}_CHANGE"] = "—"
                result[f"TICKER_{sym}_COLOR"]  = "ticker-down"

        result["TICKER_TIME"] = _kst_now()
        return result

    except Exception as e:
        print(f"WARN: Yahoo Finance download failed: {e}")
        return fallback


fetch_coingecko_ticker = fetch_yahoo_ticker  # 하위 호환 alias


def fetch_upbit_top_bottom(n: int = 3) -> dict[str, str]:
    """업비트 KRW 전체 마켓 24h 등락률 Top/Bottom n"""
    FB = {**{f"UPBIT_TOP{i}_SYMBOL": "—" for i in range(1,n+1)},
          **{f"UPBIT_TOP{i}_CHG":    "—" for i in range(1,n+1)},
          **{f"UPBIT_BOT{i}_SYMBOL": "—" for i in range(1,n+1)},
          **{f"UPBIT_BOT{i}_CHG":    "—" for i in range(1,n+1)}}
    try:
        mkts = [m["market"] for m in
                requests.get("https://api.upbit.com/v1/market/all",
                             params={"isDetails":"false"}, timeout=10).json()
                if m["market"].startswith("KRW-")]
        tickers = []
        for i in range(0, len(mkts), 100):
            tickers += requests.get("https://api.upbit.com/v1/ticker",
                                    params={"markets": ",".join(mkts[i:i+100])},
                                    timeout=10).json()
        tickers.sort(key=lambda x: x.get("signed_change_rate", 0), reverse=True)
        result = {}
        for i, t in enumerate(tickers[:n], 1):
            sym = t["market"].replace("KRW-", "")
            pct = float(t.get("signed_change_rate", 0)) * 100
            result[f"UPBIT_TOP{i}_SYMBOL"] = sym
            result[f"UPBIT_TOP{i}_CHG"]    = f"+{pct:.1f}%"
        for i, t in enumerate(reversed(tickers[-n:]), 1):
            sym = t["market"].replace("KRW-", "")
            pct = float(t.get("signed_change_rate", 0)) * 100
            result[f"UPBIT_BOT{i}_SYMBOL"] = sym
            result[f"UPBIT_BOT{i}_CHG"]    = f"{pct:.1f}%"
        return result
    except Exception as e:
        print(f"WARN: Upbit top/bottom failed: {e}")
        return FB


def fetch_premium_data(usdkrw: float | None) -> dict[str, str]:
    """김치 프리미엄 vs 코인베이스 프리미엄 계산"""
    FB = {"KIMCHI_PREM_PCT": "—", "CB_PREMIUM_PCT": "—",
          "PREMIUM_COMMENT": "프리미엄 데이터를 가져올 수 없습니다."}
    try:
        upbit_btc_krw = float(
            requests.get("https://api.upbit.com/v1/ticker",
                         params={"markets":"KRW-BTC"}, timeout=10).json()[0]["trade_price"])
        # Yahoo Finance BTC USD 기준가 (프리미엄 계산용)
        import yfinance as yf
        df_btc = yf.download("BTC-USD", period="1d", interval="1m",
                             auto_adjust=True, progress=False, threads=False)
        if df_btc.empty:
            raise ValueError("Yahoo BTC data empty")
        cg_usd = float(df_btc["Close"].iloc[-1])
        fx = usdkrw if (usdkrw and usdkrw > 100) else 1350.0
        cb_usd = float(
            requests.get("https://api.coinbase.com/v2/prices/BTC-USD/spot", timeout=10).json()["data"]["amount"])
        upbit_usd  = upbit_btc_krw / fx
        kimchi_pct = (upbit_usd - cg_usd)  / cg_usd  * 100
        cb_pct     = (upbit_usd - cb_usd)   / cb_usd  * 100

        def _c(v: float) -> str:
            arrow = "▲" if v >= 0 else "▼"
            color = GREEN if v >= 0 else RED
            return f'<span style="color:{color};font-weight:900;">{arrow}{abs(v):.2f}%</span>'

        if kimchi_pct > 1 and cb_pct > 0:
            comment = "김치·코인베이스 프리미엄 동시 양전 → 글로벌 대비 국내 수요 강세 신호."
        elif kimchi_pct > 1 and cb_pct <= 0:
            comment = "김치 프리미엄 양전, 코인베이스 디스카운트 → 국내 단독 매수세 주의."
        elif kimchi_pct < -0.5:
            comment = "김치 역프리미엄 → 국내 매도 압력 또는 원화 약세 영향 가능성."
        else:
            comment = f"김치 {kimchi_pct:+.2f}% / 코인베이스 {cb_pct:+.2f}% — 중립 구간."

        return {"KIMCHI_PREM_PCT": _c(kimchi_pct), "CB_PREMIUM_PCT": _c(cb_pct), "PREMIUM_COMMENT": comment}
    except Exception as e:
        print(f"WARN: Premium fetch failed: {e}")
        return FB

def load_etf_summary() -> dict[str, str]:
    """data/etf_summary.json → ETF 플레이스홀더 딕셔너리"""
    FB = {
        "{{ETF_BTC_INFLOW}}": "—", "{{ETF_BTC_AUM}}": "—", "{{ETF_BTC_CUM}}": "—", "{{ETF_BTC_HOLDINGS}}": "—",
        "{{ETF_ETH_INFLOW}}": "—", "{{ETF_ETH_AUM}}": "—", "{{ETF_ETH_CUM}}": "—", "{{ETF_ETH_HOLDINGS}}": "—",
        "{{ETF_SOL_INFLOW}}": "—", "{{ETF_SOL_AUM}}": "—", "{{ETF_SOL_CUM}}": "—", "{{ETF_SOL_HOLDINGS}}": "—",
        "{{ETF_BTC_INFLOW_COLOR}}": "color:#64748b;",
        "{{ETF_ETH_INFLOW_COLOR}}": "color:#64748b;",
        "{{ETF_SOL_INFLOW_COLOR}}": "color:#64748b;",
        "{{ETF_COMMENT}}": "ETF 데이터를 불러올 수 없습니다.",
        "{{ETF_ASOF}}": "—",
    }
    if not ETF_JSON.exists():
        print(f"WARN: ETF json not found: {ETF_JSON}")
        return FB
    try:
        raw = json.loads(ETF_JSON.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"WARN: ETF json parse error: {e}")
        return FB

    def _fmt_usd(val) -> str:
        try:
            v = float(val)
        except Exception:
            return "—"
        billions = v / 1_000_000_000
        if abs(billions) >= 1:
            return f"${billions:+.1f}B"
        return f"${v/1_000_000:+.0f}M"

    def _fmt_aum(val) -> str:
        try:
            return f"${float(val)/1_000_000_000:.1f}B"
        except Exception:
            return "—"

    def _fmt_holdings(val, sym) -> str:
        try:
            return f"{float(val):,.0f} {sym}"
        except Exception:
            return "—"

    def _inflow_color(val) -> str:
        try:
            v = float(val)
            if v > 0: return "color:#16a34a;font-weight:900;"
            if v < 0: return "color:#dc2626;font-weight:900;"
        except Exception:
            pass
        return "color:#64748b;"

    def _parse(coin: str, sym: str) -> dict:
        d = raw.get(coin, {})
        inflow_raw = d.get("dailyNetInflow")
        return {
            f"{{{{ETF_{sym}_INFLOW}}}}":      _fmt_usd(inflow_raw),
            f"{{{{ETF_{sym}_AUM}}}}":          _fmt_aum(d.get("totalNetAssets")),
            f"{{{{ETF_{sym}_CUM}}}}":          _fmt_usd(d.get("cumNetInflow")),
            f"{{{{ETF_{sym}_HOLDINGS}}}}":     _fmt_holdings(d.get("totalTokenHoldings"), sym),
            f"{{{{ETF_{sym}_INFLOW_COLOR}}}}": _inflow_color(inflow_raw),
        }

    result = {}
    result.update(_parse("btc", "BTC"))
    result.update(_parse("eth", "ETH"))
    result.update(_parse("sol", "SOL"))

    try:
        btc_v = float(raw.get("btc", {}).get("dailyNetInflow", 0))
        eth_v = float(raw.get("eth", {}).get("dailyNetInflow", 0))
        if btc_v > 0 and eth_v > 0:
            comment = "BTC·ETH ETF 동시 순유입 — 기관 수급 전반적 우호."
        elif btc_v > 0 and eth_v <= 0:
            comment = "BTC ETF 순유입, ETH 소폭 유출 — BTC 집중 매수 구간."
        elif btc_v < 0 and eth_v < 0:
            comment = "BTC·ETH ETF 동시 순유출 — 기관 단기 차익실현 신호."
        else:
            comment = "ETF 혼조세 — 방향성 확인 필요."
    except Exception:
        comment = "—"

    result["{{ETF_COMMENT}}"] = comment
    result["{{ETF_ASOF}}"] = str(raw.get("updatedAt", "—"))[:10]
    return result


# ------------------ small IO helpers ------------------

def load_json(p: Path) -> Any:
    if not p.exists():
        raise FileNotFoundError(f"Missing {p}")
    return json.loads(p.read_text(encoding="utf-8"))

def load_json_optional(p: Path) -> Any | None:
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))

def load_text_first_line(p: Path) -> str:
    if not p.exists():
        return "—"
    s = p.read_text(encoding="utf-8").strip()
    if not s:
        return "—"
    return (s.splitlines()[0].strip() or "—")

def load_top_news_3(p: Path) -> tuple[str, str, str]:
    if not p.exists():
        return ("—", "—", "—")
    obj = json.loads(p.read_text(encoding="utf-8"))
    items = obj.get("items", []) if isinstance(obj, dict) else (obj or [])
    items = [str(x).strip() for x in items if str(x).strip()]
    while len(items) < 3:
        items.append("—")
    return (items[0], items[1], items[2])

# ------------------ formatting helpers ------------------

def fmt_level(x: float) -> str:
    return f"{float(x):,.2f}"

def fmt_num(x: float, digits: int = 2) -> str:
    return f"{float(x):,.{digits}f}"

def fmt_share_pct(x: float) -> str:
    x = float(x)
    # accept ratio or pct
    if abs(x) <= 1.5:
        x *= 100.0
    return f"{x:.1f}%"

def fmt_krw_big(x: float) -> str:
    x = float(x)
    jo = 1_0000_0000_0000  # 1조
    eok = 1_0000_0000      # 1억
    if x >= jo:
        return f"{x/jo:.2f}조원"
    if x >= eok:
        return f"{x/eok:.1f}억원"
    return f"{x:,.0f}원"

def pct_to_display(x: float) -> float:
    """Accept ratio(<=1.5) or pct; return pct number."""
    x = float(x)
    if abs(x) <= 1.5:
        x *= 100.0
    return x

def colored_change_html(pct_value: float, digits: int = 2, wrap_parens: bool = False) -> str:
    v = float(pct_value)
    if v > 0:
        arrow, color = "▲", GREEN
    elif v < 0:
        arrow, color = "▼", RED
    else:
        arrow, color = "", INK

    s = f"{v:+.{digits}f}%"
    text = f"{arrow} {s}".strip()
    if wrap_parens:
        text = f"({text})"
    return f'<span style="color:{color};font-weight:900;">{text}</span>'

def tone_bg(pct_value: float) -> str:
    v = float(pct_value)
    if v > 0:
        return "#f0fdf4"
    if v < 0:
        return "#fef2f2"
    return "#fbfdff"

# ------------------ daily csv helpers ------------------

def load_daily_df() -> pd.DataFrame:
    if not DAILY_CSV.exists():
        raise FileNotFoundError(f"Missing {DAILY_CSV}")
    df = pd.read_csv(DAILY_CSV)

    # normalize symbol
    if "symbol" not in df.columns:
        for c in ("ticker", "asset"):
            if c in df.columns:
                df = df.rename(columns={c: "symbol"})
                break

    # normalize price_change_pct
    if "price_change_pct" not in df.columns:
        for c in ("change_pct", "pct_change", "return_1d_pct", "return_1d"):
            if c in df.columns:
                df = df.rename(columns={c: "price_change_pct"})
                break

    df["symbol"] = df["symbol"].astype(str).str.upper()
    df["price_change_pct"] = pd.to_numeric(df["price_change_pct"], errors="coerce")
    df = df.dropna(subset=["price_change_pct"])
    return df

def compute_best_worst_breadth(df: pd.DataFrame, n=3) -> Tuple[str, str, str, int, int]:
    best = df.sort_values("price_change_pct", ascending=False).head(n)
    worst = df.sort_values("price_change_pct", ascending=True).head(n)

    best_txt = "<br/>".join([f"{r.symbol} {r.price_change_pct:+.2f}%" for r in best.itertuples()])
    worst_txt = "<br/>".join([f"{r.symbol} {r.price_change_pct:+.2f}%" for r in worst.itertuples()])

    up = int((df["price_change_pct"] > 0).sum())
    down = int((df["price_change_pct"] < 0).sum())
    breadth = f"상승 {up} · 하락 {down}"
    return best_txt, worst_txt, breadth, up, down

def compute_moves_top3(df: pd.DataFrame) -> Tuple[str, str, str]:
    top = df.sort_values("price_change_pct", ascending=False).head(3)
    moves = [f"{r.symbol} {r.price_change_pct:+.2f}%" for r in top.itertuples()]
    while len(moves) < 3:
        moves.append("—")
    return moves[0], moves[1], moves[2]

def compute_top10_concentration(df: pd.DataFrame) -> str:
    """
    If CSV has volume columns, compute Top10 volume concentration.
    Fallback: return "—".
    """
    vol_col = None
    for c in ("volume_24h", "quote_volume_24h", "turnover_24h", "krw_volume_24h"):
        if c in df.columns:
            vol_col = c
            break
    if not vol_col:
        return "—"
    s = pd.to_numeric(df[vol_col], errors="coerce").dropna()
    if s.empty:
        return "—"
    top10 = s.sort_values(ascending=False).head(10).sum()
    total = s.sum()
    if total <= 0:
        return "—"
    return fmt_share_pct(top10 / total)

# ------------------ sentiment + xrp share helpers ------------------

def extract_sentiment(obj: Any) -> tuple[str, str]:
    """
    Extract sentiment label/score from a flexible bm20_history.json shape.
    Returns (label, score_str)
    """
    label = None
    score = None

    def pick(d: dict, keys: tuple[str, ...]) -> Any | None:
        for k in keys:
            if k in d and d.get(k) is not None:
                return d.get(k)
        return None

    if isinstance(obj, dict):
        label = pick(obj, ("sentiment_label", "sentimentLabel", "label", "market_sentiment_label", "sentiment"))
        score = pick(obj, ("sentiment_score", "sentimentScore", "score", "market_sentiment_score", "sentiment_index"))

        latest = obj.get("latest") if isinstance(obj.get("latest"), dict) else None
        if latest:
            if label is None:
                label = pick(latest, ("sentiment_label", "sentimentLabel", "label", "sentiment"))
            if score is None:
                score = pick(latest, ("sentiment_score", "sentimentScore", "score", "sentiment_index"))

        series = obj.get("series")
        if (label is None or score is None) and isinstance(series, list) and series and isinstance(series[-1], dict):
            last = series[-1]
            if label is None:
                label = pick(last, ("sentiment_label", "sentimentLabel", "label", "sentiment"))
            if score is None:
                score = pick(last, ("sentiment_score", "sentimentScore", "score", "sentiment_index"))

    label_txt = str(label).strip() if label is not None else "—"
    score_txt = "—"
    if score is not None:
        try:
            score_txt = f"{float(score):.0f}"
        except Exception:
            score_txt = str(score).strip() or "—"
    return label_txt, score_txt

def extract_xrp_kr_share(obj: Any) -> str:
    if not isinstance(obj, dict):
        return "—"

    def pick(d: dict, keys: tuple[str, ...]) -> Any | None:
        for k in keys:
            if k in d and d.get(k) is not None:
                return d.get(k)
        return None

    v = pick(obj, ("xrp_kr_share", "xrp_kr_share_pct", "share_pct", "share", "value"))
    if v is None and isinstance(obj.get("latest"), dict):
        v = pick(obj["latest"], ("xrp_kr_share", "xrp_kr_share_pct", "share_pct", "share", "value"))

    if v is None:
        return "—"

    try:
        return fmt_share_pct(float(v))
    except Exception:
        return str(v).strip() or "—"

# ------------------ synthetic one-line interpreters ------------------

def synth_market_one_line(bm20_dir: str, breadth: str, krw_total: str, kimchi_txt: str) -> str:
    # simple, readable, stable
    parts = []
    if bm20_dir and bm20_dir != "보합":
        parts.append(f"BM20 {bm20_dir}")
    parts.append(breadth)
    if krw_total != "—":
        parts.append(f"KRW 24h {krw_total}")
    if kimchi_txt != "—":
        parts.append(f"김치 {kimchi_txt}")
    return " · ".join(parts) if parts else "—"

def synth_treemap_one_line(best3: str, worst3: str) -> str:
    # Use first line of Best3/Worst3 for quick interpretation
    b = (best3.split("<br/>")[0] if best3 and best3 != "—" else "").strip()
    w = (worst3.split("<br/>")[0] if worst3 and worst3 != "—" else "").strip()
    if b and w:
        return f"상승 선두: {b} / 약세 선두: {w}"
    if b:
        return f"상승 선두: {b}"
    if w:
        return f"약세 선두: {w}"
    return "—"

# ------------------ placeholders ------------------

def build_placeholders() -> dict[str, str]:
    bm20 = load_json(BM20_JSON)
    krw = load_json(KRW_JSON)
    df = load_daily_df()

    # BTC series (optional)
    btc_usd_txt = "—"
    btc_1d_html = "—"
    if BTC_JSON.exists():
        series = load_json(BTC_JSON)
        try:
            if isinstance(series, list) and len(series) >= 2:
                btc_last = float(series[-1].get("price", series[-1].get("close", 0)))
                btc_prev = float(series[-2].get("price", series[-2].get("close", 0)))
                if btc_last and btc_prev:
                    btc_1d = (btc_last / btc_prev - 1) * 100.0
                    btc_usd_txt = f"{btc_last:,.0f}"
                    btc_1d_html = colored_change_html(btc_1d, digits=2, wrap_parens=False)
        except Exception:
            # keep as "—"
            pass

    # BM20
    asof = bm20.get("asOf") or bm20.get("asof") or bm20.get("date") or bm20.get("timestamp") or ""
    level = bm20.get("bm20Level", None)
    r1d_raw = (bm20.get("returns", {}) or {}).get("1D", None)

    bm20_1d_pct = None
    bm20_1d_html = "—"
    direction = "보합"
    if r1d_raw is not None:
        bm20_1d_pct = pct_to_display(r1d_raw)
        bm20_1d_html = colored_change_html(bm20_1d_pct, digits=2, wrap_parens=False)
        if bm20_1d_pct > 0:
            direction = "반등"
        elif bm20_1d_pct < 0:
            direction = "약세"

    best3, worst3, breadth, up, down = compute_best_worst_breadth(df, n=3)
    move1, move2, move3 = compute_moves_top3(df)

    # Comment chip + comment
    if bm20_1d_pct is None:
        comment = f"BM20 보합, {breadth}"
        comment_chip = f'<span style="font-weight:900;color:{INK};">보합</span>'
    else:
        chip_color = GREEN if bm20_1d_pct > 0 else (RED if bm20_1d_pct < 0 else INK)
        comment_chip = f'<span style="font-weight:900;color:{chip_color};">{direction}</span>'
        comment = f"BM20 {direction}, {breadth}"

    # Kimchi
    kimchi_p = bm20.get("kimchi_premium_pct", None)
    kimchi_html = "—"
    if kimchi_p is not None:
        kimchi_pct = float(kimchi_p)
        kimchi_html = colored_change_html(kimchi_pct, digits=2, wrap_parens=False)

    usdkrw = (bm20.get("kimchi_meta", {}) or {}).get("usdkrw", None)
    usdkrw_txt = fmt_num(usdkrw, 2) if usdkrw is not None else "—"

    # KRW totals + shares
    ts_label = krw.get("timestamp_label", "")  # already KST label in generator
    totals = (krw.get("totals", {}) or {})
    combined = totals.get("combined_24h", None)
    upbit_v = totals.get("upbit_24h", None)
    bith_v = totals.get("bithumb_24h", None)
    coin_v = totals.get("coinone_24h", None)

    upbit_share = (float(upbit_v) / float(combined) * 100.0) if (combined and upbit_v is not None) else None
    bith_share = (float(bith_v) / float(combined) * 100.0) if (combined and bith_v is not None) else None
    coin_share = (float(coin_v) / float(combined) * 100.0) if (combined and coin_v is not None) else None

    krw_total_txt = fmt_krw_big(combined) if combined is not None else "—"

    # Sentiment 
    sentiment_label, sentiment_score = ("—", "—")
    hist_obj = load_json_optional(BM20_HISTORY_JSON)
    if hist_obj:
        try:
            # 이미지처럼 전체가 리스트인 경우 마지막(최신) 데이터 추출
            latest_entry = hist_obj[-1] if isinstance(hist_obj, list) else hist_obj.get("latest", hist_obj)
            
            # 이미지 4~6행의 'sentiment' 주머니 확인
            sent_data = latest_entry.get("sentiment", {})
            label = sent_data.get("status") or sent_data.get("sentiment_label")
            score = sent_data.get("value") or sent_data.get("sentiment_score")
            
            if label: sentiment_label = str(label)
            if score is not None: sentiment_score = f"{float(score):.0f}"
        except Exception:
            pass

    # Korea signals 
    xrp_kr_share = "—"
    xrp_obj = load_json_optional(XRP_KR_SHARE_JSON)
    if xrp_obj and isinstance(xrp_obj, dict):
        # 이미지에서 확인된 'k_xrp_share_pct_24h' 키를 사용
        v_xrp = xrp_obj.get("k_xrp_share_pct_24h")
        if v_xrp is not None:
            xrp_kr_share = fmt_share_pct(float(v_xrp))

    # 3. 한국 글로벌 점유율 (bm20_history.json의 k_market 대응)
    kr_share_global = "—"
    if hist_obj:
        try:
            latest_entry = hist_obj[-1] if isinstance(hist_obj, list) else hist_obj.get("latest", hist_obj)
            # 이미지 11행의 'k_share_percent' 사용
            v_global = latest_entry.get("k_market", {}).get("k_share_percent")
            if v_global is not None:
                kr_share_global = fmt_share_pct(float(v_global))
        except Exception:
            pass

    # 4. K-Safety (기존 KRW 데이터에서 추출)
    k_safety = "—"
    if isinstance(krw, dict):
        meta = krw.get("meta", {})
        # stablecoin_ratio 등 존재하는 키 탐색
        v_safe = krw.get("k_safety") or meta.get("k_safety") or \
                 krw.get("stablecoin_ratio") or meta.get("stablecoin_ratio")
        if v_safe is not None:
            k_safety = fmt_share_pct(float(v_safe))

    top10_conc = compute_top10_concentration(df)

    # News
    news_one_liner = load_text_first_line(NEWS_ONELINER_TXT)
    news_one_liner_note = load_text_first_line(NEWS_ONELINER_NOTE_TXT)
    top1, top2, top3 = load_top_news_3(TOP_NEWS_JSON)

    # Synth lines
    market_one_line = synth_market_one_line(direction, breadth, krw_total_txt, kimchi_html)
    treemap_one_line = synth_treemap_one_line(best3, worst3)

    # Sponsor defaults (minimal + safe)
    sponsor_click = "https://blockmedia.co.kr/kr"
    sponsor_banner = TRANSPARENT_GIF
    sponsor_copy = ""

    # Dashboard preview defaults
    dash_preview_img = "https://data.blockmedia.co.kr/assets/topcoins_treemap_latest.png"
    dash_preview_caption = "대시보드에서 BM20·KRW 수급·한국 시그널·트리맵을 한 번에 확인하세요."

    # AAS defaults (until you wire real generator)
    aas_defaults = {
        "{{AAS_COIN_1}}": "—",
        "{{AAS_SIGNAL_1}}": "—",
        "{{AAS_COLOR_1}}": MUTED,
        "{{AAS_NOTE_1}}": "—",
        "{{AAS_COIN_2}}": "—",
        "{{AAS_SIGNAL_2}}": "—",
        "{{AAS_COLOR_2}}": MUTED,
        "{{AAS_NOTE_2}}": "—",
        "{{AAS_COIN_3}}": "—",
        "{{AAS_SIGNAL_3}}": "—",
        "{{AAS_COLOR_3}}": MUTED,
        "{{AAS_NOTE_3}}": "—",
    }

    # SUBSCRIBE_URL (template has plain token, not {{...}})
    subscribe_url = "https://blockmedia.co.kr/kr"

    # Data request endpoint - you told me to delete. If template still has it, neutralize to news page.
    data_request_url = "https://blockmedia.co.kr/kr"

    ph = {
        # BM20
        "{{BM20_LEVEL}}": fmt_level(level) if level is not None else "—",
        "{{BM20_ASOF}}": str(asof)[:10] if asof else "—",
        "{{BM20_1D}}": bm20_1d_html,
        "{{BM20_BREADTH}}": breadth,
        "{{BM20_BEST3}}": best3,
        "{{BM20_WORST3}}": worst3,
        "{{BM20_COMMENT}}": comment,
        "{{BM20_CHIP}}": comment_chip,

        # BTC
        "{{BTC_USD}}": btc_usd_txt,
        "{{BTC_1D}}": btc_1d_html,

        # Sentiment
        "{{SENTIMENT_LABEL}}": sentiment_label,
        "{{SENTIMENT_SCORE}}": sentiment_score,

        # Market / Treemap one-liners
        "{{MARKET_ONE_LINE}}": market_one_line,
        "{{TREEMAP_ONE_LINE}}": treemap_one_line,

        # Moves
        "{{MOVE_1}}": move1,
        "{{MOVE_2}}": move2,
        "{{MOVE_3}}": move3,

        # Kimchi + USDKRW (not displayed in your pasted template, but safe if present elsewhere)
        "{{KIMCHI_PREM}}": kimchi_html,
        "{{USDKRW}}": usdkrw_txt,

        # KRW
        "{{KRW_TOTAL_24H}}": krw_total_txt,
        "{{KRW_ASOF_KST}}": ts_label if ts_label else (str(asof)[:10] if asof else "—"),
        "{{UPBIT_SHARE_24H}}": fmt_share_pct(upbit_share) if upbit_share is not None else "—",
        "{{BITHUMB_SHARE_24H}}": fmt_share_pct(bith_share) if bith_share is not None else "—",
        "{{COINONE_SHARE_24H}}": fmt_share_pct(coin_share) if coin_share is not None else "—",

        # Korea signals
        "{{KR_SHARE_GLOBAL}}": kr_share_global,
        "{{XRP_KR_SHARE}}": xrp_kr_share,
        "{{TOP10_CONC_24H}}": top10_conc,
        "{{K_SAFETY}}": k_safety,

        # News
        "{{NEWS_ONE_LINER}}": news_one_liner,
        "{{NEWS_ONE_LINER_NOTE}}": news_one_liner_note,
        "{{TOP_NEWS_1}}": top1,
        "{{TOP_NEWS_2}}": top2,
        "{{TOP_NEWS_3}}": top3,

        # Sponsor
        "{{SPONSOR_CLICK_URL}}": sponsor_click,
        "{{SPONSOR_BANNER_URL}}": sponsor_banner,
        "{{SPONSOR_COPY}}": sponsor_copy,

        # Dashboard preview
        "{{DASHBOARD_PREVIEW_IMG_URL}}": dash_preview_img,
        "{{DASHBOARD_PREVIEW_CAPTION}}": dash_preview_caption,
    }
    ph.update(aas_defaults)

    # ETF
    ph.update(load_etf_summary())

    # ── 실시간 데이터 주입 ──
    usdkrw_float = None
    try:
        usdkrw_float = float(str(usdkrw).replace(",", "")) if usdkrw else None
    except Exception:
        pass

    # Yahoo Finance 티커 (BTC·ETH·XRP)
    for k, v in fetch_yahoo_ticker().items():
        ph["{{" + k + "}}"] = v

    # BM20 티커
    if bm20_1d_pct is not None:
        arrow = "▲" if bm20_1d_pct >= 0 else "▼"
        ph["{{TICKER_BM20_CHANGE}}"] = f"{arrow}{abs(bm20_1d_pct):.1f}%"
        ph["{{TICKER_BM20_COLOR}}"]  = "ticker-up" if bm20_1d_pct >= 0 else "ticker-down"
    else:
        ph["{{TICKER_BM20_CHANGE}}"] = "—"
        ph["{{TICKER_BM20_COLOR}}"]  = "ticker-down"

    # 업비트 Top3 / Bottom3
    for k, v in fetch_upbit_top_bottom(n=3).items():
        ph["{{" + k + "}}"] = v

    # 김치 vs 코인베이스 프리미엄
    for k, v in fetch_premium_data(usdkrw_float).items():
        ph["{{" + k + "}}"] = v

    # also replace plain tokens (not wrapped)
    ph["SUBSCRIBE_URL"] = subscribe_url
    ph["https://data.blockmedia.co.kr/data-request?utm_source=newsletter&utm_medium=email&utm_campaign=daily_letter&utm_content=request"] = data_request_url

    return ph

def render() -> None:
    if not TEMPLATE.exists():
        raise FileNotFoundError(f"Missing {TEMPLATE}")

    html = TEMPLATE.read_text(encoding="utf-8")

    ph = build_placeholders()
    # Replace longer keys first to avoid partial replacement edge-cases
    for k in sorted(ph.keys(), key=len, reverse=True):
        html = html.replace(k, str(ph[k]))

    # Show any remaining {{VARS}} so CI logs tell you what you forgot to wire
    left = sorted(set(re.findall(r"\{\{[A-Z0-9_]+\}\}", html)))
    if left:
        print("WARN: Unfilled placeholders:", left)

    OUT.write_text(html, encoding="utf-8")
    print(f"OK: wrote {OUT}")

if __name__ == "__main__":
    render()
