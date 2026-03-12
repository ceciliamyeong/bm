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

TEMPLATE = ROOT / "letter_newsletter_template.html"  # 블록미디어 공식 뉴스레터 템플릿

BM20_JSON = ROOT / "bm20_latest.json"
DAILY_CSV = ROOT / "bm20_daily_data_latest.csv"
KRW_JSON = ROOT / "out/history/krw_24h_latest.json"
BTC_JSON = ROOT / "out/history/btc_usd_series.json"  # optional

BM20_HISTORY_JSON = ROOT / "data/bm20_history.json"  # optional
XRP_KR_SHARE_JSON = ROOT / "out/global/k_xrp_share_24h_latest.json"  # optional
ETF_JSON          = ROOT / "data/etf_summary.json"  # optional
KRW_SNAPSHOTS_JSON = ROOT / "out/history/krw_24h_snapshots.json"  # optional
NASDAQ_JSON       = ROOT / "nasdaq_series.json"  # optional
KOSPI_JSON        = ROOT / "kospi_series.json"   # optional

NEWS_ONELINER_TXT = ROOT / "out/latest/news_one_liner.txt"
NEWS_ONELINER_NOTE_TXT = ROOT / "out/latest/news_one_liner_note.txt"
TOP_NEWS_JSON = ROOT / "out/latest/top_news_latest.json"

# 워드프레스 설정
WP_BASE_URL                 = "https://blockmedia.co.kr/wp-json/wp/v2"
WP_TAG_NEWSLETTER           = "뉴스레터"       # ③ 왜 그랬어? — 기사 3개
WP_TAG_NEWSLETTER_LEAD      = "뉴스레터-리드"  # ① 어제 시장 어땠어? — 편집자 헤드라인 1개
WP_TAG_ID_NEWSLETTER        = 28978
WP_TAG_ID_NEWSLETTER_LEAD   = 80405

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
    """BTC·ETH·XRP 현재가 + 24h 변동률 (Yahoo Finance — yfinance)"""
    import yfinance as yf

    SYMBOLS = {"BTC-USD": "BTC", "ETH-USD": "ETH", "XRP-USD": "XRP"}
    fb = {"PRICE": "—", "CHANGE": "—", "COLOR": "ticker-down"}
    fallback = {
        **{f"TICKER_BTC_{k}": v for k, v in fb.items()},
        **{f"TICKER_ETH_{k}": v for k, v in fb.items()},
        **{f"TICKER_XRP_{k}": v for k, v in fb.items()},
        "TICKER_TIME": _kst_now(),
    }

    try:
        tickers = yf.Tickers(" ".join(SYMBOLS.keys()))
        result = {}
        for yf_sym, sym in SYMBOLS.items():
            try:
                info  = tickers.tickers[yf_sym].fast_info
                price = float(info.last_price)
                prev  = float(info.previous_close)
                chg   = (price - prev) / prev * 100 if prev else 0.0

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
            except Exception as e:
                print(f"WARN: Yahoo ticker {yf_sym} failed: {e}")
                result[f"TICKER_{sym}_PRICE"]  = "—"
                result[f"TICKER_{sym}_CHANGE"] = "—"
                result[f"TICKER_{sym}_COLOR"]  = "ticker-down"

        result["TICKER_TIME"] = _kst_now()
        print("INFO: Ticker via Yahoo Finance")
        return result

    except Exception as e:
        print(f"WARN: Yahoo Finance fetch failed: {e}")
        return fallback


# 하위 호환 alias (기존 호출부 변경 불필요)
fetch_coingecko_ticker = fetch_yahoo_ticker


def fmt_vol_krw(v: float) -> str:
    """거래대금 KRW 단위 포맷: 조/억 단위"""
    if v >= 1_000_000_000_000:
        return f"{v/1_000_000_000_000:.1f}조"
    if v >= 100_000_000:
        return f"{v/100_000_000:.0f}억"
    return f"{v:,.0f}"


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


def fetch_exchange_vol_top3() -> dict[str, str]:
    """업비트·빗썸·코인원 거래대금 Top3 — krw_24h_latest.json by_exchange_top 에서 읽기"""
    FB = {
        **{f"UPBIT_VOL{i}_SYM":   "—" for i in range(1, 4)},
        **{f"UPBIT_VOL{i}_AMT":   "—" for i in range(1, 4)},
        **{f"BITHUMB_VOL{i}_SYM": "—" for i in range(1, 4)},
        **{f"BITHUMB_VOL{i}_AMT": "—" for i in range(1, 4)},
        **{f"COINONE_VOL{i}_SYM": "—" for i in range(1, 4)},
        **{f"COINONE_VOL{i}_AMT": "—" for i in range(1, 4)},
    }
    try:
        krw = load_json_optional(KRW_JSON)
        if not krw:
            return FB
        by_ex = krw.get("by_exchange_top", {})

        mapping = [
            ("upbit_top5",   "UPBIT"),
            ("bithumb_top5", "BITHUMB"),
            ("coinone_top5", "COINONE"),
        ]
        result = {}
        for key, prefix in mapping:
            entries = by_ex.get(key, [])[:3]
            for i, entry in enumerate(entries, 1):
                sym = entry.get("symbol", "—").replace("KRW-", "")
                val = float(entry.get("value", 0))
                result[f"{prefix}_VOL{i}_SYM"] = sym
                result[f"{prefix}_VOL{i}_AMT"] = fmt_vol_krw(val)
        FB.update(result)
    except Exception as e:
        print(f"WARN: exchange vol top3 failed: {e}")
    return FB


def fetch_premium_data(usdkrw: float | None) -> dict[str, str]:
    """김치 프리미엄 vs 코인베이스 프리미엄 계산"""
    FB = {"KIMCHI_PREM_PCT": "—", "CB_PREMIUM_PCT": "—",
          "PREMIUM_COMMENT": "프리미엄 데이터를 가져올 수 없습니다."}
    try:
        upbit_btc_krw = float(
            requests.get("https://api.upbit.com/v1/ticker",
                         params={"markets":"KRW-BTC"}, timeout=10).json()[0]["trade_price"])
        # Yahoo Finance로 BTC USD 기준가 조회
        import yfinance as yf
        yf_btc = yf.Ticker("BTC-USD").fast_info
        cg_usd = float(yf_btc.last_price)
        fx = usdkrw if (usdkrw and usdkrw > 100) else 1350.0  # 환율 힌트 없으면 하드코딩 폴백
        cb_usd = float(
            requests.get("https://api.coinbase.com/v2/prices/BTC-USD/spot", timeout=10).json()["data"]["amount"])
        upbit_usd  = upbit_btc_krw / fx
        kimchi_pct = (upbit_usd - cg_usd) / cg_usd * 100  # 한국 vs 글로벌
        cb_pct     = (cb_usd - cg_usd) / cg_usd * 100     # 미국(코베) vs 글로벌

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



# ─────────────────────────────────────────────────────────
# 워드프레스 REST API: 태그 기반 뉴스 수집
# ─────────────────────────────────────────────────────────

def _wp_get_tag_id(tag_name: str) -> int | None:
    """태그 이름으로 워드프레스 태그 ID 조회"""
    try:
        res = requests.get(
            f"{WP_BASE_URL}/tags",
            params={"search": tag_name, "per_page": 5},
            timeout=10,
        )
        res.raise_for_status()
        for t in res.json():
            if t.get("name") == tag_name:
                return int(t["id"])
        print(f"WARN: WP tag '{tag_name}' not found")
    except Exception as e:
        print(f"WARN: WP tag lookup failed ({tag_name}): {e}")
    return None


def _strip_html(text: str) -> str:
    """HTML 태그 제거 + 공백 정리"""
    import re as _re
    return _re.sub(r"<[^>]+>", "", text or "").strip()


def fetch_wp_newsletter_lead() -> dict[str, str]:
    """
    태그 '뉴스레터-리드' (ID: 80405) 최신 포스트 1개에서
    NEWS_HEADLINE, NEWS_ONE_LINER_NOTE 수집.
    없으면 '뉴스레터' (ID: 28978) 최신 1개로 fallback — 오류 없이 계속 진행.
    """
    FB = {
        "NEWS_HEADLINE": "—",
        "NEWS_ONE_LINER_NOTE": "—",
    }

    def _parse_post(post: dict) -> dict[str, str]:
        # excerpt 사용 — 기자 이름 없이 깔끔한 발췌문
        excerpt = _strip_html(post["excerpt"]["rendered"])
        if len(excerpt) > 150:
            excerpt = excerpt[:150].rstrip() + "…"
        return {
            "NEWS_HEADLINE":       _strip_html(post["title"]["rendered"]),
            "NEWS_ONE_LINER_NOTE": excerpt,
        }

    # 1차: 뉴스레터-리드 시도
    try:
        res = requests.get(
            f"{WP_BASE_URL}/posts",
            params={"tags": WP_TAG_ID_NEWSLETTER_LEAD, "per_page": 1, "orderby": "date", "status": "publish"},
            timeout=10,
        )
        res.raise_for_status()
        posts = res.json()
        if posts:
            print("INFO: 뉴스레터-리드 포스트 사용")
            return _parse_post(posts[0])
        print("WARN: 뉴스레터-리드 포스트 없음 → 뉴스레터 최신 1개로 fallback")
    except Exception as e:
        print(f"WARN: 뉴스레터-리드 fetch 실패: {e} → fallback 시도")

    # 2차: 뉴스레터 최신 1개 fallback
    try:
        res = requests.get(
            f"{WP_BASE_URL}/posts",
            params={"tags": WP_TAG_ID_NEWSLETTER, "per_page": 1, "orderby": "date", "status": "publish"},
            timeout=10,
        )
        res.raise_for_status()
        posts = res.json()
        if posts:
            print("INFO: 뉴스레터 최신 1개로 헤드라인 대체")
            return _parse_post(posts[0])
        print("WARN: 뉴스레터 포스트도 없음 → 기본값 사용")
    except Exception as e:
        print(f"WARN: 뉴스레터 fallback fetch 실패: {e}")

    return FB


def fetch_wp_newsletter_news() -> list[dict[str, str]]:
    """
    태그 '뉴스레터' (ID: 28978) 최신 포스트 3개에서
    title, excerpt, link, category 수집
    """
    empty = {"title": "—", "excerpt": "", "link": "#", "category": ""}
    try:
        tag_id = WP_TAG_ID_NEWSLETTER

        res = requests.get(
            f"{WP_BASE_URL}/posts",
            params={"tags": tag_id, "per_page": 3, "orderby": "date", "status": "publish", "_embed": 1},
            timeout=10,
        )
        res.raise_for_status()
        posts = res.json()

        if len(posts) < 3:
            raise ValueError(f"'{WP_TAG_NEWSLETTER}' 태그 발행 포스트가 {len(posts)}개뿐입니다. 3개 필요.")

        result = []
        for post in posts[:3]:
            # 카테고리명 추출 (_embed 사용)
            try:
                cats = post.get("_embedded", {}).get("wp:term", [[]])[0]
                cat_name = cats[0]["name"] if cats else ""
            except Exception:
                cat_name = ""

            result.append({
                "title":    _strip_html(post["title"]["rendered"]),
                "excerpt":  _strip_html(post["excerpt"]["rendered"]),
                "link":     post.get("link", "#"),
                "category": cat_name,
            })
        return result

    except ValueError as e:
        print(f"ERROR: {e}")
        raise
    except Exception as e:
        print(f"WARN: fetch_wp_newsletter_news failed: {e}")
        return [empty, empty, empty]

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

    def _fmt_usd(val, digits=0) -> str:
        """숫자 → 억달러 단위 포맷"""
        try:
            v = float(val)
        except Exception:
            return "—"
        billions = v / 1_000_000_000
        if abs(billions) >= 1:
            return f"${billions:+.1f}B" if digits == 0 else f"${billions:.1f}B"
        millions = v / 1_000_000
        return f"${millions:+.0f}M"

    def _fmt_aum(val) -> str:
        try:
            v = float(val)
            b = v / 1_000_000_000
            return f"${b:.1f}B"
        except Exception:
            return "—"

    def _fmt_holdings(val, sym) -> str:
        try:
            v = float(val)
            return f"{v:,.0f} {sym}"
        except Exception:
            return "—"

    def _inflow_color(val) -> str:
        try:
            v = float(val)
            if v > 0:  return f"color:#16a34a;font-weight:900;"
            if v < 0:  return f"color:#dc2626;font-weight:900;"
        except Exception:
            pass
        return "color:#64748b;"

    def _parse(coin: str, sym: str) -> dict:
        d = raw.get(coin, {})
        inflow_raw = d.get("dailyNetInflow", None)
        return {
            f"{{{{ETF_{sym}_INFLOW}}}}":       _fmt_usd(inflow_raw),
            f"{{{{ETF_{sym}_AUM}}}}":           _fmt_aum(d.get("totalNetAssets")),
            f"{{{{ETF_{sym}_CUM}}}}":           _fmt_usd(d.get("cumNetInflow"), digits=0),
            f"{{{{ETF_{sym}_HOLDINGS}}}}":      _fmt_holdings(d.get("totalTokenHoldings"), sym),
            f"{{{{ETF_{sym}_INFLOW_COLOR}}}}":  _inflow_color(inflow_raw),
        }

    result = {}
    result.update(_parse("btc", "BTC"))
    result.update(_parse("eth", "ETH"))
    result.update(_parse("sol", "SOL"))

    # ETF 코멘트 자동 생성
    try:
        btc_v = float(raw.get("btc", {}).get("dailyNetInflow", 0))
        eth_v = float(raw.get("eth", {}).get("dailyNetInflow", 0))
        if btc_v > 0 and eth_v > 0:
            comment = f"BTC·ETH ETF 동시 순유입 — 기관 수급 전반적 우호."
        elif btc_v > 0 and eth_v <= 0:
            comment = f"BTC ETF 순유입, ETH 소폭 유출 — BTC 집중 매수 구간."
        elif btc_v < 0 and eth_v < 0:
            comment = f"BTC·ETH ETF 동시 순유출 — 기관 단기 차익실현 신호."
        else:
            comment = f"ETF 혼조세 — 방향성 확인 필요."
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

def load_top_news_3(p: Path):
    """Returns list of 3 dicts: {title, excerpt, link, category}"""
    empty = {"title": "—", "excerpt": "", "link": "#", "category": ""}
    if not p.exists():
        return [empty, empty, empty]
    try:
        obj = json.loads(p.read_text(encoding="utf-8"))
        items = obj.get("items", []) if isinstance(obj, dict) else (obj or [])
        result = []
        for x in items[:3]:
            if isinstance(x, dict):
                result.append({
                    "title":    x.get("title", "—") or "—",
                    "excerpt":  x.get("excerpt", "") or "",
                    "link":     x.get("link", "#") or "#",
                    "category": x.get("category", "") or "",
                })
            elif isinstance(x, str) and x.strip():
                result.append({**empty, "title": x.strip()})
        while len(result) < 3:
            result.append(empty)
        return result
    except Exception:
        return [empty, empty, empty]

# ------------------ formatting helpers ------------------

def fmt_level(x: float) -> str:
    return f"{float(x):,.2f}"

def fmt_num(x: float, digits: int = 2) -> str:
    return f"{float(x):,.{digits}f}"

def fmt_share_pct(x: float) -> str:
    x = float(x)
    # 0~1 비율로 들어오면 100 곱하기 (0.016 같은 경우)
    # 1~100 범위면 이미 % 단위
    if abs(x) < 1.0:
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

# ------------------ index series + krw snapshots helpers ------------------

def load_krw_snapshots_top10() -> str:
    """out/history/krw_24h_snapshots.json 에서 top10 집중도"""
    try:
        import json as _json
        raw = _json.loads(KRW_SNAPSHOTS_JSON.read_text(encoding="utf-8")) if KRW_SNAPSHOTS_JSON.exists() else None
        if raw is None:
            return "—"
        item = raw[-1] if isinstance(raw, list) else raw
        pct = item.get("top10", {}).get("top10_share_pct")
        if pct is not None:
            return f"{float(pct):.1f}%"
    except Exception:
        pass
    return "—"


def load_index_series_1d(path: Path) -> str:
    """[{date, price}] 배열 마지막 두 항목으로 1D 등락 계산"""
    try:
        import json as _json
        if not path.exists():
            return "—"
        raw = _json.loads(path.read_text(encoding="utf-8"))
        if not raw or len(raw) < 2:
            return "—"
        prev = float(raw[-2]["price"])
        curr = float(raw[-1]["price"])
        if prev <= 0:
            return "—"
        chg = (curr - prev) / prev * 100
        arrow = "▲" if chg >= 0 else "▼"
        return f"{arrow}{abs(chg):.2f}%"
    except Exception:
        return "—"


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

def fetch_aas_data() -> dict[str, str]:
    """GitHub에서 AAS 데이터를 가져와 실 JSON 키값(대문자 시작)에 맞춰 가공"""
    kst_now = datetime.now(timezone(timedelta(hours=9)))
    date_str = kst_now.strftime("%Y-%m-%d")
    
    # GitHub Raw URL
    url = f"https://raw.githubusercontent.com/Blockmedia-DataTeam/AAS-Bot/main/reports/daily/{date_str}/newsletter_aas_top3_{date_str}.json"
    
    ph = {}
    # 기본값 설정 (데이터 호출 실패 시 레이아웃 유지용)
    for i in range(1, 4):
        ph.update({
            f"{{{{AAS_COIN_{i}}}}}" : "—",
            f"{{{{AAS_SCORE_{i}}}}}" : "0.00",
            f"{{{{AAS_SCORE_PERCENT_{i}}}}}" : "0",
            f"{{{{AAS_CHG_{i}}}}}" : "0.00",
            f"{{{{AAS_NOTE_{i}}}}}" : "—",
            f"{{{{AAS_ONCHAIN_{i}}}}}" : "33.3",
            f"{{{{AAS_SOCIAL_{i}}}}}" : "33.3",
            f"{{{{AAS_MOMENTUM_{i}}}}}" : "33.4",
        })

    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        
        for i, item in enumerate(data[:3], 1):
            # JSON의 실제 키값(Symbol, AAS, 24H(%), Comment) 반영
            score = float(item.get("AAS", 0))
            score_pct = min(100, int((score / 3.0) * 100))
            
            ph[f"{{{{AAS_COIN_{i}}}}}"] = item.get("Symbol", "—")
            ph[f"{{{{AAS_SCORE_{i}}}}}"] = f"{score:.2f}"
            ph[f"{{{{AAS_SCORE_PERCENT_{i}}}}}"] = str(score_pct)
            ph[f"{{{{AAS_CHG_{i}}}}}"] = f"{float(item.get('24H(%)', 0)):+.2f}"
            ph[f"{{{{AAS_NOTE_{i}}}}}"] = item.get("Comment", "—")
            
            # 기여도 차트용 데이터 (평면 구조 반영)
            ph[f"{{{{AAS_ONCHAIN_{i}}}}}"] = str(item.get("Onchain", 33.3))
            ph[f"{{{{AAS_SOCIAL_{i}}}}}"] = str(item.get("Social", 33.3))
            ph[f"{{{{AAS_MOMENTUM_{i}}}}}"] = str(item.get("Momentum", 33.4))
            
        print(f"INFO: AAS data successfully matched for {date_str}")
    except Exception as e:
        print(f"WARN: AAS Fetch failed ({url}): {e}")
        
    return ph

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
        except Exception: pass

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
        if bm20_1d_pct > 0: direction = "반등"
        elif bm20_1d_pct < 0: direction = "약세"

    best3, worst3, breadth, up, down = compute_best_worst_breadth(df, n=3)
    move1, move2, move3 = compute_moves_top3(df)

    # Comment chip
    chip_color = GREEN if (bm20_1d_pct or 0) > 0 else (RED if (bm20_1d_pct or 0) < 0 else INK)
    comment_chip = f'<span style="font-weight:900;color:{chip_color};">{direction}</span>'
    comment = f"BM20 {direction}, {breadth}"

    # Kimchi & KRW
    kimchi_p = bm20.get("kimchi_premium_pct", None)
    kimchi_html = colored_change_html(float(kimchi_p)) if kimchi_p is not None else "—"
    usdkrw = (bm20.get("kimchi_meta", {}) or {}).get("usdkrw", None)
    usdkrw_txt = fmt_num(usdkrw, 2) if usdkrw is not None else "—"

    totals = (krw.get("totals", {}) or {})
    combined = totals.get("combined_24h", None)
    krw_total_txt = fmt_krw_big(combined) if combined is not None else "—"
    
    upbit_v, bith_v, coin_v = totals.get("upbit_24h"), totals.get("bithumb_24h"), totals.get("coinone_24h")
    upbit_share = (float(upbit_v)/float(combined)*100) if combined and upbit_v else None
    bith_share = (float(bith_v)/float(combined)*100) if combined and bith_v else None
    coin_share = (float(coin_v)/float(combined)*100) if combined and coin_v else None

    # Sentiment & Korea Signals
    sentiment_label, sentiment_score = ("—", "—")
    hist_obj = load_json_optional(BM20_HISTORY_JSON)
    if hist_obj:
        try:
            latest_entry = hist_obj[-1] if isinstance(hist_obj, list) else hist_obj.get("latest", hist_obj)
            sent_data = latest_entry.get("sentiment", {})
            sentiment_label = str(sent_data.get("status") or sent_data.get("sentiment_label") or "—")
            score = sent_data.get("value") or sent_data.get("sentiment_score")
            if score is not None: sentiment_score = f"{float(score):.0f}"
        except Exception: pass

    # News
    wp_lead = fetch_wp_newsletter_lead()
    news3 = fetch_wp_newsletter_news()
    top1, top2, top3 = news3[0], news3[1], news3[2]

    # Global Index
    nasdaq_1d = load_index_series_1d(NASDAQ_JSON)
    kospi_1d  = load_index_series_1d(KOSPI_JSON)

    # SUBSCRIBE URL
    subscribe_url = "https://blockmedia.co.kr/kr"

    ph = {
        "{{BM20_LEVEL}}": fmt_level(level) if level is not None else "—",
        "{{BM20_ASOF}}": str(asof)[:10] if asof else "—",
        "{{BM20_1D}}": bm20_1d_html,
        "{{BM20_BREADTH}}": breadth,
        "{{BM20_COMMENT}}": comment,
        "{{BM20_CHIP}}": comment_chip,
        "{{BTC_USD}}": btc_usd_txt,
        "{{BTC_1D}}": btc_1d_html,
        "{{SENTIMENT_LABEL}}": sentiment_label,
        "{{SENTIMENT_SCORE}}": sentiment_score,
        "{{MARKET_ONE_LINE}}": synth_market_one_line(direction, breadth, krw_total_txt, kimchi_html),
        "{{TREEMAP_ONE_LINE}}": synth_treemap_one_line(best3, worst3),
        "{{MOVE_1}}": move1, "{{MOVE_2}}": move2, "{{MOVE_3}}": move3,
        "{{KRW_TOTAL_24H}}": krw_total_txt,
        "{{KRW_ASOF_KST}}": (str(asof)[:10] if asof else "—"),
        "{{UPBIT_SHARE_24H}}": fmt_share_pct(upbit_share) if upbit_share else "—",
        "{{BITHUMB_SHARE_24H}}": fmt_share_pct(bith_share) if bith_share else "—",
        "{{COINONE_SHARE_24H}}": fmt_share_pct(coin_share) if coin_share else "—",
        "{{NASDAQ_1D}}": nasdaq_1d,
        "{{KOSPI_1D}}": kospi_1d,
        "{{LETTER_DATE}}": datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d"),
        "{{NEWS_HEADLINE}}": wp_lead["NEWS_HEADLINE"],
        "{{NEWS_ONE_LINER_NOTE}}": wp_lead["NEWS_ONE_LINER_NOTE"],
        "{{TOP_NEWS_1}}": top1["title"], "{{TOP_NEWS_2}}": top2["title"], "{{TOP_NEWS_3}}": top3["title"],
        "{{NEWS1_EXCERPT}}": top1["excerpt"], "{{NEWS2_EXCERPT}}": top2["excerpt"], "{{NEWS3_EXCERPT}}": top3["excerpt"],
        "{{NEWS1_LINK}}": top1["link"], "{{NEWS2_LINK}}": top2["link"], "{{NEWS3_LINK}}": top3["link"],
        "{{NEWS1_CATEGORY}}": top1["category"], "{{NEWS2_CATEGORY}}": top2["category"], "{{NEWS3_CATEGORY}}": top3["category"],
    }

    # 🚀 AAS 데이터 업데이트 (여기서 BONK, PEPE 데이터가 주입됩니다)
    ph.update(fetch_aas_data())

    # ETF & 실시간 티커 데이터 업데이트
    ph.update(load_etf_summary())
    usdkrw_f = float(str(usdkrw).replace(",", "")) if usdkrw else None
    for k, v in fetch_yahoo_ticker().items(): ph["{{" + k + "}}"] = v
    for k, v in fetch_upbit_top_bottom(n=3).items(): ph["{{" + k + "}}"] = v
    for k, v in fetch_exchange_vol_top3().items(): ph["{{" + k + "}}"] = v
    for k, v in fetch_premium_data(usdkrw_f).items(): ph["{{" + k + "}}"] = v

    ph["SUBSCRIBE_URL"] = subscribe_url
    return ph

def render() -> None:
    if not TEMPLATE.exists():
        raise FileNotFoundError(f"Missing {TEMPLATE}")
    html = TEMPLATE.read_text(encoding="utf-8")
    ph = build_placeholders()
    # 긴 키부터 치환 (겹침 방지)
    for k in sorted(ph.keys(), key=len, reverse=True):
        html = html.replace(k, str(ph[k]))
    
    left = sorted(set(re.findall(r"\{\{[A-Z0-9_]+\}\}", html)))
    if left: print("WARN: Unfilled placeholders:", left)
    
    OUT.write_text(html, encoding="utf-8")
    print(f"OK: wrote {OUT}")

if __name__ == "__main__":
    render()

