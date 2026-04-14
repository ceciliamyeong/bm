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
- data/bm20_history.json        (optional; sentiment)
- data/etf_summary.json         (optional; ETF)

Output
- letter.html
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Tuple

import pandas as pd
import requests
from datetime import datetime, timezone, timedelta

ROOT = Path(__file__).resolve().parent.parent

TEMPLATE      = ROOT / "letter_newsletter_template.html"
BM20_JSON     = ROOT / "bm20_latest.json"
DAILY_CSV     = ROOT / "bm20_daily_data_latest.csv"
KRW_JSON      = ROOT / "out/history/krw_24h_latest.json"
BM20_HISTORY_JSON = ROOT / "data/bm20_history.json"   # optional
ETF_JSON      = ROOT / "data/etf_summary.json"         # optional
OUT           = ROOT / "letter.html"

# 워드프레스 설정
WP_BASE_URL               = "https://blockmedia.co.kr/wp-json/wp/v2"
WP_TAG_ID_NEWSLETTER      = 28978
WP_TAG_ID_NEWSLETTER_LEAD = 80405

GREEN = "#16a34a"
RED   = "#dc2626"
INK   = "#0f172a"


# ─────────────────────────────────────────────────────────
# 포맷 헬퍼
# ─────────────────────────────────────────────────────────

def fmt_level(x: float) -> str:
    return f"{float(x):,.2f}"

def pct_to_display(x: float) -> float:
    """비율(≤1.5) 또는 퍼센트 → 퍼센트 숫자 반환"""
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


# ─────────────────────────────────────────────────────────
# JSON / CSV 로더
# ─────────────────────────────────────────────────────────

def load_json(p: Path) -> Any:
    if not p.exists():
        raise FileNotFoundError(f"Missing {p}")
    return json.loads(p.read_text(encoding="utf-8"))

def load_json_optional(p: Path) -> Any | None:
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))

def load_daily_df() -> pd.DataFrame:
    if not DAILY_CSV.exists():
        raise FileNotFoundError(f"Missing {DAILY_CSV}")
    df = pd.read_csv(DAILY_CSV)
    if "symbol" not in df.columns:
        for c in ("ticker", "asset"):
            if c in df.columns:
                df = df.rename(columns={c: "symbol"})
                break
    if "price_change_pct" not in df.columns:
        for c in ("change_pct", "pct_change", "return_1d_pct", "return_1d"):
            if c in df.columns:
                df = df.rename(columns={c: "price_change_pct"})
                break
    df["symbol"] = df["symbol"].astype(str).str.upper()
    df["price_change_pct"] = pd.to_numeric(df["price_change_pct"], errors="coerce")
    df = df.dropna(subset=["price_change_pct"])
    return df


# ─────────────────────────────────────────────────────────
# BTC 가격 + 24h 변동률: CMC → Yahoo API fallback
# ─────────────────────────────────────────────────────────

def fetch_btc_price_and_change() -> Tuple[str, str]:
    """(btc_usd_txt, btc_1d_html) 반환"""
    cmc_key = os.getenv("CMC_API_KEY", "")
    if cmc_key:
        try:
            r = requests.get(
                "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest",
                headers={"X-CMC_PRO_API_KEY": cmc_key},
                params={"symbol": "BTC", "convert": "USD"},
                timeout=10,
            )
            r.raise_for_status()
            d = r.json()["data"]["BTC"]["quote"]["USD"]
            price = float(d["price"])
            chg   = float(d["percent_change_24h"])
            print(f"INFO: BTC from CMC — ${price:,.0f} / {chg:+.2f}%")
            return f"{price:,.0f}", colored_change_html(chg, digits=2)
        except Exception as e:
            print(f"WARN: CMC BTC fetch failed: {e} → Yahoo API fallback")

    # Yahoo Finance API fallback
    try:
        r2 = requests.get(
            "https://query1.finance.yahoo.com/v8/finance/chart/BTC-USD",
            headers={"User-Agent": "Mozilla/5.0"},
            params={"interval": "1d", "range": "5d"},
            timeout=10,
        )
        r2.raise_for_status()
        closes = r2.json()["chart"]["result"][0]["indicators"]["quote"][0]["close"]
        closes = [x for x in closes if x is not None]
        price  = float(closes[-1])
        prev   = float(closes[-2])
        chg    = (price / prev - 1) * 100 if prev else 0.0
        print(f"INFO: BTC from Yahoo API fallback — ${price:,.0f} / {chg:+.2f}%")
        return f"{price:,.0f}", colored_change_html(chg, digits=2)
    except Exception as e2:
        print(f"WARN: Yahoo API BTC fallback failed: {e2}")
        return "—", "—"


# ─────────────────────────────────────────────────────────
# 업비트 Top/Bottom 3
# ─────────────────────────────────────────────────────────

def fetch_upbit_top_bottom(n: int = 3) -> dict[str, str]:
    FB = {**{f"UPBIT_TOP{i}_SYMBOL": "—" for i in range(1, n+1)},
          **{f"UPBIT_TOP{i}_CHG":    "—" for i in range(1, n+1)},
          **{f"UPBIT_BOT{i}_SYMBOL": "—" for i in range(1, n+1)},
          **{f"UPBIT_BOT{i}_CHG":    "—" for i in range(1, n+1)}}
    try:
        mkts = [m["market"] for m in
                requests.get("https://api.upbit.com/v1/market/all",
                             params={"isDetails": "false"}, timeout=10).json()
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


# ─────────────────────────────────────────────────────────
# 김치·코인베이스 프리미엄
# ─────────────────────────────────────────────────────────

def fetch_premium_data(usdkrw: float | None) -> dict[str, str]:
    FB = {
        "KIMCHI_PREM_PCT":  "—",
        "CB_PREMIUM_PCT":   "—",
        "PREMIUM_COMMENT":  "프리미엄 데이터를 가져올 수 없습니다.",
        "PREMIUM_ASOF":     "—",
    }
    try:
        upbit_btc_krw = float(
            requests.get("https://api.upbit.com/v1/ticker",
                         params={"markets": "KRW-BTC"}, timeout=10).json()[0]["trade_price"])
        # 글로벌 BTC 기준가: Yahoo Finance API 직접 호출 (yfinance 라이브러리보다 안정적)
        _yf_r = requests.get(
            "https://query1.finance.yahoo.com/v8/finance/chart/BTC-USD",
            headers={"User-Agent": "Mozilla/5.0"},
            params={"interval": "1d", "range": "1d"},
            timeout=10,
        )
        _yf_r.raise_for_status()
        cg_usd = float(_yf_r.json()["chart"]["result"][0]["meta"]["regularMarketPrice"])
        print(f"INFO: Premium BTC global = ${cg_usd:,.0f} (Yahoo API)")
        fx = usdkrw if (usdkrw and usdkrw > 100) else 1500.0
        cb_usd = float(
            requests.get("https://api.coinbase.com/v2/prices/BTC-USD/spot",
                         timeout=10).json()["data"]["amount"])
        upbit_usd  = upbit_btc_krw / fx
        kimchi_pct = (upbit_usd - cg_usd) / cg_usd * 100
        cb_pct     = (cb_usd   - cg_usd) / cg_usd * 100

        def _c(v: float) -> str:
            color = GREEN if v >= 0 else RED
            sign  = "+" if v >= 0 else "-"
            return f'<span style="color:{color};font-weight:900;">{sign}{abs(v):.2f}%</span>'

        if kimchi_pct > 1 and cb_pct > 0:
            comment = "김치·코인베이스 프리미엄 동시 양전 → 글로벌 대비 국내 수요 강세 신호."
        elif kimchi_pct > 1 and cb_pct <= 0:
            comment = "김치 프리미엄 양전, 코인베이스 디스카운트 → 국내 단독 매수세 주의."
        elif kimchi_pct < -0.5:
            comment = "김치 역프리미엄 → 국내 매도 압력 또는 원화 약세 영향 가능성."
        else:
            comment = f"김치 {kimchi_pct:+.2f}% / 코인베이스 {cb_pct:+.2f}% — 중립 구간."

        kst = datetime.now(timezone(timedelta(hours=9)))
        asof = (f"{kst.month}월 {kst.day}일 "
                f"{'오전' if kst.hour < 12 else '오후'} "
                f"{kst.hour if kst.hour <= 12 else kst.hour - 12}시 {kst.minute:02d}분 기준")

        return {"KIMCHI_PREM_PCT": _c(kimchi_pct), "CB_PREMIUM_PCT": _c(cb_pct),
                "PREMIUM_COMMENT": comment, "PREMIUM_ASOF": asof}
    except Exception as e:
        print(f"WARN: Premium fetch failed: {e}")
        return FB


# ─────────────────────────────────────────────────────────
# 워드프레스 REST API
# ─────────────────────────────────────────────────────────

def _strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", "", text or "").strip()


def fetch_wp_newsletter_lead() -> dict[str, str]:
    FB = {"NEWS_HEADLINE": "—", "NEWS_ONE_LINER_NOTE": "—"}

    def _parse(post: dict) -> dict[str, str]:
        excerpt = _strip_html(post["excerpt"]["rendered"])
        if len(excerpt) > 150:
            excerpt = excerpt[:150].rstrip() + "…"
        return {
            "NEWS_HEADLINE":       _strip_html(post["title"]["rendered"]),
            "NEWS_ONE_LINER_NOTE": excerpt,
        }

    # 1차: 뉴스레터-리드
    try:
        res = requests.get(
            f"{WP_BASE_URL}/posts",
            params={"tags": WP_TAG_ID_NEWSLETTER_LEAD, "per_page": 1,
                    "orderby": "date", "status": "publish"},
            timeout=10,
        )
        res.raise_for_status()
        posts = res.json()
        if posts:
            print("INFO: 뉴스레터-리드 포스트 사용")
            return _parse(posts[0])
        print("WARN: 뉴스레터-리드 없음 → fallback")
    except Exception as e:
        print(f"WARN: 뉴스레터-리드 fetch 실패: {e}")

    # 2차: 뉴스레터 최신 1개 fallback
    try:
        res = requests.get(
            f"{WP_BASE_URL}/posts",
            params={"tags": WP_TAG_ID_NEWSLETTER, "per_page": 1,
                    "orderby": "date", "status": "publish"},
            timeout=10,
        )
        res.raise_for_status()
        posts = res.json()
        if posts:
            print("INFO: 뉴스레터 최신 1개로 헤드라인 대체")
            return _parse(posts[0])
    except Exception as e:
        print(f"WARN: 뉴스레터 fallback fetch 실패: {e}")

    return FB


def fetch_wp_newsletter_news() -> list[dict[str, str]]:
    empty = {"title": "—", "excerpt": "", "link": "#", "category": ""}
    try:
        res = requests.get(
            f"{WP_BASE_URL}/posts",
            params={"tags": WP_TAG_ID_NEWSLETTER, "per_page": 3,
                    "orderby": "date", "status": "publish",
                    "_embed": 1, "_fields": "id,title,excerpt,link,_embedded,meta"},
            timeout=10,
        )
        res.raise_for_status()
        posts = res.json()
        if len(posts) < 3:
            raise ValueError(f"뉴스레터 태그 발행 포스트가 {len(posts)}개뿐. 3개 필요.")

        def _get_summary(post: dict) -> str:
            meta = post.get("meta", {}) or {}
            summary = meta.get("bm_post_summary", "")
            if summary and summary.strip():
                s = summary.strip()
                return s[:150].rstrip() + "…" if len(s) > 150 else s
            try:
                r2 = requests.get(f"{WP_BASE_URL}/posts/{post['id']}",
                                   params={"_fields": "meta"}, timeout=8)
                summary2 = (r2.json().get("meta", {}) or {}).get("bm_post_summary", "")
                if summary2 and summary2.strip():
                    s2 = summary2.strip()
                    return s2[:150].rstrip() + "…" if len(s2) > 150 else s2
            except Exception as e:
                print(f"WARN: bm_post_summary 개별요청 실패 (post {post.get('id')}): {e}")
            excerpt = _strip_html(post["excerpt"]["rendered"])
            return excerpt[:150].rstrip() + "…" if len(excerpt) > 150 else excerpt

        result = []
        for post in posts[:3]:
            try:
                cats = post.get("_embedded", {}).get("wp:term", [[]])[0]
                cat_name = cats[0]["name"] if cats else ""
            except Exception:
                cat_name = ""
            result.append({
                "title":    _strip_html(post["title"]["rendered"]),
                "excerpt":  _get_summary(post),
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


# ─────────────────────────────────────────────────────────
# ETF 요약 (INFLOW + AUM만 — BTC/ETH/SOL)
# ─────────────────────────────────────────────────────────

def load_etf_summary() -> dict[str, str]:
    FB = {
        "{{ETF_BTC_INFLOW}}": "—", "{{ETF_BTC_AUM}}": "—",
        "{{ETF_ETH_INFLOW}}": "—", "{{ETF_ETH_AUM}}": "—",
        "{{ETF_SOL_INFLOW}}": "—", "{{ETF_SOL_AUM}}": "—",
    }
    if not ETF_JSON.exists():
        print(f"WARN: ETF json not found: {ETF_JSON}")
        return FB
    try:
        raw = json.loads(ETF_JSON.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"WARN: ETF json parse error: {e}")
        return FB

    def _fmt_aum(val) -> str:
        try:
            return f"${float(val) / 1_000_000_000:.1f}B"
        except Exception:
            return "—"

    def _inflow_html(val) -> str:
        try:
            v = float(val)
            billions = v / 1_000_000_000
            millions = v / 1_000_000
            text = f"${billions:+.1f}B" if abs(billions) >= 1 else f"${millions:+.0f}M"
            color = GREEN if v > 0 else (RED if v < 0 else "#64748b")
        except Exception:
            return "—"
        return f'<span style="color:{color};font-weight:900;">{text}</span>'

    def _parse(coin: str, sym: str) -> dict:
        d = raw.get(coin, {})
        return {
            f"{{{{ETF_{sym}_INFLOW}}}}": _inflow_html(d.get("dailyNetInflow")),
            f"{{{{ETF_{sym}_AUM}}}}":    _fmt_aum(d.get("totalNetAssets")),
        }

    result = {}
    result.update(_parse("btc", "BTC"))
    result.update(_parse("eth", "ETH"))
    result.update(_parse("sol", "SOL"))
    return result


# ─────────────────────────────────────────────────────────
# AAS (코생지) Top3
# ─────────────────────────────────────────────────────────

def _aas_bar_html(onchain: float, social: float, momentum: float) -> str:
    segs = [(onchain, "#2563eb"), (social, "#f97316"), (momentum, "#16a34a")]
    tds = ""
    for pct, color in segs:
        if pct <= 0:
            continue
        tds += (
            f'<td width="{pct}%" style="background-color:{color};height:28px;'
            f'font-family:Courier New,monospace;font-size:9px;font-weight:bold;'
            f'color:#fff;text-align:center;vertical-align:middle;">{pct}%</td>'
        )
    return (
        '<table role="presentation" width="100%" cellspacing="0" cellpadding="0" '
        f'border="0" style="table-layout:fixed;"><tr>{tds}</tr></table>'
    )


def _aas_note_tag(text: str) -> str:
    t = text.strip()
    SOCIAL_KEYWORDS   = ("관심", "버즈", "소셜")
    MOMENTUM_KEYWORDS = ("추세", "모멘텀", "상승")

    if "고래" in t or "매집" in t:
        emoji = "🐋 "
    elif "과매도" in t:
        emoji = "📉 "
    elif any(k in t for k in SOCIAL_KEYWORDS):
        emoji = "🔥 "
    elif any(k in t for k in MOMENTUM_KEYWORDS):
        emoji = "🚀 "
    else:
        emoji = ""

    if any(k in t for k in SOCIAL_KEYWORDS):
        style = "font-family:'맑은 고딕','Apple SD Gothic Neo',sans-serif;font-size:12px;font-weight:bold;color:#c2410c;"
    elif any(k in t for k in MOMENTUM_KEYWORDS):
        style = "font-family:'맑은 고딕','Apple SD Gothic Neo',sans-serif;font-size:12px;font-weight:bold;color:#15803d;"
    else:
        style = "font-family:'맑은 고딕','Apple SD Gothic Neo',sans-serif;font-size:12px;font-weight:bold;color:#1d4ed8;"
    return f'<span style="{style}">{emoji}{t}</span>'


def fetch_aas_data() -> dict[str, str]:
    """GitHub AAS-Bot에서 Top3 데이터 가져오기. KST 오늘 → 어제 순 시도."""
    kst_now = datetime.now(timezone(timedelta(hours=9)))
    date_candidates = [
        kst_now.strftime("%Y-%m-%d"),
        (kst_now - timedelta(days=1)).strftime("%Y-%m-%d"),
    ]

    # 기본값
    ph: dict[str, str] = {}
    for i in range(1, 4):
        ph.update({
            f"{{{{AAS_COIN_{i}}}}}":          "—",
            f"{{{{AAS_SCORE_{i}}}}}":         "0.00",
            f"{{{{AAS_SCORE_PERCENT_{i}}}}}": "0",
            f"{{{{AAS_CHG_{i}}}}}":           "0.00",
            f"{{{{AAS_NOTE_TAG_{i}}}}}":      _aas_note_tag("—"),
            f"{{{{AAS_BAR_{i}}}}}":           _aas_bar_html(33.3, 33.3, 33.4),
        })

    aas_token = os.environ.get("AAS_BOT_TOKEN", "")
    headers = {"Authorization": f"token {aas_token}"} if aas_token else {}

    data = None
    for date_str in date_candidates:
        url = (f"https://raw.githubusercontent.com/Blockmedia-DataTeam/AAS-Bot"
               f"/main/reports/daily/{date_str}/newsletter_aas_top3_{date_str}.json")
        try:
            r = requests.get(url, timeout=10, headers=headers)
            r.raise_for_status()
            data = r.json()
            print(f"INFO: AAS data fetched for {date_str}")
            break
        except Exception as e:
            print(f"WARN: AAS fetch failed for {date_str}: {e}")

    if data is None:
        print("WARN: AAS data unavailable. Using defaults.")
        return ph

    for i, item in enumerate(data[:3], 1):
        score     = float(item.get("AAS", 0))
        score_pct = min(100, int((score / 3.0) * 100))
        note_text = item.get("Comment", "—")
        onchain   = float(item.get("Onchain",  33.3))
        social    = float(item.get("Social",   33.3))
        momentum  = float(item.get("Momentum", 33.4))
        ph[f"{{{{AAS_COIN_{i}}}}}"]          = item.get("Symbol", "—")
        ph[f"{{{{AAS_SCORE_{i}}}}}"]         = f"{score:.2f}"
        ph[f"{{{{AAS_SCORE_PERCENT_{i}}}}}"] = str(score_pct)
        ph[f"{{{{AAS_CHG_{i}}}}}"]           = f"{float(item.get('24H(%)', 0)):+.2f}"
        ph[f"{{{{AAS_NOTE_TAG_{i}}}}}"]      = _aas_note_tag(note_text)
        ph[f"{{{{AAS_BAR_{i}}}}}"]           = _aas_bar_html(onchain, social, momentum)

    return ph


# ─────────────────────────────────────────────────────────
# 메인 플레이스홀더 빌드
# ─────────────────────────────────────────────────────────

def build_placeholders() -> dict[str, str]:
    bm20 = load_json(BM20_JSON)
    krw  = load_json(KRW_JSON)

    # BTC
    btc_usd_txt, btc_1d_html = fetch_btc_price_and_change()

    # BM20
    level   = bm20.get("bm20Level", None)
    r1d_raw = (bm20.get("returns", {}) or {}).get("1D", None)
    bm20_1d_html = "—"
    if r1d_raw is not None:
        bm20_1d_html = colored_change_html(pct_to_display(r1d_raw), digits=2)

    # usdkrw (프리미엄 계산용)
    usdkrw = (bm20.get("kimchi_meta", {}) or {}).get("usdkrw", None)
    usdkrw_f = float(str(usdkrw).replace(",", "")) if usdkrw else None

    # Sentiment
    sentiment_label, sentiment_score = "—", "—"
    hist_obj = load_json_optional(BM20_HISTORY_JSON)
    if hist_obj:
        try:
            latest = hist_obj[-1] if isinstance(hist_obj, list) else hist_obj.get("latest", hist_obj)
            sent   = latest.get("sentiment", {})
            sentiment_label = str(sent.get("status") or sent.get("sentiment_label") or "—")
            score  = sent.get("value") or sent.get("sentiment_score")
            if score is not None:
                sentiment_score = f"{float(score):.0f}"
        except Exception:
            pass

    # 뉴스
    wp_lead = fetch_wp_newsletter_lead()
    news3   = fetch_wp_newsletter_news()
    top1, top2, top3 = news3[0], news3[1], news3[2]

    ph: dict[str, str] = {
        "{{LETTER_DATE}}":         datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d"),
        # BTC / BM20 / Sentiment
        "{{BTC_USD}}":             btc_usd_txt,
        "{{BTC_1D}}":              btc_1d_html,
        "{{BM20_LEVEL}}":          fmt_level(level) if level is not None else "—",
        "{{BM20_1D}}":             bm20_1d_html,
        "{{SENTIMENT_LABEL}}":     sentiment_label,
        "{{SENTIMENT_SCORE}}":     sentiment_score,
        # 뉴스
        "{{NEWS_HEADLINE}}":       wp_lead["NEWS_HEADLINE"],
        "{{NEWS_ONE_LINER_NOTE}}": wp_lead["NEWS_ONE_LINER_NOTE"],
        "{{TOP_NEWS_1}}":          top1["title"],
        "{{TOP_NEWS_2}}":          top2["title"],
        "{{TOP_NEWS_3}}":          top3["title"],
        "{{NEWS1_EXCERPT}}":       top1["excerpt"],
        "{{NEWS2_EXCERPT}}":       top2["excerpt"],
        "{{NEWS3_EXCERPT}}":       top3["excerpt"],
        "{{NEWS1_LINK}}":          top1["link"],
        "{{NEWS2_LINK}}":          top2["link"],
        "{{NEWS3_LINK}}":          top3["link"],
        "{{NEWS1_CATEGORY}}":      top1["category"],
        "{{NEWS2_CATEGORY}}":      top2["category"],
        "{{NEWS3_CATEGORY}}":      top3["category"],
        # 수신거부 (Stibee 발송 시 자동 치환)
        "{{UNSUB_URL}}":           "{{UNSUB_URL}}",
    }

    # 업비트 Top/Bottom
    upbit = fetch_upbit_top_bottom(n=3)
    ph["{{UPBIT_TOP1_SYMBOL}}"] = upbit.get("UPBIT_TOP1_SYMBOL", "—")
    ph["{{UPBIT_TOP2_SYMBOL}}"] = upbit.get("UPBIT_TOP2_SYMBOL", "—")
    ph["{{UPBIT_TOP3_SYMBOL}}"] = upbit.get("UPBIT_TOP3_SYMBOL", "—")
    ph["{{UPBIT_TOP1_CHG}}"]    = upbit.get("UPBIT_TOP1_CHG", "—")
    ph["{{UPBIT_TOP2_CHG}}"]    = upbit.get("UPBIT_TOP2_CHG", "—")
    ph["{{UPBIT_TOP3_CHG}}"]    = upbit.get("UPBIT_TOP3_CHG", "—")
    ph["{{UPBIT_BOT1_SYMBOL}}"] = upbit.get("UPBIT_BOT1_SYMBOL", "—")
    ph["{{UPBIT_BOT2_SYMBOL}}"] = upbit.get("UPBIT_BOT2_SYMBOL", "—")
    ph["{{UPBIT_BOT3_SYMBOL}}"] = upbit.get("UPBIT_BOT3_SYMBOL", "—")
    ph["{{UPBIT_BOT1_CHG}}"]    = upbit.get("UPBIT_BOT1_CHG", "—")
    ph["{{UPBIT_BOT2_CHG}}"]    = upbit.get("UPBIT_BOT2_CHG", "—")
    ph["{{UPBIT_BOT3_CHG}}"]    = upbit.get("UPBIT_BOT3_CHG", "—")

    # 프리미엄
    prem = fetch_premium_data(usdkrw_f)
    ph["{{KIMCHI_PREM_PCT}}"]  = prem.get("KIMCHI_PREM_PCT", "—")
    ph["{{CB_PREMIUM_PCT}}"]   = prem.get("CB_PREMIUM_PCT", "—")
    ph["{{PREMIUM_COMMENT}}"]  = prem.get("PREMIUM_COMMENT", "—")
    ph["{{PREMIUM_ASOF}}"]     = prem.get("PREMIUM_ASOF", "—")

    # ETF
    ph.update(load_etf_summary())

    # AAS
    ph.update(fetch_aas_data())

    return ph


# ─────────────────────────────────────────────────────────
# 렌더
# ─────────────────────────────────────────────────────────

def render() -> None:
    if not TEMPLATE.exists():
        raise FileNotFoundError(f"Missing {TEMPLATE}")
    html = TEMPLATE.read_text(encoding="utf-8")
    ph   = build_placeholders()
    for k in sorted(ph.keys(), key=len, reverse=True):
        html = html.replace(k, str(ph[k]))

    left = sorted(set(re.findall(r"\{\{[A-Z0-9_]+\}\}", html)))
    # UNSUB_URL는 Stibee가 치환하므로 경고 제외
    left = [v for v in left if v != "{{UNSUB_URL}}"]
    if left:
        print("WARN: Unfilled placeholders:", left)

    OUT.write_text(html, encoding="utf-8")
    print(f"OK: wrote {OUT}")


if __name__ == "__main__":
    render()
