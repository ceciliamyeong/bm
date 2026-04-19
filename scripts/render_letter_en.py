#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
render_letter_en.py
────────────────────
영문 뉴스레터 letter_en.html 생성.
기존 render_letter.py 가 먼저 실행된 후 사용되는 JSON 파일들을 공유.

Inputs (JSON, render_letter.py 실행 후 생성):
  bm20_latest.json
  nasdaq_series.json
  out/history/krw_24h_snapshots.json
  data/bm20_history.json     (optional)
  data/etf_summary.json      (optional)

API 호출:
  Upbit (Top/Bottom 3)
  Binance / Coinbase (프리미엄)
  WordPress REST API (뉴스)
  AAS-Bot GitHub (CLM 데이터)
  DeepL (번역, optional)

Output:
  letter_en.html
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any
from datetime import datetime, timezone, timedelta

import requests

ROOT = Path(__file__).resolve().parent.parent

TEMPLATE_EN    = ROOT / "letter_newsletter_template_EN.html"
OUT_EN         = ROOT / "letter_en.html"
BM20_JSON      = ROOT / "bm20_latest.json"
NASDAQ_JSON    = ROOT / "nasdaq_series.json"
SNAPSHOTS_JSON = ROOT / "out/history/krw_24h_snapshots.json"
BM20_HIST_JSON = ROOT / "data/bm20_history.json"
ETF_JSON       = ROOT / "data/etf_summary.json"

WP_BASE_URL               = "https://blockmedia.co.kr/wp-json/wp/v2"
WP_TAG_ID_NEWSLETTER      = 28978
WP_TAG_ID_NEWSLETTER_LEAD = 80405

DEEPL_API_KEY = os.getenv("DEEPL_API_KEY", "")
AAS_BOT_TOKEN = os.getenv("AAS_BOT_TOKEN", "")

GREEN = "#16a34a"
RED   = "#dc2626"
INK   = "#0f172a"
KST   = timezone(timedelta(hours=9))


# ─────────────────────────────────────────────────────────
# 헬퍼
# ─────────────────────────────────────────────────────────

def load_json(p: Path) -> Any:
    if not p.exists():
        raise FileNotFoundError(f"Missing {p}")
    return json.loads(p.read_text(encoding="utf-8"))

def load_json_optional(p: Path) -> Any | None:
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))

def colored_change_html(pct: float, digits: int = 2) -> str:
    v = float(pct)
    arrow, color = ("▲", GREEN) if v > 0 else (("▼", RED) if v < 0 else ("", INK))
    return f'<span style="color:{color};font-weight:900;">{arrow} {v:+.{digits}f}%</span>'

def fmt_krw_vol(val: float) -> str:
    t = val / 1_000_000_000_000
    if t >= 1:
        return f"₩{t:.2f}T"
    b = val / 100_000_000
    return f"₩{b:.0f}B"

def strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", "", text or "").strip()

def pct_display(x: float) -> float:
    x = float(x)
    return x * 100 if abs(x) <= 1.5 else x


# ─────────────────────────────────────────────────────────
# DeepL 번역
# ─────────────────────────────────────────────────────────

def translate(text: str) -> str:
    if not text or text == "—" or not DEEPL_API_KEY:
        return text
    try:
        r = requests.post(
            "https://api-free.deepl.com/v2/translate",
            headers={"Authorization": f"DeepL-Auth-Key {DEEPL_API_KEY}"},
            json={"text": [text], "source_lang": "KO", "target_lang": "EN"},
            timeout=10,
        )
        r.raise_for_status()
        return r.json()["translations"][0]["text"]
    except Exception as e:
        print(f"WARN: DeepL failed: {e}")
        return text


# ─────────────────────────────────────────────────────────
# BTC + BM20 (bm20_latest.json)
# ─────────────────────────────────────────────────────────

def load_bm20() -> dict:
    bm20 = load_json(BM20_JSON)
    r1d  = (bm20.get("returns", {}) or {}).get("1D", None)
    level = bm20.get("bm20Level", None)
    usdkrw = (bm20.get("kimchi_meta", {}) or {}).get("usdkrw", None)
    usdkrw_f = float(str(usdkrw).replace(",", "")) if usdkrw else None

    # BTC 가격 (CMC 또는 Yahoo fallback)
    btc_usd, btc_1d = fetch_btc()

    return {
        "{{BTC_USD}}":   btc_usd,
        "{{BTC_1D}}":    btc_1d,
        "{{BM20_LEVEL}}": f"{float(level):,.2f}" if level else "—",
        "{{BM20_1D}}":   colored_change_html(pct_display(r1d)) if r1d is not None else "—",
        "_usdkrw":       usdkrw_f,
    }

def fetch_btc() -> tuple[str, str]:
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
            return f"{price:,.0f}", colored_change_html(chg)
        except Exception as e:
            print(f"WARN: CMC BTC failed: {e}")
    try:
        r2 = requests.get(
            "https://query1.finance.yahoo.com/v8/finance/chart/BTC-USD",
            headers={"User-Agent": "Mozilla/5.0"},
            params={"interval": "1d", "range": "5d"},
            timeout=10,
        )
        r2.raise_for_status()
        closes = [x for x in r2.json()["chart"]["result"][0]["indicators"]["quote"][0]["close"] if x]
        price  = float(closes[-1])
        chg    = (price / float(closes[-2]) - 1) * 100 if len(closes) >= 2 else 0.0
        return f"{price:,.0f}", colored_change_html(chg)
    except Exception as e:
        print(f"WARN: Yahoo BTC failed: {e}")
        return "—", "—"


# ─────────────────────────────────────────────────────────
# NASDAQ (nasdaq_series.json)
# ─────────────────────────────────────────────────────────

def load_nasdaq() -> dict:
    try:
        data = load_json(NASDAQ_JSON)
        if len(data) < 2:
            return {"{{NASDAQ_1D}}": "—", "{{NASDAQ_PRICE}}": "—"}
        price = float(data[-1]["price"])
        prev  = float(data[-2]["price"])
        chg   = (price / prev - 1) * 100
        return {
            "{{NASDAQ_1D}}":    colored_change_html(chg),
            "{{NASDAQ_PRICE}}": f"{price:,.0f}",
        }
    except Exception as e:
        print(f"WARN: NASDAQ load failed: {e}")
        return {"{{NASDAQ_1D}}": "—", "{{NASDAQ_PRICE}}": "—"}


# ─────────────────────────────────────────────────────────
# Sentiment (bm20_history.json)
# ─────────────────────────────────────────────────────────

def load_sentiment() -> dict:
    hist = load_json_optional(BM20_HIST_JSON)
    if not hist:
        return {"{{SENTIMENT_LABEL}}": "—", "{{SENTIMENT_SCORE}}": "—"}
    try:
        latest = hist[-1] if isinstance(hist, list) else hist.get("latest", hist)
        sent   = latest.get("sentiment", {})
        label  = str(sent.get("status") or sent.get("sentiment_label") or "—")
        score  = sent.get("value") or sent.get("sentiment_score")
        return {
            "{{SENTIMENT_LABEL}}": label,
            "{{SENTIMENT_SCORE}}": f"{float(score):.0f}" if score is not None else "—",
        }
    except Exception:
        return {"{{SENTIMENT_LABEL}}": "—", "{{SENTIMENT_SCORE}}": "—"}


# ─────────────────────────────────────────────────────────
# KRW 거래량 (krw_24h_snapshots.json)
# ─────────────────────────────────────────────────────────

def load_krw_volume() -> dict:
    FB = {
        "{{KRW_TOTAL_VOL}}":       "—",
        "{{KRW_UPBIT_VOL}}":       "—",
        "{{KRW_BITHUMB_VOL}}":     "—",
        "{{KRW_UPBIT_TOP5_ROWS}}": "—",
    }
    try:
        data   = load_json(SNAPSHOTS_JSON)
        latest = data[-1]
        totals = latest.get("totals", {})
        top5   = latest.get("by_exchange_top", {}).get("upbit_top5", [])

        rows_html = ""
        for i, item in enumerate(top5[:5]):
            sym    = item["symbol"].replace("KRW-", "")
            val    = fmt_krw_vol(float(item["value"]))
            border = "border-bottom:1px solid #f1f5f9;" if i < 4 else ""
            rows_html += (
                f'<table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0">'
                f'<tr>'
                f'<td style="font-family:-apple-system,BlinkMacSystemFont,\'Segoe UI\',Arial,sans-serif;'
                f'font-size:13px;font-weight:900;color:#0f172a;padding:5px 0;{border}">{sym}</td>'
                f'<td align="right" style="font-family:Courier New,Courier,monospace;'
                f'font-size:13px;font-weight:900;color:#475569;padding:5px 0;{border}">{val}</td>'
                f'</tr></table>'
            )

        return {
            "{{KRW_TOTAL_VOL}}":       fmt_krw_vol(float(totals.get("combined_24h", 0))),
            "{{KRW_UPBIT_VOL}}":       fmt_krw_vol(float(totals.get("upbit_24h", 0))),
            "{{KRW_BITHUMB_VOL}}":     fmt_krw_vol(float(totals.get("bithumb_24h", 0))),
            "{{KRW_UPBIT_TOP5_ROWS}}": rows_html,
        }
    except Exception as e:
        print(f"WARN: KRW volume load failed: {e}")
        return FB


# ─────────────────────────────────────────────────────────
# 업비트 Top/Bottom 3
# ─────────────────────────────────────────────────────────

def fetch_upbit_top_bottom(n: int = 3) -> dict:
    FB = {**{f"{{{{UPBIT_TOP{i}_SYMBOL}}}}": "—" for i in range(1, n+1)},
          **{f"{{{{UPBIT_TOP{i}_CHG}}}}":    "—" for i in range(1, n+1)},
          **{f"{{{{UPBIT_BOT{i}_SYMBOL}}}}": "—" for i in range(1, n+1)},
          **{f"{{{{UPBIT_BOT{i}_CHG}}}}":    "—" for i in range(1, n+1)}}
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
            result[f"{{{{UPBIT_TOP{i}_SYMBOL}}}}"] = sym
            result[f"{{{{UPBIT_TOP{i}_CHG}}}}"]    = f"+{pct:.1f}%"
        for i, t in enumerate(reversed(tickers[-n:]), 1):
            sym = t["market"].replace("KRW-", "")
            pct = float(t.get("signed_change_rate", 0)) * 100
            result[f"{{{{UPBIT_BOT{i}_SYMBOL}}}}"] = sym
            result[f"{{{{UPBIT_BOT{i}_CHG}}}}"]    = f"{pct:.1f}%"
        return result
    except Exception as e:
        print(f"WARN: Upbit top/bottom failed: {e}")
        return FB


# ─────────────────────────────────────────────────────────
# 김치·코인베이스 프리미엄
# ─────────────────────────────────────────────────────────

def fetch_premium(usdkrw: float | None) -> dict:
    FB = {
        "{{KIMCHI_PREM_PCT}}": "—",
        "{{CB_PREMIUM_PCT}}":  "—",
        "{{PREMIUM_COMMENT}}": "Premium data unavailable.",
        "{{PREMIUM_ASOF}}":    "—",
    }
    def _c(v: float) -> str:
        color = GREEN if v >= 0 else RED
        sign  = "+" if v >= 0 else "-"
        return f'<span style="color:{color};font-weight:900;">{sign}{abs(v):.2f}%</span>'
    try:
        upbit_krw  = float(requests.get("https://api.upbit.com/v1/ticker",
                                         params={"markets": "KRW-BTC"}, timeout=10).json()[0]["trade_price"])
        binance_usd = None
        for base in ["https://api.binance.com", "https://data-api.binance.vision"]:
            try:
                br = requests.get(f"{base}/api/v3/ticker/price",
                                   params={"symbol": "BTCUSDT"}, timeout=10)
                br.raise_for_status()
                binance_usd = float(br.json()["price"])
                break
            except Exception:
                continue
        if binance_usd is None:
            raise RuntimeError("Binance BTC price unavailable")
        fx     = usdkrw if (usdkrw and usdkrw > 100) else 1450.0
        cb_usd = float(requests.get("https://api.coinbase.com/v2/prices/BTC-USD/spot",
                                     timeout=10).json()["data"]["amount"])
        kimchi_pct = (upbit_krw / fx - binance_usd) / binance_usd * 100
        cb_pct     = (cb_usd - binance_usd) / binance_usd * 100

        if kimchi_pct > 1 and cb_pct > 0:
            comment = "Both Kimchi and Coinbase premiums positive → strong domestic demand vs. global markets."
        elif kimchi_pct > 1 and cb_pct <= 0:
            comment = "Kimchi premium positive but Coinbase at discount → isolated Korean buying pressure, caution advised."
        elif kimchi_pct < -0.5:
            comment = "Kimchi discount → possible selling pressure in Korea or KRW weakness."
        else:
            comment = f"Kimchi {kimchi_pct:+.2f}% / Coinbase {cb_pct:+.2f}% — neutral range."

        kst_now = datetime.now(KST)
        asof    = kst_now.strftime("As of %b %-d, %I:%M %p KST")

        return {
            "{{KIMCHI_PREM_PCT}}": _c(kimchi_pct),
            "{{CB_PREMIUM_PCT}}":  _c(cb_pct),
            "{{PREMIUM_COMMENT}}": comment,
            "{{PREMIUM_ASOF}}":    asof,
        }
    except Exception as e:
        print(f"WARN: Premium fetch failed: {e}")
        return FB


# ─────────────────────────────────────────────────────────
# ETF
# ─────────────────────────────────────────────────────────

def load_etf() -> dict:
    FB = {
        "{{ETF_BTC_INFLOW}}": "—", "{{ETF_BTC_AUM}}": "—",
        "{{ETF_ETH_INFLOW}}": "—", "{{ETF_ETH_AUM}}": "—",
        "{{ETF_SOL_INFLOW}}": "—", "{{ETF_SOL_AUM}}": "—",
    }
    if not ETF_JSON.exists():
        return FB
    try:
        raw = json.loads(ETF_JSON.read_text(encoding="utf-8"))
        def _aum(v) -> str:
            return f"${float(v)/1_000_000_000:.1f}B" if v else "—"
        def _inflow(v) -> str:
            val = float(v)
            b   = val / 1_000_000_000
            m   = val / 1_000_000
            txt = f"${b:+.1f}B" if abs(b) >= 1 else f"${m:+.0f}M"
            color = GREEN if val > 0 else (RED if val < 0 else "#64748b")
            return f'<span style="color:{color};font-weight:900;">{txt}</span>'
        result = {}
        for coin, sym in [("btc","BTC"),("eth","ETH"),("sol","SOL")]:
            d = raw.get(coin, {})
            result[f"{{{{ETF_{sym}_INFLOW}}}}"] = _inflow(d.get("dailyNetInflow", 0))
            result[f"{{{{ETF_{sym}_AUM}}}}"]    = _aum(d.get("totalNetAssets"))
        return result
    except Exception as e:
        print(f"WARN: ETF load failed: {e}")
        return FB


# ─────────────────────────────────────────────────────────
# 워드프레스 뉴스
# ─────────────────────────────────────────────────────────

def fetch_news_lead() -> dict:
    FB = {"{{NEWS_HEADLINE}}": "—", "{{NEWS_ONE_LINER_NOTE}}": "—"}
    def _parse(post: dict) -> dict:
        excerpt = strip_html(post["excerpt"]["rendered"])
        if len(excerpt) > 150:
            excerpt = excerpt[:150].rstrip() + "…"
        return {
            "{{NEWS_HEADLINE}}":       strip_html(post["title"]["rendered"]),
            "{{NEWS_ONE_LINER_NOTE}}": excerpt,
        }
    for tag_id in [WP_TAG_ID_NEWSLETTER_LEAD, WP_TAG_ID_NEWSLETTER]:
        try:
            res = requests.get(f"{WP_BASE_URL}/posts",
                               params={"tags": tag_id, "per_page": 1,
                                       "orderby": "date", "status": "publish"},
                               timeout=10)
            res.raise_for_status()
            posts = res.json()
            if posts:
                return _parse(posts[0])
        except Exception as e:
            print(f"WARN: WP lead fetch failed (tag {tag_id}): {e}")
    return FB

def fetch_news_list() -> list[dict]:
    empty = {"title": "—", "excerpt": "", "link": "#", "category": ""}
    try:
        res = requests.get(f"{WP_BASE_URL}/posts",
                           params={"tags": WP_TAG_ID_NEWSLETTER, "per_page": 3,
                                   "orderby": "date", "status": "publish",
                                   "_embed": 1,
                                   "_fields": "id,title,excerpt,link,_embedded,meta"},
                           timeout=10)
        res.raise_for_status()
        posts = res.json()
        if len(posts) < 3:
            raise ValueError(f"Only {len(posts)} posts found")
        result = []
        for post in posts[:3]:
            try:
                cats     = post.get("_embedded", {}).get("wp:term", [[]])[0]
                cat_name = cats[0]["name"] if cats else ""
            except Exception:
                cat_name = ""
            meta    = post.get("meta", {}) or {}
            summary = meta.get("bm_post_summary", "")
            if not summary:
                summary = strip_html(post["excerpt"]["rendered"])
            if len(summary) > 150:
                summary = summary[:150].rstrip() + "…"
            result.append({
                "title":    strip_html(post["title"]["rendered"]),
                "excerpt":  summary,
                "link":     post.get("link", "#"),
                "category": cat_name,
            })
        return result
    except Exception as e:
        print(f"WARN: WP news list failed: {e}")
        return [empty, empty, empty]


# ─────────────────────────────────────────────────────────
# AAS (CLM)
# ─────────────────────────────────────────────────────────

def _aas_bar_html(onchain: float, social: float, momentum: float) -> str:
    segs = [(onchain, "#2563eb"), (social, "#f97316"), (momentum, "#16a34a")]
    tds  = ""
    for pct, color in segs:
        if pct <= 0:
            continue
        tds += (f'<td width="{pct}%" style="background-color:{color};height:28px;'
                f'font-family:Courier New,monospace;font-size:9px;font-weight:bold;'
                f'color:#fff;text-align:center;vertical-align:middle;">{pct}%</td>')
    return (f'<table role="presentation" width="100%" cellspacing="0" cellpadding="0" '
            f'border="0" style="table-layout:fixed;"><tr>{tds}</tr></table>')

def _aas_note_tag(text: str) -> str:
    t = text.strip()
    style = "font-family:'Segoe UI',Arial,sans-serif;font-size:12px;font-weight:bold;color:#1d4ed8;"
    return f'<span style="{style}">{t}</span>'

def fetch_aas() -> dict:
    ph: dict = {}
    for i in range(1, 4):
        ph.update({
            f"{{{{AAS_COIN_{i}}}}}":          "—",
            f"{{{{AAS_SCORE_{i}}}}}":         "0.00",
            f"{{{{AAS_SCORE_PERCENT_{i}}}}}\": \"0",
            f"{{{{AAS_CHG_{i}}}}}":           "0.00",
            f"{{{{AAS_NOTE_TAG_{i}}}}}":      _aas_note_tag("—"),
            f"{{{{AAS_BAR_{i}}}}}":           _aas_bar_html(33.3, 33.3, 33.4),
        })
    headers = {"Authorization": f"token {AAS_BOT_TOKEN}"} if AAS_BOT_TOKEN else {}
    kst_now = datetime.now(KST)
    for date_str in [kst_now.strftime("%Y-%m-%d"),
                     (kst_now - timedelta(days=1)).strftime("%Y-%m-%d")]:
        url = (f"https://raw.githubusercontent.com/Blockmedia-DataTeam/AAS-Bot"
               f"/main/reports/daily/{date_str}/newsletter_aas_top3_{date_str}.json")
        try:
            r = requests.get(url, timeout=10, headers=headers)
            r.raise_for_status()
            data = r.json()
            print(f"INFO: AAS data fetched for {date_str}")
            for i, item in enumerate(data[:3], 1):
                score     = float(item.get("AAS", 0))
                score_pct = min(100, int((score / 3.0) * 100))
                ph[f"{{{{AAS_COIN_{i}}}}}"]          = item.get("Symbol", "—")
                ph[f"{{{{AAS_SCORE_{i}}}}}"]         = f"{score:.2f}"
                ph[f"{{{{AAS_SCORE_PERCENT_{i}}}}}"] = str(score_pct)
                ph[f"{{{{AAS_CHG_{i}}}}}"]           = f"{float(item.get('24H(%)', 0)):+.2f}"
                ph[f"{{{{AAS_NOTE_TAG_{i}}}}}"]      = _aas_note_tag(item.get("Comment", "—"))
                ph[f"{{{{AAS_BAR_{i}}}}}"]           = _aas_bar_html(
                    float(item.get("Onchain", 33.3)),
                    float(item.get("Social", 33.3)),
                    float(item.get("Momentum", 33.4)),
                )
            return ph
        except Exception as e:
            print(f"WARN: AAS fetch failed for {date_str}: {e}")
    return ph


# ─────────────────────────────────────────────────────────
# 메인
# ─────────────────────────────────────────────────────────

def build_placeholders() -> dict:
    # BM20 + BTC
    bm20_data = load_bm20()
    usdkrw    = bm20_data.pop("_usdkrw", None)

    ph: dict = {}
    ph["{{LETTER_DATE}}"] = datetime.now(KST).strftime("%Y-%m-%d")
    ph.update(bm20_data)
    ph.update(load_nasdaq())
    ph.update(load_sentiment())
    ph.update(load_krw_volume())
    ph.update(fetch_upbit_top_bottom())
    ph.update(fetch_premium(usdkrw))
    ph.update(load_etf())
    ph.update(fetch_aas())

    # 뉴스
    lead = fetch_news_lead()
    news = fetch_news_list()
    ph["{{NEWS_HEADLINE}}"]       = lead["{{NEWS_HEADLINE}}"]
    ph["{{NEWS_ONE_LINER_NOTE}}"] = lead["{{NEWS_ONE_LINER_NOTE}}"]
    for i, n in enumerate(news[:3], 1):
        ph[f"{{{{TOP_NEWS_{i}}}}}"]     = n["title"]
        ph[f"{{{{NEWS{i}_EXCERPT}}}}"]  = n["excerpt"]
        ph[f"{{{{NEWS{i}_LINK}}}}"]     = n["link"]
        ph[f"{{{{NEWS{i}_CATEGORY}}}}"] = n["category"]

    ph["{{UNSUB_URL}}"] = "{{UNSUB_URL}}"
    return ph


def render() -> None:
    if not TEMPLATE_EN.exists():
        raise FileNotFoundError(f"Missing {TEMPLATE_EN}")

    ph   = build_placeholders()
    html = TEMPLATE_EN.read_text(encoding="utf-8")

    # DeepL 번역 (뉴스 텍스트)
    news_keys = [
        "{{NEWS_HEADLINE}}", "{{NEWS_ONE_LINER_NOTE}}",
        "{{TOP_NEWS_1}}", "{{TOP_NEWS_2}}", "{{TOP_NEWS_3}}",
        "{{NEWS1_EXCERPT}}", "{{NEWS2_EXCERPT}}", "{{NEWS3_EXCERPT}}",
        "{{NEWS1_CATEGORY}}", "{{NEWS2_CATEGORY}}", "{{NEWS3_CATEGORY}}",
    ]
    if DEEPL_API_KEY:
        print("INFO: Translating news via DeepL...")
        for k in news_keys:
            if k in ph and ph[k] and ph[k] != "—":
                ph[k] = translate(ph[k])

    for k in sorted(ph.keys(), key=len, reverse=True):
        html = html.replace(k, str(ph[k]))

    left = [v for v in sorted(set(re.findall(r"\{\{[A-Z0-9_]+\}\}", html)))
            if v != "{{UNSUB_URL}}"]
    if left:
        print("WARN: Unfilled placeholders:", left)

    OUT_EN.write_text(html, encoding="utf-8")
    print(f"OK: wrote {OUT_EN}")


if __name__ == "__main__":
    render()
