#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
render_aas_brief.py
───────────────────
AAS-Bot private repo에서 JSON + CSV + PNG를 읽어
aas_brief_template.html 의 플레이스홀더를 채워 aas_brief.html 을 생성합니다.

render_letter.py 와 동일한 방식 (순수 string replace, no JS)

Inputs (GitHub raw URL — private repo, AAS_BOT_TOKEN 환경변수 필요)
  reports/daily/{date}/newsletter_aas_top3_{date}.json
  reports/daily/{date}/daily_report_{date}.csv

Output
  clm_brief.html
"""

from __future__ import annotations

import os
import re
import json
import requests
import pandas as pd
from io import StringIO
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT     = Path(__file__).resolve().parent
TEMPLATE = ROOT.parent / "aas_brief_template.html"
OUT      = ROOT.parent / "clm_brief.html"

REPO     = "Blockmedia-DataTeam/AAS-Bot"
BRANCH   = "main"
BASE_RAW = f"https://raw.githubusercontent.com/{REPO}/{BRANCH}"
BASE_API = f"https://api.github.com/repos/{REPO}/contents"


def _token_headers() -> dict:
    token = os.environ.get("AAS_BOT_TOKEN", "")
    return {"Authorization": f"token {token}"} if token else {}


def _color_class(v: float) -> str:
    return "up" if v >= 0 else "dn"


def _fmt_chg(v: float) -> str:
    return f"+{v:.2f}%" if v >= 0 else f"{v:.2f}%"


def _fmt_price(p: float) -> str:
    if p >= 1000:  return f"{p:,.2f}"
    if p >= 1:     return f"{p:.4f}"
    if p >= 0.01:  return f"{p:.6f}"
    return f"{p:.8f}"


def _action_badge(rsi: float, comment: str) -> str:
    if "고래" in comment or "accum" in comment.lower():
        return '<span class="badge badge-accum">ACCUM 🐋</span>'
    if "관심" in comment or "specu" in comment.lower():
        return '<span class="badge badge-specu">SPECU 🗣️</span>'
    if "과열" in comment or "caution" in comment.lower():
        return '<span class="badge badge-caution">CAUTION</span>'
    if rsi <= 30:
        return '<span class="badge badge-over">OVERSOLD</span>'
    return '<span class="badge badge-specu">WATCH</span>'


COIN_LOGO_URL = {
    "BTC":  "https://assets.coingecko.com/coins/images/1/small/bitcoin.png",
    "ETH":  "https://assets.coingecko.com/coins/images/279/small/ethereum.png",
    "SOL":  "https://assets.coingecko.com/coins/images/4128/small/solana.png",
    "XRP":  "https://assets.coingecko.com/coins/images/44/small/xrp-symbol-white-128.png",
    "BNB":  "https://assets.coingecko.com/coins/images/825/small/bnb-icon2_2x.png",
    "ADA":  "https://assets.coingecko.com/coins/images/975/small/cardano.png",
    "DOGE": "https://assets.coingecko.com/coins/images/5/small/dogecoin.png",
    "AVAX": "https://assets.coingecko.com/coins/images/12559/small/Avalanche_Circle_RedWhite_Trans.png",
    "DOT":  "https://assets.coingecko.com/coins/images/12171/small/polkadot.png",
    "ATOM": "https://assets.coingecko.com/coins/images/1481/small/cosmos_hub.png",
    "SUI":  "https://assets.coingecko.com/coins/images/26375/small/sui_asset.jpeg",
}


def _major_card(rank: int, sym: str, aas: float, aas_delta: float,
                price: float, chg: float, rsi: float, comment: str) -> str:
    logo_url = COIN_LOGO_URL.get(sym, "")
    logo_html = (
        f'<img src="{logo_url}" alt="{sym}" onerror="this.style.display=\'none\';this.nextElementSibling.style.display=\'flex\'">'
        f'<span class="major-card-logo-fallback" style="display:none">{sym[:2]}</span>'
    ) if logo_url else f'<span class="major-card-logo-fallback">{sym[:2]}</span>'

    badge = _action_badge(rsi, comment)
    delta_cls = "up" if aas_delta >= 0 else "dn"
    delta_arrow = "▲" if aas_delta >= 0 else "▼"
    chg_cls = "up" if chg >= 0 else "dn"
    chg_str = _fmt_chg(chg)

    return f"""    <div class="major-card">
      <div class="major-card-head">
        <div class="major-card-logo">
          {logo_html}
          <div>
            <div class="major-card-name">{sym}</div>
            <div class="major-card-rank">코생지 Rank #{rank}</div>
          </div>
        </div>
        {badge}
      </div>
      <div class="major-card-clm">
        <span class="major-card-clm-val">{aas:.2f}</span>
        <span class="major-card-clm-delta {delta_cls}">{delta_arrow} {abs(aas_delta):.2f}</span>
      </div>
      <div class="major-card-meta">
        <span>가격 <span class="meta-val neutral">${_fmt_price(price)}</span></span>
        <span>24H <span class="meta-val {chg_cls}">{chg_str}</span></span>
        <span>RSI <span class="meta-val neutral">{rsi:.0f}</span></span>
      </div>
      <div class="major-card-comment">{comment}</div>
    </div>"""


def _comment_class(comment: str) -> str:
    if "고래" in comment: return "whale"
    if "관심" in comment: return "specu"
    if "과열" in comment: return "caution"
    return ""


def _date_candidates() -> list[str]:
    """KST 07:00 이후면 당일 포함, 이전이면 전날부터 — 최근 4일 후보 반환"""
    kst = timezone(timedelta(hours=9))
    now_kst = datetime.now(kst)
    # 07:00 이후면 당일 데이터가 올라왔을 가능성 있음
    if now_kst.hour >= 7:
        base = now_kst.date()
    else:
        base = (now_kst - timedelta(days=1)).date()
    return [(base - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(4)]


def _find_latest_date() -> str | None:
    """GitHub API로 폴더 목록을 가져와 가장 최신 날짜 반환.
    KST 07:00 이후라면 당일 폴더가 있는지 우선 확인."""
    kst = timezone(timedelta(hours=9))
    now_kst = datetime.now(kst)

    try:
        r = requests.get(f"{BASE_API}/reports/daily", headers=_token_headers(), timeout=10)
        r.raise_for_status()
        folders = sorted([i["name"] for i in r.json() if i["type"] == "dir"], reverse=True)
        if not folders:
            return None

        latest = folders[0]  # 가장 최신 폴더

        # KST 07:00 이전이면 최신 폴더가 당일이어도 아직 업데이트 전일 수 있음
        # → 그 경우 두 번째 폴더(전날)로 fallback
        today_str = now_kst.date().strftime("%Y-%m-%d")
        if now_kst.hour < 7 and latest == today_str and len(folders) > 1:
            print(f"INFO: KST {now_kst.strftime('%H:%M')} — 07:00 이전, {folders[1]} 사용")
            return folders[1]

        print(f"INFO: latest folder: {latest}")
        return latest

    except Exception as e:
        print(f"WARN: folder list failed: {e}")
        return None


def _fetch_raw(path: str) -> requests.Response | None:
    try:
        r = requests.get(f"{BASE_RAW}/{path}", headers=_token_headers(), timeout=15)
        r.raise_for_status()
        print(f"INFO: fetched {path}")
        return r
    except Exception as e:
        print(f"WARN: fetch failed {path}: {e}")
        return None




def _table_row(rank: int, sym: str, aas: float, price: float,
               chg: float, rsi: float, comment: str, hq: bool = False) -> str:
    return f"""
    <tr class="{'hq' if hq else ''}">
      <td><span class="rank-c {'top' if rank <= 3 else ''}">{rank}</span></td>
      <td class="sym">{sym}</td>
      <td class="aas">{aas:.2f}</td>
      <td class="price">{_fmt_price(price)}</td>
      <td class="chg {_color_class(chg)}">{_fmt_chg(chg)}</td>
      <td class="rsi">{rsi:.0f}</td>
      <td>{_action_badge(rsi, comment)}</td>
      <td class="cmt {_comment_class(comment)}">{comment}</td>
    </tr>"""


def render() -> None:
    if not TEMPLATE.exists():
        raise FileNotFoundError(f"Missing template: {TEMPLATE}")

    # 1. 최신 날짜 찾기
    date_str = _find_latest_date()
    if not date_str:
        for d in _date_candidates():
            if _fetch_raw(f"reports/daily/{d}/newsletter_aas_top3_{d}.json"):
                date_str = d
                break
    if not date_str:
        raise RuntimeError("코생지 데이터를 찾을 수 없습니다")

    print(f"INFO: using date {date_str}")
    base = f"reports/daily/{date_str}"

    # 2. JSON (Top 3)
    r_json = _fetch_raw(f"{base}/newsletter_aas_top3_{date_str}.json")
    top3   = r_json.json() if r_json else []

    # 3. CSV (전체) + 전날 CSV (delta용)
    r_csv = _fetch_raw(f"{base}/daily_report_{date_str}.csv")
    if r_csv:
        df = pd.read_csv(StringIO(r_csv.text))
        required = {"Rank", "symbol", "AAS_score", "price", "24h", "RSI"}
        missing = required - set(df.columns)
        if missing:
            print(f"WARN: CSV 컬럼 누락: {missing}")
            df = pd.DataFrame()
        top10_df = df[df["Rank"] <= 10].copy() if not df.empty else pd.DataFrame()
    else:
        df = top10_df = pd.DataFrame()

    # 전날 CSV — delta 계산용
    prev_df = pd.DataFrame()
    try:
        prev_date = (datetime.strptime(date_str, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
        r_prev = _fetch_raw(f"reports/daily/{prev_date}/daily_report_{prev_date}.csv")
        if r_prev:
            prev_df = pd.read_csv(StringIO(r_prev.text))
            print(f"INFO: prev CSV loaded ({prev_date})")
    except Exception as e:
        print(f"WARN: prev CSV 실패: {e}")

    def _clm_delta(sym: str) -> float:
        try:
            if prev_df.empty or df.empty: return 0.0
            if "symbol" not in prev_df.columns or "AAS_score" not in prev_df.columns: return 0.0
            prev_row  = prev_df[prev_df["symbol"] == sym]
            today_row = df[df["symbol"] == sym]
            if prev_row.empty or today_row.empty: return 0.0
            return float(today_row.iloc[0]["AAS_score"]) - float(prev_row.iloc[0]["AAS_score"])
        except Exception as e:
            print(f"WARN: CLM delta 계산 실패 ({sym}): {e}")
            return 0.0


    # 5. Major coins — 카드 형태
    major_rows_html = ""
    if not df.empty:
        for sym in ["ETH", "SOL", "XRP"]:
            row = df[df["symbol"] == sym]
            if row.empty: continue
            row = row.iloc[0]
            rsi     = float(row["RSI"])
            aas     = float(row["AAS_score"])
            aas_delta = _clm_delta(sym)
            comment = "과열 직전 (주의)" if rsi >= 70 else "과매도 + 관심도 유지 — 투심 회복 기대"
            major_rows_html += _major_card(
                rank=int(row["Rank"]), sym=sym,
                aas=aas, aas_delta=aas_delta,
                price=float(row["price"]),
                chg=float(row["24h"]),
                rsi=rsi, comment=comment,
            )

    # 6. Top 10
    top3_map = {i.get("Symbol"): i.get("Comment", "") for i in top3}
    top10_rows_html = ""
    if not top10_df.empty:
        for _, row in top10_df.iterrows():
            sym = row["symbol"]
            aas = float(row["AAS_score"])
            chg = float(row["24h"])
            rsi = float(row["RSI"])
            comment = top3_map.get(sym,
                "고래 매집" if aas >= 1.5 else
                "관심 집중" if aas >= 1.0 else
                "모니터링"
            )
            top10_rows_html += _table_row(
                rank=int(row["Rank"]), sym=sym,
                aas=aas, price=float(row["price"]),
                chg=chg, rsi=rsi,
                comment=comment, hq=(aas >= 1.5)
            )

    # 7. 대시보드 통계
    if not top10_df.empty:
        chg_vals  = top10_df["24h"].astype(float)
        avg_top10 = chg_vals.mean()
        median    = chg_vals.median()
        excl1     = chg_vals.iloc[1:].mean() if len(chg_vals) > 1 else avg_top10
        best_row  = top10_df.loc[chg_vals.idxmax()]
        worst_row = top10_df.loc[chg_vals.idxmin()]
        best_str  = f"{best_row['symbol']} ({_fmt_chg(float(best_row['24h']))})"
        worst_str = f"{worst_row['symbol']} ({_fmt_chg(float(worst_row['24h']))})"
    else:
        avg_top10 = median = excl1 = 0.0
        best_str = worst_str = "—"

    # BTC 수익률: ① CSV → ② CoinGecko API fallback
    btc_return: float | None = None
    if not df.empty and "BTC" in df["symbol"].values:
        btc_row = df[df["symbol"] == "BTC"]
        if not btc_row.empty:
            btc_return = float(btc_row.iloc[0]["24h"])
            print(f"INFO: BTC from CSV: {btc_return:.2f}%")

    if btc_return is None:
        try:
            cg_url = (
                "https://api.coingecko.com/api/v3/simple/price"
                "?ids=bitcoin&vs_currencies=usd&include_24hr_change=true"
            )
            cg_r = requests.get(cg_url, timeout=10)
            cg_r.raise_for_status()
            btc_return = float(cg_r.json()["bitcoin"]["usd_24h_change"])
            print(f"INFO: BTC from CoinGecko: {btc_return:.2f}%")
        except Exception as e:
            print(f"WARN: BTC CoinGecko 실패: {e}")
            btc_return = 0.0

    alpha_btc = avg_top10 - btc_return

    # 8. Insight
    hq_coins = [r["symbol"] for _, r in top10_df.iterrows()
                if float(r["AAS_score"]) >= 1.5] if not top10_df.empty else []
    hq_str   = ", ".join(hq_coins[:4]) if hq_coins else "—"
    oversold = sum(1 for _, r in top10_df.iterrows()
                   if float(r["RSI"]) <= 40) if not top10_df.empty else 0

    def _colored(v: float, text: str = None) -> str:
        t = text or _fmt_chg(v)
        cls = "up" if v >= 0 else "dn"
        return f'<span class="{cls}" style="font-weight:700">{t}</span>'

    insight1 = (f"코생지 Top 10이 24시간 평균 {_colored(avg_top10)} 상승하며 "
                f"비트코인 대비 {_colored(alpha_btc, f'{alpha_btc:+.2f}%p')}의 초과수익을 기록했다.")
    insight2 = (f"Top 10 중 <strong>{oversold}개 종목</strong>이 "
                f"<strong>OVERSOLD</strong> 신호를 보이며 과매도 국면이 우세하다.")
    insight3 = (f"코생지 1.3 이상·RSI 35 이하 조건을 충족한 "
                f"<strong>{hq_str}</strong> 종목의 추이를 주목할 필요가 있다.")

    # 9. 표시 날짜 — 데이터 폴더 날짜와 무관하게 항상 KST 오늘
    kst = timezone(timedelta(hours=9))
    today_kst = datetime.now(kst)
    day_kr = ["월","화","수","목","금","토","일"][today_kst.weekday()]
    report_date = f"{today_kst.strftime('%Y-%m-%d')} ({day_kr})"

    # 10. 플레이스홀더 치환
    ph = {
        "{{REPORT_DATE}}":       report_date,
        "{{AVG_TOP10}}":         _fmt_chg(avg_top10),
        "{{AVG_TOP10_COLOR}}":   _color_class(avg_top10),
        "{{MEDIAN_RETURN}}":     _fmt_chg(median),
        "{{MEDIAN_COLOR}}":      _color_class(median),
        "{{EXCL1_RETURN}}":      _fmt_chg(excl1),
        "{{EXCL1_COLOR}}":       _color_class(excl1),
        "{{BEST_PERFORMER}}":    best_str,
        "{{WORST_PERFORMER}}":   worst_str,
        "{{BTC_RETURN}}":        _fmt_chg(btc_return),
        "{{BTC_COLOR}}":         _color_class(btc_return),
        "{{ALPHA_BTC}}":         f"{alpha_btc:+.2f}%p",
        "{{ALPHA_COLOR}}":       _color_class(alpha_btc),
        "{{INSIGHT_1}}":         insight1,
        "{{INSIGHT_2}}":         insight2,
        "{{INSIGHT_3}}":         insight3,
        "{{MAJOR_ROWS}}":        major_rows_html,
        "{{TOP10_ROWS}}":        top10_rows_html,
    }

    html = TEMPLATE.read_text(encoding="utf-8")
    for k in sorted(ph.keys(), key=len, reverse=True):
        html = html.replace(k, str(ph[k]))

    left = sorted(set(re.findall(r"\{\{[A-Z0-9_]+\}\}", html)))
    if left:
        print("WARN: unfilled placeholders:", left)

    OUT.write_text(html, encoding="utf-8")
    print(f"OK: wrote {OUT}")


if __name__ == "__main__":
    render()
