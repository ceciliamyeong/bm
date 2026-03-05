#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import re
import requests
import pandas as pd
import io
from pathlib import Path
from typing import Any, Tuple

# --- 경로 및 URL 설정 ---
ROOT = Path(__file__).resolve().parent.parent
TEMPLATE = ROOT / "letter_newsletter_template.html"
OUT = ROOT / "letter.html"

BASE_URL = "https://data.blockmedia.co.kr"
URLS = {
    "BM20_JSON": f"{BASE_URL}/bm20_latest.json",
    "DAILY_CSV": f"{BASE_URL}/bm20_daily_data_latest.csv",
    "KRW_JSON": f"{BASE_URL}/out/history/krw_24h_latest.json",
    "BTC_JSON": f"{BASE_URL}/btc_usd_series.json",
    "BM20_HISTORY": f"{BASE_URL}/data/bm20_history.json",
    "XRP_SHARE": f"{BASE_URL}/out/global/k_xrp_share_24h_latest.json"
}

# 수동 파일 (로컬 저장소 유지)
NEWS_ONELINER_TXT = ROOT / "out/latest/news_one_liner.txt"
NEWS_ONELINER_NOTE_TXT = ROOT / "out/latest/news_one_liner_note.txt"
TOP_NEWS_JSON = ROOT / "out/latest/top_news_latest.json"

GREEN = "#16a34a"
RED = "#dc2626"
INK = "#0f172a"

# ------------------ 데이터 로드 헬퍼 (URL 접속) ------------------

def fetch_json(url_key: str) -> Any:
    try:
        resp = requests.get(URLS[url_key], timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"로드 실패 ({url_key}): {e}")
        return {}

def fetch_daily_df() -> pd.DataFrame:
    try:
        resp = requests.get(URLS["DAILY_CSV"], timeout=10)
        resp.raise_for_status()
        df = pd.read_csv(io.StringIO(resp.text))
        # 컬럼 표준화 로직
        for c in ("ticker", "asset"):
            if c in df.columns and "symbol" not in df.columns:
                df = df.rename(columns={c: "symbol"})
        for c in ("change_pct", "pct_change", "return_1d_pct", "return_1d"):
            if c in df.columns and "price_change_pct" not in df.columns:
                df = df.rename(columns={c: "price_change_pct"})
        df["symbol"] = df["symbol"].astype(str).str.upper()
        df["price_change_pct"] = pd.to_numeric(df["price_change_pct"], errors="coerce")
        return df.dropna(subset=["price_change_pct"])
    except Exception:
        return pd.DataFrame()

# ------------------ 기존 코드의 핵심 헬퍼 함수들 ------------------

def fmt_share_pct(x: Any) -> str:
    try:
        val = float(x)
        if abs(val) <= 1.5: val *= 100.0
        return f"{val:.1f}%"
    except: return "—"

def colored_change_html(pct_value: float) -> str:
    v = float(pct_value)
    arrow, color = ("▲", GREEN) if v > 0 else (("▼", RED) if v < 0 else ("", INK))
    return f'<span style="color:{color};font-weight:900;">{arrow} {v:+.2f}%</span>'

def extract_sentiment(obj: Any) -> tuple[str, str]:
    if not isinstance(obj, dict): return "—", "—"
    target = obj.get("latest") or (obj.get("series")[-1] if obj.get("series") else obj)
    label = target.get("sentiment_label") or target.get("label") or "—"
    score = target.get("sentiment_score") or target.get("score") or "—"
    return str(label), (f"{float(score):.0f}" if score != "—" else "—")

# ------------------ 플레이스홀더 빌드 ------------------

def build_placeholders():
    bm20 = fetch_json("BM20_JSON")
    krw = fetch_json("KRW_JSON")
    df = fetch_daily_df()
    hist = fetch_json("BM20_HISTORY")
    xrp_data = fetch_json("XRP_SHARE")

    # BM20 & Kimchi
    level = bm20.get("bm20Level", "—")
    r1d = bm20.get("returns", {}).get("1D", 0)
    bm20_1d = float(r1d)*100 if abs(float(r1d)) <= 1.5 else float(r1d)
    
    # KRW Signals (스크린샷에 비어있던 부분)
    meta = krw.get("meta", {})
    kr_share_global = meta.get("kr_share_global") or krw.get("kr_share_global", "—")
    k_safety = meta.get("k_safety") or krw.get("k_safety", "—")
    
    # Best/Worst (Move 1,2,3)
    moves = ["—", "—", "—"]
    breadth = "—"
    top10_conc = "—"
    if not df.empty:
        top3 = df.sort_values("price_change_pct", ascending=False).head(3)
        moves = [f"{r.symbol} {r.price_change_pct:+.2f}%" for r in top3.itertuples()]
        up, down = (df["price_change_pct"] > 0).sum(), (df["price_change_pct"] < 0).sum()
        breadth = f"상승 {up} · 하락 {down}"
        # Top 10 집중도 계산
        vol_col = next((c for c in ["volume_24h", "krw_volume_24h"] if c in df.columns), None)
        if vol_col:
            top10_sum = df.sort_values(vol_col, ascending=False).head(10)[vol_col].sum()
            top10_conc = fmt_share_pct(top10_sum / df[vol_col].sum())

    sent_label, sent_score = extract_sentiment(hist)

    ph = {
        "{{BM20_LEVEL}}": f"{float(level):,.2f}" if level != "—" else "—",
        "{{BM20_1D}}": colored_change_html(bm20_1d),
        "{{BM20_BREADTH}}": breadth,
        "{{SENTIMENT_LABEL}}": sent_label,
        "{{SENTIMENT_SCORE}}": sent_score,
        "{{KR_SHARE_GLOBAL}}": fmt_share_pct(kr_share_global),
        "{{XRP_KR_SHARE}}": fmt_share_pct(xrp_data.get("value") or xrp_data.get("xrp_kr_share")),
        "{{TOP10_CONC_24H}}": top10_conc,
        "{{K_SAFETY}}": fmt_share_pct(k_safety),
        "{{MOVE_1}}": moves[0], "{{MOVE_2}}": moves[1], "{{MOVE_3}}": moves[2],
        "{{KRW_TOTAL_24H}}": f"{krw.get('totals', {}).get('combined_24h', 0)/1e12:.2f}조원",
        "{{DASHBOARD_PREVIEW_IMG_URL}}": f"{BASE_URL}/assets/topcoins_treemap_latest.png",
        "SUBSCRIBE_URL": "https://blockmedia.co.kr/kr"
    }
    
    # 수동 텍스트 추가
    ph["{{NEWS_ONE_LINER}}"] = (NEWS_ONELINER_TXT.read_text(encoding="utf-8").strip() if NEWS_ONELINER_TXT.exists() else "—")
    
    return ph

def render():
    html = TEMPLATE.read_text(encoding="utf-8")
    ph = build_placeholders()
    
    # URL 치환 (Data Request -> News)
    html = html.replace("https://data.blockmedia.co.kr/data-request?utm_source=newsletter&utm_medium=email&utm_campaign=daily_letter&utm_content=request", "https://blockmedia.co.kr/kr")
    
    for k, v in ph.items():
        html = html.replace(k, str(v))
    
    OUT.write_text(html, encoding="utf-8")
    print(f"OK: {OUT} 렌더링 완료")

if __name__ == "__main__":
    render()
