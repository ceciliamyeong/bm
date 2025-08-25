#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Publish latest BM20 to /bm/latest.html without touching main page
# - Copies out/YYYY-MM-DD -> archive/YYYY-MM-DD
# - Generates /bm/latest.html and /bm/bm20_bar_latest.png /bm/bm20_trend_latest.png
# - If out/ has no dated folder, falls back to existing /bm/latest.html & *_latest.png

import re, shutil, html
from pathlib import Path
import datetime as dt

DARK_STYLE = """
body{font-family:-apple-system,BlinkMacSystemFont,"NanumGothic","Noto Sans CJK","Malgun Gothic",Arial,sans-serif;
background:#0b1020;color:#e6ebff;margin:0}
.wrap{max-width:820px;margin:0 auto;padding:20px}
.card{background:#121831;border:1px solid rgba(255,255,255,.08);border-radius:12px;padding:20px;margin-bottom:16px}
h1{font-size:22px;margin:0 0 8px 0;text-align:center}
h2{font-size:15px;margin:16px 0 8px 0;color:#cfd6ff}
.muted{color:#99a1b3;text-align:center}.center{text-align:center}
table{width:100%;border-collapse:collapse;font-size:14px}
th,td{border:1px solid rgba(255,255,255,.08);padding:8px}
th{background:#1a2240;color:#e6ebff}
.footer{font-size:12px;color:#99a1b3;text-align:center;margin-top:16px}
img{max-width:100%;background:#0f1429;border-radius:8px;border:1px solid rgba(255,255,255,.08)}
"""

ROOT = Path(__file__).resolve().parents[1]
OUT  = ROOT / "out"
ARCH = ROOT / "archive"
BM   = ROOT / "bm"        # ← 메인 페이지가 있는 폴더(메인은 건드리지 않음)

def is_ymd(name: str) -> bool:
    try:
        dt.datetime.strptime(name, "%Y-%m-%d")
        return True
    except Exception:
        return False

def find_latest_out_dir() -> Path | None:
    if not OUT.exists():
        return None
    dated = [p for p in OUT.iterdir() if p.is_dir() and is_ymd(p.name)]
    return sorted(dated, key=lambda p: p.name)[-1] if dated else None

def ensure_daily_html(latest_dir: Path, require_news: bool = False) -> Path | None:
    """out/YYYY-MM-DD/bm20_daily_YYYY-MM-DD.html 없으면 자동 생성(뉴스 없어도 생성)."""
    ymd = latest_dir.name
    html_path = latest_dir / f"bm20_daily_{ymd}.html"
    if html_path.exists():
        return html_path

    bar = latest_dir / f"bm20_bar_{ymd}.png"
    trd = latest_dir / f"bm20_trend_{ymd}.png"
    news_file = latest_dir / f"bm20_news_{ymd}.txt"

    news = ""
    if news_file.exists():
        news = html.escape(news_file.read_text(encoding="utf-8").strip()).replace("\n", "<br/>")
    elif require_news:
        print("[ensure_daily_html] skip: news required but not found", news_file)
        return None

    bar_tag = f'<p class="center"><img src="bm20_bar_{ymd}.png" alt="Performance"></p>' if bar.exists() else ""
    trd_tag = f'<p class="center"><img src="bm20_trend_{ymd}.png" alt="Trend"></p>' if trd.exists() else ""

    tpl = f"""<!doctype html><html lang="ko"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>BM20 데일리 {ymd}</title>
<style>{DARK_STYLE}</style></head><body>
<div class="wrap">
  <div class="card">
    <h1>BM20 데일리 리포트</h1>
    <div class="muted">{ymd}</div>
  </div>
  <div class="card">
    <h2>코인별 퍼포먼스 (1D, USD)</h2>
    {bar_tag}
    <h2>BTC & ETH 7일 가격 추세</h2>
    {trd_tag}
  </div>
  <div class="card"><h2>BM20 데일리 뉴스</h2><p>{news or "—"}</p></div>
  <div class="footer">© Blockmedia · Data: CoinGecko, Upbit, Binance/Bybit</div>
</div></body></html>"""
    html_path.write_text(tpl, encoding="utf-8")
    print("[auto] generated", html_path)
    return html_path

def copy_dir(src: Path) -> Path:
    dst = ARCH / src.name
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(src, dst)
    print("[copy]", src, "→", dst)
    return dst

def publish_latest_to_bm(dst_daily_dir: Path):
    """
    /bm/ 아래에 고정 산출물만 생성/교체:
      - /bm/latest.html
      - /bm/bm20_bar_latest.png
      - /bm/bm20_trend_latest.png
    (메인 index.html 은 절대 수정하지 않음)
    """
    ymd = dst_daily_dir.name
    html_src = dst_daily_dir / f"bm20_daily_{ymd}.html"
    bar_src  = dst_daily_dir / f"bm20_bar_{ymd}.png"
    trd_src  = dst_daily_dir / f"bm20_trend_{ymd}.png"

    if not html_src.exists():
        print("[publish_latest] skip: html not found", html_src)
        return

    BM.mkdir(parents=True, exist_ok=True)

    # latest.html: 이미지 경로를 고정 파일명으로 치환
    html_txt = html_src.read_text(encoding="utf-8")
    html_txt = html_txt.replace(f"bm20_bar_{ymd}.png",   "bm20_bar_latest.png")
    html_txt = html_txt.replace(f"bm20_trend_{ymd}.png", "bm20_trend_latest.png")
    (BM / "latest.html").write_text(html_txt, encoding="utf-8")

    # 최신 이미지 고정 파일명으로 복사(없으면 건너뜀)
    if bar_src.exists():
        shutil.copyfile(bar_src, BM / "bm20_bar_latest.png")
    if trd_src.exists():
        shutil.copyfile(trd_src, BM / "bm20_trend_latest.png")

    print(f"[publish_latest] wrote {(BM / 'latest.html').as_posix()}")

def ensure_latest_dir() -> Path:
    """
    최신 out/YYYY-MM-DD 디렉터리를 보장하여 반환.
    - 있으면 그대로 사용
    - 없으면 /bm 의 고정 에셋(latest.html, *_latest.png)로 오늘 폴더를 생성해 채운 뒤 반환
    - 이후 ensure_daily_html()로 일간 HTML 생성 시도
    """
    latest = find_latest_out_dir()
    if latest is None:
        today = dt.date.today().isoformat()
        latest = OUT / today
        latest.mkdir(parents=True, exist_ok=True)

        bar_fixed  = BM / "bm20_bar_latest.png"
        trd_fixed  = BM / "bm20_trend_latest.png"
        html_fixed = BM / "latest.html"

        # 고정 이미지 → 날짜 파일명으로 복사
        if bar_fixed.exists():
            shutil.copyfile(bar_fixed, latest / f"bm20_bar_{today}.png")
        if trd_fixed.exists():
            shutil.copyfile(trd_fixed, latest / f"bm20_trend_{today}.png")

        # latest.html이 있으면 날짜 파일명으로 치환해 저장
        if html_fixed.exists():
            html_txt = html_fixed.read_text(encoding="utf-8")
            html_txt = html_txt.replace("bm20_bar_latest.png",   f"bm20_bar_{today}.png")
            html_txt = html_txt.replace("bm20_trend_latest.png", f"bm20_trend_{today}.png")
            (latest / f"bm20_daily_{today}.html").write_text(html_txt, encoding="utf-8")

    # 일간 HTML 보장(뉴스 없어도 생성)
    ensure_daily_html(latest, require_news=False)
    return latest

def main():
    latest = ensure_latest_dir()
    dst = copy_dir(latest)          # archive/YYYY-MM-DD
    publish_latest_to_bm(dst)       # /bm/latest.html & *_latest.png
    print("[done] site updated (bm/latest only)")

if __name__ == "__main__":
    main()
