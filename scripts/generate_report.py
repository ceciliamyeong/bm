#!/usr/bin/env python3
# Find latest out/YYYY-MM-DD, copy to archive/YYYY-MM-DD, and update index.html
# + Publish latest: creates bm20/latest.html and bm20_bar_latest.png / bm20_trend_latest.png
# - Injects news preview (reads bm20_news_YYYY-MM-DD.txt)
# - Creates .nojekyll to avoid Jekyll processing
# - Robust when out/ has no dated folder (falls back to root files)

import os, re, shutil, html
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

ROOT  = Path(__file__).resolve().parents[1]
OUT   = ROOT / "out"
ARCH  = ROOT / "archive"
INDEX = ROOT / "index.html"

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
    if dated:
        return sorted(dated, key=lambda p: p.name)[-1]
    return None

def ensure_daily_html(latest_dir: Path) -> Path | None:
    """out/YYYY-MM-DD/ 아래에 bm20_daily_YYYY-MM-DD.html 없으면 자동 생성."""
    ymd = latest_dir.name
    html_path = latest_dir / f"bm20_daily_{ymd}.html"
    if html_path.exists():
        return html_path

    # 필수 이미지/뉴스 확보
    bar = latest_dir / f"bm20_bar_{ymd}.png"
    trd = latest_dir / f"bm20_trend_{ymd}.png"
    news_file = latest_dir / f"bm20_news_{ymd}.txt"
    if not news_file.exists():
        print("[ensure_daily_html] skip: news not found", news_file); return None

    news = news_file.read_text(encoding="utf-8").strip()
    news = html.escape(news).replace("\n","<br/>")
    # 이미지 없어도 HTML은 생성(섹션만 비움)
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
  <div class="card"><h2>BM20 데일리 뉴스</h2><p>{news}</p></div>
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

def read_news_preview(latest_dir: Path, max_chars: int = 380) -> str | None:
    """bm20_news_YYYY-MM-DD.txt 읽어서 미리보기(짧은 문단) 반환"""
    ymd = latest_dir.name
    news_file = latest_dir / f"bm20_news_{ymd}.txt"
    if not news_file.exists():
        return None
    txt = news_file.read_text(encoding="utf-8").strip()
    if not txt:
        return None
    # 첫 문단 위주로, 너무 길면 자르고 … 처리
    txt = txt.replace("\r\n", "\n").replace("\r", "\n")
    para = txt.split("\n")[0] if "\n" in txt else txt
    if len(para) > max_chars:
        para = para[:max_chars].rstrip() + "…"
    return html.escape(para)

def update_index(latest_dir: Path):
    """index.html의 <!--LATEST_START--> ... <!--LATEST_END--> 블록을 오늘자 프리뷰로 교체"""
    if not INDEX.exists():
        print("[warn] index.html not found; skip update")
        return

    ymd = latest_dir.name
    files = {p.name: p for p in latest_dir.iterdir() if p.is_file()}

    links = []
    if f"bm20_daily_{ymd}.html" in files:
        links.append(f'<a href="archive/{ymd}/bm20_daily_{ymd}.html">HTML</a>')
    if f"bm20_daily_{ymd}.pdf" in files:
        links.append(f'<a href="archive/{ymd}/bm20_daily_{ymd}.pdf">PDF</a>')

    img_tag = ""
    if f"bm20_bar_{ymd}.png" in files:
        img_tag = (
            f'<img src="archive/{ymd}/bm20_bar_{ymd}.png" alt="performance" '
            f'style="max-width:100%;border:1px solid #eee;border-radius:8px;margin-top:8px;" />'
        )

    news_html = ""
    preview = read_news_preview(latest_dir)
    if preview:
        news_html = (
            f'<div style="margin-top:10px;padding:10px;border:1px dashed #e0e0e0;'
            f'background:#fafafa;border-radius:8px">'
            f'<strong>BM20 데일리 뉴스</strong><br>{preview}'
            f'</div>'
        )

    block = f"""
<div>
  <strong>Latest: {ymd}</strong> — {' | '.join(links) if links else 'no files'}
  {img_tag}
  {news_html}
</div>
""".strip()

    html_src = INDEX.read_text(encoding="utf-8")
    new_html = re.sub(
        r"(<!--LATEST_START-->)(.*?)(<!--LATEST_END-->)",
        lambda m: f"{m.group(1)}\n{block}\n{m.group(3)}",
        html_src, flags=re.S
    )
    INDEX.write_text(new_html, encoding="utf-8")
    print("[update] index.html latest block (with news) updated")

# --- publish latest (fixed assets) -----------------------------------
def publish_latest(latest_dir: Path):
    """
    Always create under bm20/:
      - bm20/latest.html
      - bm20/bm20_bar_latest.png
      - bm20/bm20_trend_latest.png
    """
    ymd = latest_dir.name
    html_src = latest_dir / f"bm20_daily_{ymd}.html"
    bar_src  = latest_dir / f"bm20_bar_{ymd}.png"
    trd_src  = latest_dir / f"bm20_trend_{ymd}.png"

    if not html_src.exists():
        print("[publish_latest] skip: html not found", html_src)
        return
    if not bar_src.exists() or not trd_src.exists():
        print("[publish_latest] skip: image(s) not found", bar_src, trd_src)
        return

    # 항상 bm20/ 아래로 배포
    target_root = ROOT / "bm20"
    target_root.mkdir(parents=True, exist_ok=True)

    # latest.html: 이미지 경로를 고정 파일명으로 치환
    html_txt = html_src.read_text(encoding="utf-8")
    html_txt = html_txt.replace(f"bm20_bar_{ymd}.png",   "bm20_bar_latest.png")
    html_txt = html_txt.replace(f"bm20_trend_{ymd}.png", "bm20_trend_latest.png")
    (target_root / "latest.html").write_text(html_txt, encoding="utf-8")

    # 최신 이미지 고정 파일명으로 복사
    shutil.copyfile(bar_src, target_root / "bm20_bar_latest.png")
    shutil.copyfile(trd_src, target_root / "bm20_trend_latest.png")

    print(f"[publish_latest] wrote {(target_root / 'latest.html').as_posix()}")
# ---------------------------------------------------------------------

def main():
    latest = ensure_latest_dir()
    dst = copy_dir(latest)
    update_index(dst)
    publish_latest(dst)  # ★ 최신 고정 링크/이미지 생성
    (ROOT / ".nojekyll").write_text("", encoding="utf-8")
    print("[done] site updated")

if __name__ == "__main__":
    main()
