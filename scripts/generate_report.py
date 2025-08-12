#!/usr/bin/env python3
# Find latest out/YYYY-MM-DD, copy to archive/YYYY-MM-DD, and update index.html
# - Injects news preview (reads bm20_news_YYYY-MM-DD.txt)
# - Creates .nojekyll to avoid Jekyll processing
# - Robust when out/ has no dated folder (falls back to root files)

import os, re, shutil, html
from pathlib import Path
import datetime as dt

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

def ensure_latest_dir() -> Path:
    latest = find_latest_out_dir()
    if latest:
        return latest

    # Fallback: out 루트에 파일만 떨어져있는 경우 임시 날짜 폴더 생성
    root_files = list(OUT.glob("*"))
    if not root_files:
        raise SystemExit(f"[generate_report] no dated folder and no files under {OUT}")
    ts = max(p.stat().st_mtime for p in root_files)
    ymd = dt.datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
    tmp = OUT / ymd
    tmp.mkdir(exist_ok=True)
    for p in root_files:
        if p.is_file():
            shutil.move(str(p), tmp / p.name)
    print(f"[fallback] created {tmp} from root files")
    return tmp

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
        # index.html 안에서 스타일에 종속되지 않도록 가볍게만 지정
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

def main():
    latest = ensure_latest_dir()
    dst = copy_dir(latest)
    update_index(dst)
    (ROOT / ".nojekyll").write_text("", encoding="utf-8")
    print("[done] site updated")

if __name__ == "__main__":
    main()
