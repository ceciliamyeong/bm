#!/usr/bin/env python3
# copies out/YYYY-MM-DD/** → archive/YYYY-MM-DD/** and updates index.html
import os, shutil, re
from pathlib import Path
from datetime import datetime, timezone, timedelta

ROOT = Path(__file__).resolve().parents[1]
OUT  = ROOT / "out"
ARCH = ROOT / "archive"
INDEX= ROOT / "index.html"

KST = timezone(timedelta(hours=9))
today = datetime.now(KST).strftime("%Y-%m-%d")

def copy_today():
    src = OUT / today
    if not src.exists():
        raise SystemExit(f"[generate_report] source not found: {src}")
    dst = ARCH / today
    if dst.exists(): shutil.rmtree(dst)
    shutil.copytree(src, dst)
    print("[copy]", src, "→", dst)
    return dst

def update_index(latest_dir: Path):
    # index.html 안에 다음 마커 사이 내용을 교체
    # <!--LATEST_START--> ... <!--LATEST_END-->
    if not INDEX.exists():
        print("[warn] index.html not found; skip update")
        return
    html = INDEX.read_text(encoding="utf-8")

    # 링크들 구성
    ymd = latest_dir.name
    files = {p.name:p for p in latest_dir.iterdir() if p.is_file()}
    link_html = []
    if f"bm20_daily_{ymd}.html" in files:
        link_html.append(f'<a href="archive/{ymd}/bm20_daily_{ymd}.html">HTML</a>')
    if f"bm20_daily_{ymd}.pdf" in files:
        link_html.append(f'<a href="archive/{ymd}/bm20_daily_{ymd}.pdf">PDF</a>')
    if f"bm20_bar_{ymd}.png" in files:
        img_tag = f'<img src="archive/{ymd}/bm20_bar_{ymd}.png" alt="performance" style="max-width:100%;border:1px solid #eee;border-radius:8px;margin-top:8px;" />'
    else:
        img_tag = ""

    block = f"""
<div>
  <strong>Latest: {ymd}</strong> — {' | '.join(link_html)}
  {img_tag}
</div>
""".strip()

    new_html = re.sub(
        r"(<!--LATEST_START-->)(.*?)(<!--LATEST_END-->)",
        lambda m: f"{m.group(1)}\n{block}\n{m.group(3)}",
        html, flags=re.S
    )
    INDEX.write_text(new_html, encoding="utf-8")
    print("[update] index.html latest block updated")

def main():
    latest = copy_today()
    update_index(latest)

if __name__ == "__main__":
    main()
