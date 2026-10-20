#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
보고서 HTML 의 카드를 카드별 PNG 이미지로 저장 (기사에 붙일 용도)

    python make_cards.py                     지난주(가장 최근에 끝난 월~일) 보고서
    python make_cards.py --week 2026-10-12

입력  out/weekly_<월요일>_report.html  (make_weekly_report.py 결과)
출력  out/cards_<월요일>/01_bm20.png, 02_total.png, 03_exchanges.png, 04_stable.png, 05_returns.png, 06_top_coins.png

필요: pip install playwright  그리고  playwright install chromium
카드 폭은 모두 같게(휴대폰 레이아웃, 가로 700px) 2배 해상도로 저장합니다. 기사 본문 폭에 맞게 쓰기 좋습니다.
"""
import argparse
import os
import sys

NAMES = [  # (카드 제목에 포함된 말, 파일 이름)
    ("BM20", "bm20"),
    ("5대 거래소 주간 거래대금", "total"),
    ("거래소별", "exchanges"),
    ("스테이블코인", "stable"),
    ("수익률", "returns"),
    ("거래대금 상위", "top_coins"),
    ("증감률", "movers"),
]
SKIP = ("이번 주 시황", "데이터 내려받기")


def main():
    ap = argparse.ArgumentParser(description="보고서 카드를 PNG 로 저장")
    ap.add_argument("--week", help="집계 주의 월요일 (기본: 가장 최근에 끝난 주)")
    ap.add_argument("--out-dir", default="out")
    ap.add_argument("--width", type=int, default=700, help="카드 폭(px, 기본 700)")
    args = ap.parse_args()

    here = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, here)
    from run_weekly import last_finished_monday
    week = args.week or last_finished_monday()
    src = os.path.abspath(os.path.join(args.out_dir, f"weekly_{week}_report.html"))
    if not os.path.exists(src):
        sys.exit(f"{src} 가 없어요. 먼저 run_weekly.py 또는 make_weekly_report.py 를 실행하세요.")
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        sys.exit("playwright 가 없어요: pip install playwright  →  playwright install chromium")

    dest = os.path.join(args.out_dir, f"cards_{week}")
    os.makedirs(dest, exist_ok=True)
    saved = []
    with sync_playwright() as p:
        b = p.chromium.launch()
        pg = b.new_page(viewport={"width": args.width, "height": 900}, device_scale_factor=2, color_scheme="light")
        pg.goto("file://" + src)
        pg.wait_for_timeout(800)  # 웹폰트 적용 대기
        cards = pg.query_selector_all("section.card")
        n = 0
        for c in cards:
            h2 = c.query_selector("h2")
            title = h2.inner_text().strip() if h2 else ""
            if not title or any(s in title for s in SKIP):
                continue
            slug = next((s for k, s in NAMES if k in title), None)
            if not slug:
                continue
            n += 1
            path = os.path.join(dest, f"{n:02d}_{slug}.png")
            c.screenshot(path=path)
            saved.append(path)
        b.close()
    for s in saved:
        print("저장:", s)
    if not saved:
        sys.exit("저장된 카드가 없어요.")


if __name__ == "__main__":
    main()
