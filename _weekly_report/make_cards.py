#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
보고서 HTML 을 기사에 붙일 '긴 이미지 한 장'으로 저장 (PDF처럼 위에서 아래로 쭉)

    python make_cards.py                     지난주(가장 최근에 끝난 월~일) 보고서
    python make_cards.py --week 2026-10-12

입력  out/weekly_<월요일>_report.html  (make_weekly_report.py 결과)
출력  out/weekly_<월요일>_report_long.png

내용: 제목·기간, BM20, 거래대금, 거래소별, 스테이블코인, 수익률 상위·하위, 거래대금 상위, 집계 기준.
      (시황 글은 기사 본문에 있으므로, 다운로드 버튼과 함께 뺍니다.)
폭 700px 를 2배 해상도(1400px)로 저장합니다.

필요: pip install playwright  그리고  playwright install chromium
"""
import argparse
import os
import sys


def main():
    ap = argparse.ArgumentParser(description="보고서를 긴 이미지 한 장으로 저장")
    ap.add_argument("--week", help="집계 주의 월요일 (기본: 가장 최근에 끝난 주)")
    ap.add_argument("--out-dir", default="out")
    ap.add_argument("--width", type=int, default=700, help="이미지 폭(CSS px, 기본 700)")
    args = ap.parse_args()

    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from run_weekly import last_finished_monday
    week = args.week or last_finished_monday()
    src = os.path.abspath(os.path.join(args.out_dir, f"weekly_{week}_report.html"))
    if not os.path.exists(src):
        sys.exit(f"{src} 가 없어요. 먼저 run_weekly.py 또는 make_weekly_report.py 를 실행하세요.")
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        sys.exit("playwright 가 없어요: pip install playwright  →  playwright install chromium")

    dest = os.path.join(args.out_dir, f"weekly_{week}_report_long.png")
    with sync_playwright() as p:
        b = p.chromium.launch()
        pg = b.new_page(viewport={"width": args.width, "height": 900}, device_scale_factor=2, color_scheme="light")
        pg.goto("file://" + src)
        pg.wait_for_timeout(800)  # 웹폰트 적용 대기
        pg.add_style_tag(content=".story,.nodl,.wmslot{display:none!important}")
        pg.wait_for_timeout(200)
        pg.screenshot(path=dest, full_page=True)
        b.close()
    print("저장:", dest)


if __name__ == "__main__":
    main()
