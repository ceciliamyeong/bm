#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
주간 보고서 한 번에 실행 (수집 -> 수익률 -> 시황 글 -> 보고서)

    python run_weekly.py                  지난주(가장 최근에 끝난 월~일)를 자동으로 계산해서 실행
    python run_weekly.py --week 2026-10-12   특정 주 지정
    python run_weekly.py --dry-run           실행하지 않고 계산한 주와 명령만 출력

같은 폴더에 weekly_volume_collector.py, weekly_returns.py, make_article.py, make_weekly_report.py 가 있어야 합니다.
결과: out/weekly_<월요일>_*.csv|json|html, data/ (원자료), logs/run_<월요일>.log

윈도우 작업 스케줄러에 등록 (cmd, 매주 월요일 오전 9시 예시)
    schtasks /create /tn "MarketLabWeekly" /sc weekly /d MON /st 09:00 ^
      /tr "\"C:\\경로\\python.exe\" \"C:\\경로\\run_weekly.py\"" /f
(작업 폴더는 스크립트가 자기 위치로 맞춥니다. PC가 켜져 있어야 실행됩니다.)
"""
import argparse
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone

KST = timezone(timedelta(hours=9))
HERE = os.path.dirname(os.path.abspath(__file__))


def last_finished_monday(now=None):
    now = now or datetime.now(KST)
    this_monday = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    return (this_monday - timedelta(days=7)).strftime("%Y-%m-%d")


def steps(week, extra_report_args, bm20_dir=None):
    py = sys.executable
    bm = ["--bm20-dir", bm20_dir] if bm20_dir else []
    return [
        [py, "weekly_volume_collector.py", "--week", week],
        [py, "weekly_returns.py", "--week", week],
        [py, "make_article.py", "--week", week] + bm,
        [py, "make_weekly_report.py", "--week", week] + bm + extra_report_args,
    ]


def main():
    ap = argparse.ArgumentParser(description="주간 보고서 일괄 실행")
    ap.add_argument("--week", help="집계 주의 월요일 (기본: 가장 최근에 끝난 주)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--bm20-dir", help="bm 레포 사본 경로 (없으면 GitHub raw 에서 읽음)")
    ap.add_argument("report_args", nargs="*", help="make_weekly_report.py 에 그대로 넘길 옵션 (예: --min-listings 2)")
    args = ap.parse_args()

    week = args.week or last_finished_monday()
    if datetime.strptime(week, "%Y-%m-%d").weekday() != 0:
        sys.exit("--week 는 월요일이어야 해요.")
    cmds = steps(week, args.report_args, args.bm20_dir)

    print(f"대상 주: {week} ~ {(datetime.strptime(week, '%Y-%m-%d') + timedelta(days=6)):%Y-%m-%d}")
    for c in cmds:
        print("  " + " ".join(os.path.basename(x) if x == sys.executable else x for x in c))
    if args.dry_run:
        return

    os.chdir(HERE)
    os.makedirs("logs", exist_ok=True)
    log_path = os.path.join("logs", f"run_{week}.log")
    with open(log_path, "a", encoding="utf-8") as log:
        log.write(f"\n===== {datetime.now(KST):%Y-%m-%d %H:%M:%S} 시작 =====\n")
        for c in cmds:
            name = c[1]
            print(f"\n▶ {name}")
            log.write(f"\n▶ {' '.join(c[1:])}\n")
            log.flush()
            p = subprocess.run(c, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding="utf-8", errors="replace")
            print(p.stdout)
            log.write(p.stdout)
            if p.returncode != 0 and name == "make_article.py":
                msg = f"(경고) {name} 실패 - 시황 글 없이 보고서를 만듭니다. 로그: {log_path}"
                print(msg); log.write(msg + "\n")
                continue
            if p.returncode != 0:
                msg = f"{name} 가 실패했어요 (코드 {p.returncode}). 로그: {log_path}"
                log.write(msg + "\n")
                sys.exit(msg)
        log.write("완료\n")
    print(f"\n완료: out/weekly_{week}_report.html  (로그 {log_path})")


if __name__ == "__main__":
    main()
