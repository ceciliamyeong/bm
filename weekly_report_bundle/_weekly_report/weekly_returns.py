#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
주간 수익률 수집 (원화 기준)

    월요일 00:00(KST) 첫 1시간봉의 시가  ->  일요일 23시 마지막 1시간봉의 종가
    거래대금 집계와 같은 구간입니다.

가격은 종목마다 한 거래소의 원화 가격을 씁니다 (업비트 우선, 없으면 빗썸).
   - 업비트·빗썸에 없는 종목(코인원·디지털X·고팍스 단독 상장)은 수익률에서 제외합니다.
   - 주중에 새로 상장했거나 주 중간에 거래가 끊긴 종목은 '완전한 한 주'가 아니라서 순위에서 빼도록 표시합니다.

사용 (cmd)  weekly_volume_collector.py 와 같은 폴더에서
    python weekly_returns.py --week 2026-10-12
    python weekly_returns.py --week 2026-10-12 --min-krw-eok 100     (주간 거래대금 100억 이상 종목만)

입력  out/weekly_<월요일>_by_coin.csv  (weekly_volume_collector.py 결과)
출력  data/returns_<월요일>.csv        (make_weekly_report.py 가 읽음)
"""
import argparse
import csv
import os
import sys
from datetime import datetime, timedelta

from weekly_volume_collector import (KST, NAIVE_FMT, SLEEP, UPBIT_LIKE, get_json, kst_str_to_ms,
                                     markets_upbit_like, to_ms)

DATA_DIR = "data"
OUT_DIR = "out"
EDGE_HOURS = 3  # 첫/마지막 봉이 주 경계에서 이 시간 이내여야 '완전한 한 주'로 본다


def week_prices(ex, native, start, end):
    """한 종목의 주간 (시가, 종가, 첫 봉 ms, 마지막 봉 ms). 거래가 없으면 None."""
    base, naive_tz = UPBIT_LIKE[ex]
    s_ms, e_ms = to_ms(start), to_ms(end)
    to_cursor = end.astimezone(naive_tz).strftime(NAIVE_FMT)  # 'to' 미포함
    rows = {}
    for _ in range(3):  # 주 168개 봉 -> 200개 한 번이면 충분, 안전하게 최대 3번
        data = get_json(f"{base}/v1/candles/minutes/60",
                        {"market": native, "count": 200, "to": to_cursor}, SLEEP[ex])
        if not data:
            break
        oldest = None
        for c in data:
            ms = kst_str_to_ms(c["candle_date_time_kst"])
            oldest = ms if oldest is None else min(oldest, ms)
            if s_ms <= ms < e_ms:
                rows[ms] = (float(c["opening_price"]), float(c["trade_price"]))
        if oldest <= s_ms or len(data) < 200:
            break
        to_cursor = datetime.fromtimestamp(oldest / 1000, tz=KST).astimezone(naive_tz).strftime(NAIVE_FMT)
    if not rows:
        return None
    first, last = min(rows), max(rows)
    return rows[first][0], rows[last][1], first, last


def main():
    ap = argparse.ArgumentParser(description="주간 수익률 수집 (업비트/빗썸 1시간봉)")
    ap.add_argument("--week", required=True, help="집계 주의 월요일 (YYYY-MM-DD)")
    ap.add_argument("--min-krw-eok", type=float, default=100, help="이 금액(억원) 미만 종목은 건너뜀 (기본 100)")
    ap.add_argument("--refresh", action="store_true", help="저장본이 있어도 다시 받음")
    args = ap.parse_args()

    ws = datetime.strptime(args.week, "%Y-%m-%d").replace(tzinfo=KST)
    if ws.weekday() != 0:
        sys.exit("--week 는 월요일이어야 해요.")
    we = ws + timedelta(days=7)
    if datetime.now(KST) < we:
        sys.exit("아직 끝나지 않은 주예요.")

    src = os.path.join(OUT_DIR, f"weekly_{args.week}_by_coin.csv")
    if not os.path.exists(src):
        sys.exit(f"{src} 가 없어요. 먼저 weekly_volume_collector.py --week {args.week} 를 실행하세요.")
    vol = {}
    for r in csv.DictReader(open(src, encoding="utf-8-sig")):
        vol[r["종목"]] = float(r["주간거래대금_원"])
    targets = [c for c, v in vol.items() if v >= args.min_krw_eok * 1e8]

    markets = {ex: {m["coin"]: m["native"] for m in markets_upbit_like(ex)} for ex in ("upbit", "bithumb")}

    os.makedirs(DATA_DIR, exist_ok=True)
    out_path = os.path.join(DATA_DIR, f"returns_{args.week}.csv")
    done = {}
    if os.path.exists(out_path) and not args.refresh:
        for r in csv.DictReader(open(out_path, encoding="utf-8-sig")):
            done[r["coin"]] = r

    rows, skipped, failed = [], [], []
    for i, coin in enumerate(targets, 1):
        if coin in done:
            rows.append(done[coin])
            continue
        ex = next((e for e in ("upbit", "bithumb") if coin in markets[e]), None)
        if not ex:
            skipped.append(coin)
            continue
        try:
            p = week_prices(ex, markets[ex][coin], ws, we)
        except Exception as e:
            failed.append((coin, str(e)[:80]))
            continue
        if not p:
            skipped.append(coin)
            continue
        o, c, f_ms, l_ms = p
        full = (f_ms - to_ms(ws) <= EDGE_HOURS * 3_600_000) and (to_ms(we) - l_ms <= (EDGE_HOURS + 1) * 3_600_000)
        rows.append({"coin": coin, "exchange": ex, "open": o, "close": c,
                     "ret_pct": f"{(c / o - 1) * 100:.4f}" if o else "", "full_week": "Y" if full else "N"})
        if i % 50 == 0:
            print(f"  {i}/{len(targets)}")

    with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=["coin", "exchange", "open", "close", "ret_pct", "full_week"])
        w.writeheader()
        w.writerows(rows)
    print(f"\n수익률 {len(rows)}개 종목 저장: {out_path}")
    if skipped:
        print(f"업비트·빗썸에 없어 제외 {len(skipped)}개: {', '.join(skipped[:15])}{' …' if len(skipped) > 15 else ''}")
    if failed:
        print(f"실패 {len(failed)}개 (다시 실행하면 이어서 받음): {', '.join(c for c, _ in failed[:10])}")


if __name__ == "__main__":
    main()
