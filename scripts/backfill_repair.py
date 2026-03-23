#!/usr/bin/env python3
"""
backfill_current_basket.csv 누락 구간 복구 스크립트
- out/YYYY-MM-DD/bm20_daily_data_YYYY-MM-DD.csv 를 순서대로 읽어서
- backfill_current_basket.csv 에 없는 날짜를 채워넣는다.

사용법:
  python backfill_repair.py

실행 위치: 레포 루트 (out/ 와 같은 위치)
"""

import csv
import os
from pathlib import Path
from datetime import datetime

ROOT      = Path(__file__).resolve().parents[1]  # scripts/ 상위 = 레포 루트
OUT_DIR   = ROOT / "out"
BACKFILL  = OUT_DIR / "backfill_current_basket.csv"

def load_backfill():
    if not BACKFILL.exists():
        raise FileNotFoundError(f"Not found: {BACKFILL}")
    rows = list(csv.DictReader(BACKFILL.open(encoding="utf-8")))
    return rows

def save_backfill(rows):
    with BACKFILL.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["date", "index", "ret"])
        writer.writeheader()
        writer.writerows(rows)
    print(f"[save] {BACKFILL} ({len(rows)} rows)")

def calc_ret_from_daily_csv(csv_path: Path) -> float:
    """weight_ratio * price_change_pct/100 합산 → 일수익률(ratio)"""
    ret = 0.0
    for r in csv.DictReader(csv_path.open(encoding="utf-8")):
        w   = r.get("weight_ratio")
        pct = r.get("price_change_pct")
        if w in (None, "") or pct in (None, ""):
            continue
        ret += float(w) * (float(pct) / 100.0)
    return ret

def get_dated_out_dirs() -> list[Path]:
    dirs = []
    for p in OUT_DIR.iterdir():
        if not p.is_dir():
            continue
        try:
            datetime.strptime(p.name, "%Y-%m-%d")
            dirs.append(p)
        except ValueError:
            continue
    return sorted(dirs, key=lambda p: p.name)

def main():
    rows = load_backfill()
    existing_dates = {r["date"] for r in rows}
    last_date = rows[-1]["date"]
    last_index = float(rows[-1]["index"])

    print(f"[info] backfill last: {last_date} / index: {last_index:.4f}")
    print(f"[info] existing dates: {len(existing_dates)}")

    dated_dirs = get_dated_out_dirs()
    print(f"[info] out/ 날짜 폴더 수: {len(dated_dirs)}")

    # 누락된 날짜만 골라서 순서대로 처리
    missing = [d for d in dated_dirs if d.name not in existing_dates]
    print(f"[info] 누락 날짜 수: {len(missing)}")

    if not missing:
        print("[done] 누락 없음. 복구 불필요.")
        return

    added = []
    cur_index = last_index
    cur_date  = last_date

    for d in missing:
        date = d.name

        # 날짜가 backfill 마지막보다 이전이면 스킵 (역행 방지)
        if date <= cur_date:
            print(f"[skip] {date} <= cur_date {cur_date}")
            continue

        csv_path = d / f"bm20_daily_data_{date}.csv"
        if not csv_path.exists():
            print(f"[warn] CSV 없음, 스킵: {csv_path}")
            continue

        ret = calc_ret_from_daily_csv(csv_path)
        cur_index = cur_index * (1.0 + ret)
        cur_date  = date

        row = {"date": date, "index": str(cur_index), "ret": str(ret)}
        rows.append(row)
        added.append(row)
        print(f"[add] {date}  ret={ret:+.4%}  index={cur_index:.4f}")

    if not added:
        print("[done] 추가된 날짜 없음.")
        return

    # 날짜 순 정렬 후 저장
    rows.sort(key=lambda r: r["date"])
    save_backfill(rows)
    print(f"\n[done] {len(added)}개 날짜 복구 완료: {added[0]['date']} ~ {added[-1]['date']}")

if __name__ == "__main__":
    main()
