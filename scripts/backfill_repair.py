#!/usr/bin/env python3
"""
backfill_current_basket.csv 누락/오염 구간 복구 스크립트
- 2026-02-25 이후 오염된 행 제거 후 out/ 폴더 기준으로 재계산
- 단일 종목 +-50% 초과 이상값은 0으로 클램프

사용법:
  python scripts/backfill_repair.py
"""

import csv
from pathlib import Path
from datetime import datetime

ROOT     = Path(__file__).resolve().parents[1]  # scripts/ 상위 = 레포 루트
OUT_DIR  = ROOT / "out"
BACKFILL = OUT_DIR / "backfill_current_basket.csv"

# 이 날짜 이후 행을 제거하고 재계산 (오염 시작일)
REPAIR_FROM = "2026-02-25"


def load_backfill():
    if not BACKFILL.exists():
        raise FileNotFoundError("Not found: " + str(BACKFILL))
    return list(csv.DictReader(BACKFILL.open(encoding="utf-8")))


def save_backfill(rows):
    with BACKFILL.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["date", "index", "ret"])
        writer.writeheader()
        writer.writerows(rows)
    print("[save] " + str(BACKFILL) + " (" + str(len(rows)) + " rows)")


def calc_ret_from_daily_csv(csv_path):
    ret = 0.0
    for r in csv.DictReader(csv_path.open(encoding="utf-8")):
        w   = r.get("weight_ratio")
        pct = r.get("price_change_pct")
        sym = r.get("symbol", "?")
        if w in (None, "") or pct in (None, ""):
            continue
        pct_val = float(pct)
        if abs(pct_val) > 50:
            print("[clamp] " + sym + " pct=" + str(round(pct_val, 2)) + " -> 0 (이상값 제외)")
            pct_val = 0.0
        ret += float(w) * (pct_val / 100.0)
    return ret


def get_dated_out_dirs():
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
    print("[info] 원본 마지막: " + rows[-1]["date"] + " / index: " + str(round(float(rows[-1]["index"]), 4)))
    print("[info] 원본 rows: " + str(len(rows)))

    # 오염 시작일 이후 제거
    rows = [r for r in rows if r["date"] < REPAIR_FROM]
    print("[remove] " + REPAIR_FROM + " 이후 제거 -> 남은 rows: " + str(len(rows)))
    print("[info] 제거 후 마지막: " + rows[-1]["date"] + " / index: " + str(round(float(rows[-1]["index"]), 4)))

    date_to_index = {r["date"]: float(r["index"]) for r in rows}
    existing_dates = set(date_to_index.keys())

    dated_dirs = get_dated_out_dirs()
    print("[info] out/ 날짜 폴더 수: " + str(len(dated_dirs)))

    missing = [d for d in dated_dirs if d.name not in existing_dates]
    print("[info] 재계산 날짜 수: " + str(len(missing)))

    if not missing:
        print("[done] 재계산 대상 없음.")
        return

    all_dates = sorted(list(existing_dates) + [d.name for d in missing])
    added = 0

    for d in missing:
        date = d.name
        csv_path = d / ("bm20_daily_data_" + date + ".csv")
        if not csv_path.exists():
            print("[warn] CSV 없음, 스킵: " + str(csv_path))
            continue

        idx = all_dates.index(date)
        prev_index = None
        for i in range(idx - 1, -1, -1):
            prev_date = all_dates[i]
            if prev_date in date_to_index:
                prev_index = date_to_index[prev_date]
                break

        if prev_index is None:
            print("[warn] " + date + " 이전 index 없음, 스킵")
            continue

        ret = calc_ret_from_daily_csv(csv_path)
        new_index = prev_index * (1.0 + ret)
        date_to_index[date] = new_index

        rows.append({"date": date, "index": str(new_index), "ret": str(ret)})
        added += 1
        print("[add] " + date + "  ret=" + str(round(ret * 100, 4)) + "%  index=" + str(round(new_index, 4)))

    if added == 0:
        print("[done] 추가된 날짜 없음.")
        return

    rows.sort(key=lambda r: r["date"])
    save_backfill(rows)
    print("\n[done] " + str(added) + "개 복구 완료: " + missing[0].name + " ~ " + missing[-1].name)


if __name__ == "__main__":
    main()
