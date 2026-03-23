#!/usr/bin/env python3
"""
backfill_current_basket.csv 누락 구간 복구 스크립트
- out/YYYY-MM-DD/bm20_daily_data_YYYY-MM-DD.csv 를 순서대로 읽어서
- backfill_current_basket.csv 에 없는 날짜를 올바른 위치에 삽입한다.

사용법:
  python scripts/backfill_repair.py

실행 위치: 레포 루트 (out/ 와 같은 위치)
"""

import csv
from pathlib import Path
from datetime import datetime

ROOT     = Path(__file__).resolve().parents[1]  # scripts/ 상위 = 레포 루트
OUT_DIR  = ROOT / "out"
BACKFILL = OUT_DIR / "backfill_current_basket.csv"

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
    ret = 0.0
    for r in csv.DictReader(csv_path.open(encoding="utf-8")):
        w   = r.get("weight_ratio")
        pct = r.get("price_change_pct")
        if w in (None, "") or pct in (None, ""):
            continue
        pct_val = float(pct)
        # 이상값 클램프: 단일 종목 일간 등락이 +-50% 초과면 0 처리
        if abs(pct_val) > 50:
            print(f"[clamp] {r.get("symbol","?")} pct={pct_val:.2f} → 0 (이상값 제외)")
            pct_val = 0.0
        ret += float(w) * (pct_val / 100.0)
    return ret

def get_dated_out_dirs() -> list:
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
    # date → index 맵
    date_to_index = {r["date"]: float(r["index"]) for r in rows}
    existing_dates = set(date_to_index.keys())

    print(f"[info] backfill 마지막: {rows[-1]['date']} / index: {float(rows[-1]['index']):.4f}")
    print(f"[info] existing dates: {len(existing_dates)}")

    dated_dirs = get_dated_out_dirs()
    print(f"[info] out/ 날짜 폴더 수: {len(dated_dirs)}")

    # 2026-02-25부터 오염 — 해당일 이후 전부 재계산
    REPAIR_FROM = "2026-02-25"
    rows = remove_from_date(rows, REPAIR_FROM)
    date_to_index = {r["date"]: float(r["index"]) for r in rows}
    existing_dates = set(date_to_index.keys())

    missing = [d for d in dated_dirs if d.name not in existing_dates]
    print(f"[info] 누락 날짜 수: {len(missing)}")

    if not missing:
        print("[done] 누락 없음.")
        return

    added = 0
    all_dates = sorted(list(existing_dates) + [d.name for d in missing])

    for d in missing:
        date = d.name
        csv_path = d / f"bm20_daily_data_{date}.csv"
        if not csv_path.exists():
            print(f"[warn] CSV 없음, 스킵: {csv_path}")
            continue

        # 이 날짜 바로 이전 날짜의 index 찾기
        idx = all_dates.index(date)
        prev_index = None
        for i in range(idx - 1, -1, -1):
            prev_date = all_dates[i]
            if prev_date in date_to_index:
                prev_index = date_to_index[prev_date]
                break

        if prev_index is None:
            print(f"[warn] {date} 이전 index 없음, 스킵")
            continue

        ret = calc_ret_from_daily_csv(csv_path)
        new_index = prev_index * (1.0 + ret)

        date_to_index[date] = new_index
        rows.append({"date": date, "index": str(new_index), "ret": str(ret)})
        added += 1
        print(f"[add] {date}  ret={ret:+.4%}  index={new_index:.4f}")

    if added == 0:
        print("[done] 추가된 날짜 없음.")
        return

    # 날짜 순 정렬 후 저장
    rows.sort(key=lambda r: r["date"])
    save_backfill(rows)
    print(f"\n[done] {added}개 날짜 복구 완료")

if __name__ == "__main__":
    main()


def remove_from_date(rows, from_date):
    """from_date 이후 행 제거 (재계산용)"""
    kept = [r for r in rows if r["date"] < from_date]
    removed = len(rows) - len(kept)
    print(f"[remove] {from_date} 이후 {removed}개 행 제거")
    return kept
