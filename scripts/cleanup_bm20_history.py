#!/usr/bin/env python3
"""
cleanup_bm20_history.py
───────────────────────
bm20_history.json 중복 제거 — 1회만 실행하세요.

- 같은 날짜 항목이 여러 개면 마지막(최신) 값만 유지
- 날짜순 정렬
- 원본은 bm20_history.backup.json 으로 백업

실행 방법:
  python scripts/cleanup_bm20_history.py
"""

import json
from pathlib import Path
from datetime import datetime

HISTORY_PATH = Path("data/bm20_history.json")
BACKUP_PATH  = Path("data/bm20_history.backup.json")


def main():
    if not HISTORY_PATH.exists():
        print(f"[ERROR] {HISTORY_PATH} 없음")
        return

    with open(HISTORY_PATH, encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        print("[ERROR] bm20_history.json이 배열 형식이 아님")
        return

    before = len(data)

    # 1) 백업
    with open(BACKUP_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"[OK] 백업 완료 → {BACKUP_PATH}")

    # 2) 날짜별 마지막 항목만 유지 (같은 날짜면 나중에 실행된 값 우선)
    seen = {}
    for item in data:
        date = str(item.get("timestamp", ""))[:10]
        if date:
            seen[date] = item  # 덮어쓰기 → 마지막 값 유지

    # 3) 날짜순 정렬
    cleaned = sorted(seen.values(), key=lambda x: str(x.get("timestamp", ""))[:10])

    after = len(cleaned)

    # 4) 저장
    with open(HISTORY_PATH, "w", encoding="utf-8") as f:
        json.dump(cleaned, f, ensure_ascii=False, indent=2)

    print(f"[OK] 정리 완료: {before}개 → {after}개 ({before - after}개 중복 제거)")
    print(f"[OK] 기간: {cleaned[0]['timestamp'][:10]} ~ {cleaned[-1]['timestamp'][:10]}")


if __name__ == "__main__":
    main()
