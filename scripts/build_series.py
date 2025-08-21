#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Minimal builder for out/series.json
- 목적: CI 통과용 기본 파일 생성
- 추후 실제 데이터 생성 로직으로 대체
"""
from pathlib import Path
import json, sys, os, datetime

OUT = Path("out/series.json")
OUT.parent.mkdir(parents=True, exist_ok=True)

# 기본 스켈레톤 (필요 시 키 추가 가능)
payload = {
    "index": "BM20",
    "updated_at": datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
    "series": [],           # <- 실제 시계열 데이터 채우기 전까지 빈 리스트
    "meta": {
        "source": "placeholder",
        "note": "replace with real BM20 series builder",
    },
}

# 만약 다른 산출물이 있으면(예: out/latest.json), 일부 필드만 반영해서 보강
latest_path = Path("out/latest.json")
if latest_path.exists():
    try:
        latest = json.loads(latest_path.read_text(encoding="utf-8"))
        payload["meta"]["latest_available"] = True
        payload["meta"]["latest_sample_keys"] = list(latest.keys())[:10]
    except Exception as e:
        payload["meta"]["latest_available"] = False
        payload["meta"]["latest_error"] = str(e)

OUT.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
print(f"[build] wrote: {OUT.resolve()}")
