#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Send newsletter via Stibee API.
- letter.html    → KR list (STIBEE_LIST_ID)
- letter_en.html → EN list (STIBEE_LIST_ID_EN = 486277)

Required env vars:
  STIBEE_API_KEY
  STIBEE_LIST_ID
  STIBEE_LIST_ID_EN  (optional; skip EN if not set)
  STIBEE_FROM_EMAIL
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent

API_KEY    = os.environ["STIBEE_API_KEY"]
LIST_ID_KR = os.environ["STIBEE_LIST_ID"]
LIST_ID_EN = os.environ.get("STIBEE_LIST_ID_EN", "486277")
FROM_EMAIL = os.environ["STIBEE_FROM_EMAIL"]

STIBEE_URL = "https://api.stibee.com/v1/letters"
HEADERS    = {"AccessToken": API_KEY, "Content-Type": "application/json"}

KST        = timezone(timedelta(hours=9))


def load_html(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"Missing {path}")
    return path.read_text(encoding="utf-8")


def make_subject(lang: str = "kr") -> str:
    now = datetime.now(KST)
    if lang == "kr":
        return f"[블록미디어] {now.strftime('%Y년 %-m월 %-d일')} 크립토 데일리 브리핑"
    return f"[Blockmedia] Crypto Daily Brief · {now.strftime('%b %-d, %Y')}"


def send(list_id: str, subject: str, html: str, label: str) -> None:
    payload = {
        "listId":    list_id,
        "fromEmail": FROM_EMAIL,
        "fromName":  "블록미디어" if label == "KR" else "Blockmedia",
        "subject":   subject,
        "contents":  html,
        "sendType":  "1",  # 즉시발송
    }
    r = requests.post(STIBEE_URL, json=payload, headers=HEADERS, timeout=30)
    if r.status_code in (200, 201):
        print(f"[OK] {label} newsletter sent — status {r.status_code}")
    else:
        print(f"[ERROR] {label} send failed — {r.status_code}: {r.text}")
        sys.exit(1)


def main() -> None:
    # KR 발송
    kr_html = load_html(ROOT / "letter.html")
    send(LIST_ID_KR, make_subject("kr"), kr_html, "KR")

    # EN 발송
    en_path = ROOT / "letter_en.html"
    if en_path.exists():
        en_html = load_html(en_path)
        send(LIST_ID_EN, make_subject("en"), en_html, "EN")
    else:
        print("WARN: letter_en.html not found, skipping EN send")


if __name__ == "__main__":
    main()
