"""
send_newsletter.py
──────────────────
스티비 API를 통해 letter.html을 뉴스레터로 발송합니다.

흐름:
  1. 이메일 콘텐츠 업데이트 (POST /emails/{id}/content, Content-Type: text/html)
  2. 발송 트리거 (POST /emails/{id}/send)

환경변수:
  STIBEE_API_KEY   : 스티비 API 키 (필수)
  STIBEE_LIST_ID   : 스티비 주소록 ID (필수)
  STIBEE_EMAIL_ID  : 스티비 이메일 ID (필수)
"""

import os
import sys
import json
import datetime
import requests
from pathlib import Path

# ── 설정 ──────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
LETTER_HTML = ROOT / "letter.html"

STIBEE_API_KEY  = os.environ.get("STIBEE_API_KEY", "")
STIBEE_LIST_ID  = os.environ.get("STIBEE_LIST_ID", "")
STIBEE_EMAIL_ID = os.environ.get("STIBEE_EMAIL_ID", "")

BASE_URL = "https://api.stibee.com/v2"

# ── 유효성 체크 ────────────────────────────────────────
def check_env():
    missing = []
    if not STIBEE_API_KEY:  missing.append("STIBEE_API_KEY")
    if not STIBEE_LIST_ID:  missing.append("STIBEE_LIST_ID")
    if not STIBEE_EMAIL_ID: missing.append("STIBEE_EMAIL_ID")
    if missing:
        print(f"[ERROR] 환경변수 누락: {', '.join(missing)}")
        sys.exit(1)

def load_html() -> str:
    if not LETTER_HTML.exists():
        print(f"[ERROR] letter.html 없음: {LETTER_HTML}")
        sys.exit(1)
    content = LETTER_HTML.read_text(encoding="utf-8")
    print(f"[OK] letter.html 로드 완료 ({len(content):,} bytes)")
    return content

# ── 제목 생성 ──────────────────────────────────────────
def make_subject() -> str:
    today = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9)))
    date_str = today.strftime("%m/%d")
    weekdays = ["월", "화", "수", "목", "금", "토", "일"]
    weekday  = weekdays[today.weekday()]
    return f"[블록미디어] {date_str}({weekday}) 오늘의 크립토 인사이트"

# ── Step 1: HTML 콘텐츠 업데이트 ──────────────────────
def update_content(html: str):
    url = f"{BASE_URL}/emails/{STIBEE_EMAIL_ID}/content"
    headers = {
        "AccessToken":  STIBEE_API_KEY,
        "Content-Type": "text/html",
    }
    print(f"[→] 콘텐츠 업데이트: {url}")
    resp = requests.post(url, headers=headers, data=html.encode("utf-8"), timeout=30)
    print(f"[←] HTTP {resp.status_code}")
    if resp.status_code != 200:
        print(f"[ERROR] 콘텐츠 업데이트 실패: {resp.text}")
        sys.exit(1)
    print("[OK] HTML 콘텐츠 업데이트 완료")

# ── Step 2: 발송 트리거 ────────────────────────────────
def send_email():
    url = f"{BASE_URL}/emails/{STIBEE_EMAIL_ID}/send"
    headers = {
        "AccessToken":  STIBEE_API_KEY,
        "Content-Type": "application/json",
    }
    print(f"[→] 발송 트리거: {url}")
    print(f"    주소록 ID : {STIBEE_LIST_ID}")
    resp = requests.post(url, headers=headers, timeout=30)
    print(f"[←] HTTP {resp.status_code}")
    if resp.status_code == 200:
        print(f"[✓] 발송 성공!")
        print(f"    응답: {resp.text}")
    else:
        print(f"[ERROR] 발송 실패: {resp.text}")
        sys.exit(1)

# ── 메인 ──────────────────────────────────────────────
if __name__ == "__main__":
    check_env()
    html    = load_html()
    subject = make_subject()
    print(f"[제목] {subject}")
    update_content(html)
    send_email()
