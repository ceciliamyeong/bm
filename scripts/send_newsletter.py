"""
send_newsletter.py
──────────────────
스티비 API를 통해 letter.html을 뉴스레터로 발송합니다.

환경변수:
  STIBEE_API_KEY   : 스티비 API 키 (필수)
  STIBEE_LIST_ID   : 스티비 주소록 ID (필수)

실행:
  python scripts/send_newsletter.py
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

STIBEE_API_KEY = os.environ.get("STIBEE_API_KEY", "")
STIBEE_LIST_ID = os.environ.get("STIBEE_LIST_ID", "")

STIBEE_API_URL = "https://api.stibee.com/v1/letters"

# 발신자 정보 (필요시 수정)
FROM_NAME  = "블록미디어"
FROM_EMAIL = "newsletter@blockmedia.co.kr"

# ── 유효성 체크 ────────────────────────────────────────
def check_env():
    missing = []
    if not STIBEE_API_KEY:
        missing.append("STIBEE_API_KEY")
    if not STIBEE_LIST_ID:
        missing.append("STIBEE_LIST_ID")
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

# ── 스티비 발송 ────────────────────────────────────────
def send(html: str, subject: str):
    headers = {
        "AccessToken": STIBEE_API_KEY,
        "Content-Type": "application/json",
    }

    payload = {
        "name":        subject,          # 캠페인 이름 (내부용)
        "email_title": subject,          # 이메일 제목
        "from_name":   FROM_NAME,
        "from_email":  FROM_EMAIL,
        "html":        html,
        "is_send_now": True,             # 즉시 발송
        "list_ids":    [int(STIBEE_LIST_ID)],
    }

    print(f"[→] 발송 시도: {subject}")
    print(f"    주소록 ID : {STIBEE_LIST_ID}")
    print(f"    발신자    : {FROM_NAME} <{FROM_EMAIL}>")

    resp = requests.post(STIBEE_API_URL, headers=headers, json=payload, timeout=30)

    print(f"[←] HTTP {resp.status_code}")

    if resp.status_code == 200:
        data = resp.json()
        print(f"[✓] 발송 성공!")
        print(f"    응답: {json.dumps(data, ensure_ascii=False, indent=2)}")
    else:
        print(f"[ERROR] 발송 실패")
        print(f"    응답: {resp.text}")
        sys.exit(1)

# ── 메인 ──────────────────────────────────────────────
if __name__ == "__main__":
    check_env()
    html    = load_html()
    subject = make_subject()
    send(html, subject)
