"""
fetch_top_news.py
─────────────────────────────────────────────────────────────────
워드프레스 REST API에서 '뉴스레터' 태그가 달린 최신 3개 기사를 가져와
out/latest/top_news_latest.json 에 저장한다.

WP 관리자 → 태그 → '뉴스레터' 태그 ID 확인 후 NEWSLETTER_TAG_ID 설정.
태그 ID 확인 방법:
  GET https://blockmedia.co.kr/wp-json/wp/v2/tags?search=뉴스레터
"""

import json
import os
import re
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests

# ── 설정 ──────────────────────────────────────────────────────────
WP_BASE_URL       = os.environ.get("WP_BASE_URL", "https://blockmedia.co.kr")
NEWSLETTER_TAG_ID = int(os.environ.get("NEWSLETTER_TAG_ID", "28978"))  # 뉴스레터 태그 ID
MAX_ARTICLES      = 3   # 뉴스 기사 최대 수
MAX_FETCH         = 6   # API 요청 수 (한 줄 글 포함해서 여유있게)
REQUEST_TIMEOUT   = 15

OUT_DIR  = Path(__file__).resolve().parent.parent / "out" / "latest"
OUT_FILE = OUT_DIR / "top_news_latest.json"

KST = timezone(timedelta(hours=9))

# ── HTML 태그 제거 헬퍼 ────────────────────────────────────────────
def strip_html(text: str) -> str:
    text = re.sub(r"<[^>]+>", "", text or "")
    text = text.replace("&nbsp;", " ").replace("&amp;", "&")
    text = text.replace("&lt;", "<").replace("&gt;", ">").replace("&quot;", '"')
    return re.sub(r"\s+", " ", text).strip()


def truncate(text: str, max_len: int = 120) -> str:
    text = strip_html(text)
    if len(text) <= max_len:
        return text
    return text[:max_len].rsplit(" ", 1)[0] + "…"


# ── 태그 ID 자동 조회 ─────────────────────────────────────────────
def resolve_tag_id(tag_name: str = "뉴스레터") -> int:
    """태그명으로 ID 자동 조회 (NEWSLETTER_TAG_ID=0 일 때 폴백)"""
    try:
        res = requests.get(
            f"{WP_BASE_URL}/wp-json/wp/v2/tags",
            params={"search": tag_name, "per_page": 5},
            timeout=REQUEST_TIMEOUT,
        )
        res.raise_for_status()
        items = res.json()
        for item in items:
            if item.get("name") == tag_name:
                return int(item["id"])
        # 완전 일치 없으면 첫 번째
        if items:
            return int(items[0]["id"])
    except Exception as e:
        print(f"WARN: 태그 ID 조회 실패: {e}")
    return 0


# ── 썸네일 URL 조회 ───────────────────────────────────────────────
def get_thumbnail_url(featured_media_id: int) -> str:
    if not featured_media_id:
        return ""
    try:
        res = requests.get(
            f"{WP_BASE_URL}/wp-json/wp/v2/media/{featured_media_id}",
            timeout=REQUEST_TIMEOUT,
        )
        res.raise_for_status()
        data = res.json()
        # medium_large → medium → full 순으로 시도
        sizes = data.get("media_details", {}).get("sizes", {})
        for size in ("medium_large", "medium", "full"):
            if size in sizes:
                return sizes[size].get("source_url", "")
        return data.get("source_url", "")
    except Exception as e:
        print(f"WARN: 썸네일 조회 실패 (media_id={featured_media_id}): {e}")
        return ""


# ── 카테고리명 조회 ───────────────────────────────────────────────
_cat_cache: dict[int, str] = {}

def get_category_name(cat_id: int) -> str:
    if cat_id in _cat_cache:
        return _cat_cache[cat_id]
    try:
        res = requests.get(
            f"{WP_BASE_URL}/wp-json/wp/v2/categories/{cat_id}",
            timeout=REQUEST_TIMEOUT,
        )
        res.raise_for_status()
        name = res.json().get("name", "")
        _cat_cache[cat_id] = name
        return name
    except Exception:
        return ""


# ── 메인 ─────────────────────────────────────────────────────────
def fetch_tagged_posts(tag_id: int) -> tuple[str, list[dict]]:
    """
    '뉴스레터' 태그 글을 가져와서 분리:
      - 본문이 비어있는 글 → 오늘의 한 줄 (제목 사용)
      - 본문이 있는 글     → 뉴스 기사 (최대 3개)
    반환: (today_quote, articles)
    """
    params = {
        "tags":     tag_id,
        "per_page": MAX_FETCH,
        "orderby":  "date",
        "order":    "desc",
        "_fields":  "id,title,excerpt,content,link,date,featured_media,categories,acf",
    }
    res = requests.get(
        f"{WP_BASE_URL}/wp-json/wp/v2/posts",
        params=params,
        timeout=REQUEST_TIMEOUT,
    )
    res.raise_for_status()
    posts = res.json()

    today_quote = ""
    articles    = []

    for post in posts:
        acf      = post.get("acf") or {}
        body     = strip_html(post.get("content", {}).get("rendered", ""))
        title    = strip_html(post["title"].get("rendered", ""))

        # 본문이 비어있으면 → 오늘의 한 줄 전용 글
        if not body.strip():
            if not today_quote:   # 첫 번째 것만 사용
                today_quote = title
            continue

        # 본문 있으면 → 뉴스 기사
        if len(articles) >= MAX_ARTICLES:
            continue

        cat_ids   = post.get("categories", [])
        cat_name  = get_category_name(cat_ids[0]) if cat_ids else "뉴스"

        # 썸네일: featured_media → fifu_image_url 순
        thumb_url = get_thumbnail_url(post.get("featured_media", 0))
        if not thumb_url:
            thumb_url = strip_html(acf.get("fifu_image_url") or "")

        articles.append({
            "id":        post["id"],
            "title":     title,
            "excerpt":   truncate(post["excerpt"].get("rendered", ""), 100),
            "link":      post["link"],
            "date":      post.get("date", ""),
            "category":  cat_name,
            "thumbnail": thumb_url,
        })

    return today_quote, articles


def main():
    global NEWSLETTER_TAG_ID

    if NEWSLETTER_TAG_ID == 0:
        print("INFO: NEWSLETTER_TAG_ID 미설정 → 태그명으로 자동 조회")
        NEWSLETTER_TAG_ID = resolve_tag_id("뉴스레터")
        if NEWSLETTER_TAG_ID == 0:
            print("ERROR: '뉴스레터' 태그를 찾을 수 없습니다.")
            sys.exit(1)
        print(f"INFO: 태그 ID = {NEWSLETTER_TAG_ID}")

    print(f"INFO: WP에서 뉴스레터 글 가져오는 중... (tag_id={NEWSLETTER_TAG_ID})")
    today_quote, articles = fetch_tagged_posts(NEWSLETTER_TAG_ID)

    if not today_quote:
        print("WARN: 오늘의 한 줄 글 없음 (본문 빈 글 없음)")
    else:
        print(f"INFO: 오늘의 한 줄 → {today_quote}")

    if not articles:
        print("WARN: 뉴스 기사 없음")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    output = {
        "fetched_at":   datetime.now(KST).isoformat(),
        "tag_id":       NEWSLETTER_TAG_ID,
        "today_quote":  today_quote,
        "count":        len(articles),
        "items":        articles,
    }

    OUT_FILE.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"OK: 저장 완료 → {OUT_FILE}")
    for i, a in enumerate(articles, 1):
        print(f"  뉴스 {i}. {a['title'][:60]}")


if __name__ == "__main__":
    main()
