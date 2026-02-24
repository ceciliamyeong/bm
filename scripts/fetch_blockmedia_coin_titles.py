# scripts/fetch_blockmedia_coin_titles.py
import json
import re
from datetime import datetime, timezone
from pathlib import Path

import requests

BASE = "https://www.blockmedia.co.kr"
DEFAULT_URL = "https://www.blockmedia.co.kr/coins/btc"

def norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def extract_articles(html: str, limit: int = 8):
    """
    마크업이 바뀌어도 최대한 버티도록:
    - /archives/ 링크만 추출
    - anchor text를 제목으로 사용
    - URL 중복 제거
    """
    links = re.findall(r'href="([^"]*?/archives/[^"]+)"', html)
    # 주변 텍스트(제목)는 간단히: href 주변의 a 태그 텍스트를 다시 훑는다
    # (정교 파싱이 필요하면 BeautifulSoup로 교체 가능)
    items = []
    seen = set()

    # 빠르게 a태그 블록을 통째로 잡고, archives 링크 포함한 것만 처리
    for m in re.finditer(r'<a[^>]+href="([^"]*?/archives/[^"]+)"[^>]*>(.*?)</a>', html, re.S):
        href = m.group(1)
        inner = m.group(2)

        # 태그 제거
        title = re.sub(r"<[^>]+>", "", inner)
        title = norm_space(title)

        if not title or len(title) < 8:
            continue

        if href.startswith("/"):
            href = BASE + href
        if href in seen:
            continue

        seen.add(href)
        items.append({"title": title, "url": href, "source": "Blockmedia"})
        if len(items) >= limit:
            break

    # 혹시 위 루프에서 못 잡으면(마크업 변경) 링크라도 살려둠
    if not items:
        for href in links:
            if href.startswith("/"):
                href = BASE + href
            if href in seen:
                continue
            seen.add(href)
            items.append({"title": "Blockmedia 기사", "url": href, "source": "Blockmedia"})
            if len(items) >= limit:
                break

    return items

def main():
    url = DEFAULT_URL
    r = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0 (BM20Bot/1.0)"})
    r.raise_for_status()

    items = extract_articles(r.text, limit=10)

    payload = {
        "asOf": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "topic": "BTC",
        "sourceUrl": url,
        "items": items,
    }

    # bm20_index.html이 읽기 쉬운 위치 2군데에 저장(루트 + bm/)
    Path("bm").mkdir(parents=True, exist_ok=True)
    Path("bm20_btc_news.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    Path("bm/bm20_btc_news.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

if __name__ == "__main__":
    main()
