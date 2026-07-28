"""
차트 이미지 분석 프로토타입
사용법: python3 analyze_chart.py <이미지경로>
필요: 환경변수 ANTHROPIC_API_KEY
"""
import sys
import base64
import json
import urllib.request

def analyze_chart(image_path: str) -> str:
    with open(image_path, "rb") as f:
        image_b64 = base64.b64encode(f.read()).decode("utf-8")

    media_type = "image/png" if image_path.lower().endswith(".png") else "image/jpeg"

    prompt = (
        "이 이미지는 사용자가 올린 주식/코인 차트야. 화질이 좋지 않거나, 이평선·거래량 같은 "
        "지표가 안 보이거나, 특정 구간만 잘려 있어도 괜찮으니 보이는 정보 안에서 최대한 분석해줘. "
        "완전히 차트가 아니거나 도저히 가격 흐름을 알 수 없는 이미지일 때만 분석을 생략하고 그 이유를 짧게 알려줘.\n\n"
        "다음 항목을 한국어로 간결하게 정리해줘 (보이지 않는 항목은 자연스럽게 생략):\n"
        "1. 전반적 추세 (상승/하락/횡보)\n"
        "2. 이동평균선 배열 상태 (정배열/역배열/혼조, 보이는 경우)\n"
        "3. 거래량 특이사항 (보이는 경우)\n"
        "4. 주요 캔들 패턴 (있다면)\n\n"
        "마지막에 두 가지를 덧붙여줘:\n"
        "- 이번 이미지에서 아쉬웠던 점이 있으면(화질, 잘린 구간, 지표 미표시 등) '다음엔 이렇게 올려주시면 더 정확해요' "
        "형태로 한 줄 팁 (문제 없었으면 생략)\n"
        "- '이 분석은 투자 조언이 아니며 참고용입니다' 문구"
    )

    body = {
        "model": "claude-sonnet-4-6",
        "max_tokens": 800,
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "image", "source": {"type": "base64", "media_type": media_type, "data": image_b64}},
                    {"type": "text", "text": prompt},
                ],
            }
        ],
    }

    import os
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("환경변수 ANTHROPIC_API_KEY가 설정되어 있지 않습니다.")

    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
        },
        method="POST",
    )

    with urllib.request.urlopen(req) as resp:
        data = json.loads(resp.read().decode("utf-8"))

    text_blocks = [b["text"] for b in data.get("content", []) if b.get("type") == "text"]
    return "\n".join(text_blocks)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("사용법: python3 analyze_chart.py <이미지경로>")
        sys.exit(1)
    result = analyze_chart(sys.argv[1])
    print(result)
