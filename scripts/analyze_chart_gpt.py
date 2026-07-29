"""
차트 이미지 분석 프로토타입 (OpenAI GPT 버전)
사용법: python3 analyze_chart_gpt.py <이미지경로>
필요: 환경변수 OPENAI_API_KEY
"""
import sys
import os
import base64
import json
import urllib.request
import urllib.error

PROMPT = (
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

# 필요에 따라 교체:
#   gpt-5.6           - 최고 성능 (Sol)
#   gpt-5.6-terra     - 성능/비용 균형
#   gpt-5.6-luna      - 저비용, 대량 처리용
MODEL = "gpt-5.6"


def analyze_chart(image_path: str) -> str:
    with open(image_path, "rb") as f:
        image_b64 = base64.b64encode(f.read()).decode("utf-8")

    media_type = "image/png" if image_path.lower().endswith(".png") else "image/jpeg"
    data_url = f"data:{media_type};base64,{image_b64}"

    body = {
        "model": MODEL,
        "max_completion_tokens": 800,
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": PROMPT},
                    {"type": "image_url", "image_url": {"url": data_url}},
                ],
            }
        ],
    }

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("환경변수 OPENAI_API_KEY가 설정되어 있지 않습니다.")

    req = urllib.request.Request(
        "https://api.openai.com/v1/chat/completions",
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8")
        raise RuntimeError(f"API 오류 ({e.code}): {error_body}") from None

    return data["choices"][0]["message"]["content"]


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("사용법: python3 analyze_chart_gpt.py <이미지경로>")
        sys.exit(1)
    result = analyze_chart(sys.argv[1])
    print(result)
