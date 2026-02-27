BM20 Index

Blockmedia Digital Asset Benchmark

BM20은 블록미디어(Blockmedia)가 산출·운영하는 디지털자산 대표지수다.
글로벌 시가총액 상위 자산을 기반으로 하되, 한국 시장의 거래 현실과 유동성을 반영해 설계됐다.

BM20은 단순한 시총 가중 지수가 아니다.
이 지수는 국내 투자자들이 실제로 접근하고 거래하는 시장 구조를 반영한 “미디어 기반 벤치마크”다.

공식 데이터 대시보드:
https://data.blockmedia.co.kr/


1. Why BM20?

글로벌 암호자산 시장에는 다양한 지수가 존재하지만,
대부분은 글로벌 시가총액을 기계적으로 반영하거나 해외 거래소 중심 구조를 따른다.

BM20은 다음과 같은 문제의식에서 출발했다:
	•	한국 투자자들이 실제로 거래 가능한 자산 구조는 무엇인가?
	•	글로벌 시총 상위 코인이 국내 시장에서도 동일한 영향력을 가지는가?
	•	미디어는 어떤 기준으로 시장을 설명해야 하는가?

BM20은
**“한국 시장을 반영한 디지털자산 기준점”**을 목표로 설계됐다.

⸻

2. Index Methodology

■ Universe
	•	글로벌 시가총액 상위 20종목 기준
	•	APT 제외
	•	SUI 포함

■ Weight Structure (Fixed Weight Model)
	•	BTC: 30%
	•	ETH: 20%
	•	XRP: 5%
	•	USDT: 5%
	•	BNB: 5%
	•	나머지 15개 자산: 총 35% 균등 배분 (각 약 2.33%)

BM20은 시총 자동 가중 방식이 아니라,
시장 대표성과 구조적 안정성을 고려한 고정 가중 모델을 채택한다.

■ Rebalancing
	•	분기별 리밸런싱
	•	적용 시점: 1월 / 4월 / 7월 / 10월 시작일

■ Base Date
	•	기준일: 2018-01-01
	•	기준값: 100

■ Index Formula

Index Level = (Current Portfolio Value / Base Portfolio Value) × 100

⸻

3. Data & Calculation
	•	기본 가격 데이터: CoinGecko
	•	예외 상황 시: Yahoo Finance 매핑 폴백
	•	히스토리 데이터 저장: CSV 기반
	•	산출 주기: Daily

BM20은 단순 지수 레벨뿐 아니라 다음 데이터를 함께 제공한다:
	•	1D / 7D / 30D / MTD / YTD 수익률
	•	Best 3 Performers
	•	Worst 3 Performers
	•	자산별 기여도 분석
	•	BTC / ETH 트렌드 차트

⸻

4. Public Dashboard

BM20의 공식 지수 및 차트는 아래에서 확인할 수 있다:

https://data.blockmedia.co.kr/

해당 페이지는 다음 정보를 제공한다:
	•	BM20 지수 레벨
	•	기간별 수익률
	•	Raw Market Cap 대비 비교
	•	포트폴리오 구성 비중
	•	히스토리 트렌드

⸻

5. Automation Architecture

BM20은 완전 자동화된 파이프라인을 통해 운영된다.

Daily Flow:

Data Collection
→ Weight Application
→ Index Calculation
→ History Update
→ Chart Generation
→ Editorial Summary Generation
→ Web Publishing

GitHub Actions 기반으로 매일 자동 산출 및 배포가 진행된다.

⸻

6. Positioning

BM20은 다음을 목표로 한다:
	•	Blockmedia 기사 및 시황의 기준 지수
	•	국내 디지털자산 시장 벤치마크
	•	향후 ETF / 펀드 / 파생상품 확장 가능 구조
	•	데이터 기반 미디어 모델 구축

BM20은 “보도용 지수”를 넘어
장기적으로는 디지털자산 시장의 구조적 기준점이 되는 것을 목표로 한다.

⸻

7. Disclaimer

BM20은 정보 제공을 목적으로 하며 투자 권유가 아니다.
지수 구성 및 산출 방식은 시장 환경에 따라 변경될 수 있다.
데이터 오류 가능성이 존재할 수 있다.
