# 주간 보고서 (국내 5대 거래소 거래대금 + BM20 + 시황 글)

폴더 이름이 `_`로 시작해서 GitHub Pages(사이트)에는 공개되지 않습니다. 결과 파일은 이 저장소 화면과 Actions 실행 결과(Artifacts)에서 볼 수 있습니다.

## 파일
| 파일 | 하는 일 |
|---|---|
| `weekly_volume_collector.py` | 5대 거래소 1시간봉(고팍스 30분봉) 합산, 거래소별·종목별·일별 거래대금 |
| `weekly_returns.py` | 종목별 주간 수익률 (월 첫 시가 → 일 마지막 종가) |
| `make_article.py` | 집계 데이터만으로 시황 글 초안 작성 (규칙 기반) |
| `make_weekly_report.py` | 카드형 HTML 보고서 (인쇄 시 A4 4페이지) |
| `run_weekly.py` | 위 네 단계를 한 번에 실행 |

## 실행 (로컬, cmd)
```
cd _weekly_report
python run_weekly.py                       지난주 자동 계산
python run_weekly.py --week 2026-10-12     특정 주
python run_weekly.py --bm20-dir ..         BM20 시계열을 저장소 사본에서 읽기 (기본은 GitHub raw)
```
결과: `out/weekly_<월요일>_*.csv|json|html`, `out/article_weekly_<월요일>.*`, `data/`(원자료), `logs/`

## 자동 실행
`.github/workflows/weekly_report.yml` — 매주 월요일 09:10(KST). Actions 탭에서 "Run workflow"로 특정 주를 수동 실행할 수 있습니다.
- 커밋되는 것: `out/` 전체, `data/returns_*.csv`, `data/korean_names.json`
- 커밋되지 않는 것: 시간봉 원자료(`data/hourly_*.csv`, 용량이 큼) → Actions 캐시에 보관

## 기준
KST 월 00:00 ~ 다음 월 00:00, 원화마켓. 디지털X·고팍스 거래대금은 종가×거래량 근사치. 거래소 공시(09시 기준)와 1~3% 차이가 날 수 있음.
