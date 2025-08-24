---
title: "Methodology"
layout: default
permalink: /indices/methodology/
active: methodology
---



# BM20 산정기준 (Methodology)

## 지수 기본
- **기준 통화:** USD
- **기준일/베이스:** 2018-01-01 = 100
- **리밸런싱 주기:** 분기(Quarterly) — 분기 내 가중치 고정
- **편입 대상:** 시총/거래대금 상위, 스테이블 제외, 편집 스냅샷으로 확정

## 가중치 책정
우선순위(분기별):
1. **시총(Market Cap)** – CoinGecko (API 가용 범위: 최근 1년)
2. **폴백:** **달러거래대금** = Σ(일별 `Close × Volume`) (야후 파이낸스)
3. **최종 폴백:** 편집 스냅샷(`bm20_constituents.weight`)

보정:
- **KR 보너스:** 국내 상장(`listed_in_kr3`) 및 `kr_bonus_applied=True` → × **1.3**
- **상한(Cap):** BTC 30%, ETH 15%, XRP 5%, 기타 15%  
  (초과분은 잔여 종목에 **비례 재분배**)
- **상장 전 분기:** 가중치 0

## 지수 산출
- **가격:** yfinance USD 종가
- **일간 수익률:** 단순 수익률 `r_t = P_t / P_{t-1} − 1`
- **지수:** `Index_t = Base × ∏ (1 + r_i·w_i)` (분기 내 고정 가중치)

## 운영/감리
- 분기마다 **소스 선택(시총/거래대금/스냅샷)** 로그 기록
- 산출/배포 이력은 “데이터”와 GitHub Actions 로그로 관리
