---
title: BM20 산정기준
layout: default
---
{% include bm_index_nav.html active="method" %}

# 산정기준 (Methodology)

- **기준통화:** USD
- **베이스:** 100 (2018-01-01)
- **리밸런싱:** 분기(Quarterly). 분기 내 가중치는 고정.
- **가중치 우선순위**
  1. 최근 1년 시총(CoinGecko, API 키 가용 범위)
  2. 폴백: 분기 누적 **달러거래대금**(yfinance: Close × Volume)
  3. 최종 폴백: 편집 스냅샷 `bm20_constituents.weight`
- **KR 보너스:** `kr_bonus_applied=True` → × **1.3**
- **상한(Cap):** BTC 30%, ETH 15%, XRP 5%, 기타 15% (초과분은 잔여 종목에 비례 재분배)
- **상장 전:** 최초 유효가격 이전 분기에는 가중치 0
- **가격/수익률:** yfinance 일별 종가, 단순 일수익률 누적 곱으로 지수 산출

> 운영 로그: 분기별 적용 소스(`mcap` / `dollar_volume` / `snapshot_weight`)는 “구성·가중치” 페이지에서 분기별 표로 확인할 수 있어요.
