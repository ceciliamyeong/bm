---
title: BM20 FAQ
layout: default
---
{% include bm_index_nav.html active="faq" %}

# 자주 묻는 질문

**Q. 과거 구간 시총이 없는 경우는?**  
A. 최근 1년 외 구간은 분기 **달러거래대금**으로 폴백, 그래도 없으면 스냅샷 가중치를 사용합니다.

**Q. 업데이트 시각은?**  
A. 매일 KST N시 이후 자동 갱신(또는 수동 트리거). 변경 이력은 GitHub Actions 로그와 시트 `updated_at`으로 확인합니다.

**Q. CSV 형식은 고정인가요?**  
A. 주요 컬럼(`date,index,updated_at` / `quarter,coin_id,weight,...`)은 고정 유지합니다.
