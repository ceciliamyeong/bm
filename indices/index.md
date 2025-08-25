---
title: BM Indices
layout: default
permalink: /indices/
---

{% include bm_index_nav.html active="overview" %}

# BM Indices

BM20 지수 관련 주요 페이지들을 모아둔 허브입니다. 아래 링크를 클릭하면 각 세부 페이지로 이동합니다.

- [Components]({{ '/indices/components/' | relative_url }})  
  → 지수 구성 종목과 주요 데이터 테이블

- [Methodology]({{ '/indices/methodology/' | relative_url }})  
  → BM20 지수 산정 방식 및 가중치 규칙

- [Performance]({{ '/indices/performance/' | relative_url }})  
  → 지수 성과 및 차트 시각화

- [Weights]({{ '/indices/weights/' | relative_url }})  
  → 코인별 가중치 비율

- [Portfolio 2025Q3]({{ '/indices/portfolio-2025q3/' | relative_url }})  
  → 2025년 3분기 기준 포트폴리오 상세

# BM20 Index

<div id="bm20" style="height:380px; max-width:1000px; margin:24px 0;"></div>
<div id="bm20-meta" style="font:12px/1.6 system-ui,-apple-system,Segoe UI,Roboto,sans-serif;color:#666;"></div>

<script src="https://cdn.jsdelivr.net/npm/echarts@5"></script>
<script>
// CSV 엔드포인트
const CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTndyrPd3WWwFtfzv2CZxJeDcH-l8ibQIdO5ouYS4HsaGpbeXQQbs6WEr9qPqqZbRoT6cObdFxJpief/pub?gid=720141148&single=true&output=csv";

// CSV 가져오기 (캐시 무효화)
async function fetchCsv() {
  const url = CSV_URL + (CSV_URL.includes("?") ? "&" : "?") + "v=" + Date.now();
  const res = await fetch(url, { cache: "no-store" });
  if (!res.ok) throw new Error("CSV fetch failed: " + res.status);
  return res.text();
}

// 아주 심플한 CSV 파서 (date,index,updated_at 가정)
function parseCsv(text) {
  const lines = text.trim().split(/\r?\n/);
  const header = lines.shift().split(",");
  const iD = header.indexOf("date");
  const iV = header.indexOf("index");
  const iU = header.indexOf("updated_at");

  return lines.map(l => {
    const c = l.split(",");
    const v = parseFloat((c[iV] || "").replace(/"/g, ""));
    return { date: c[iD], value: Number.isFinite(v) ? v : null, updated: iU >= 0 ? c[iU] : null };
  }).filter(r => r.value !== null);
}

function renderChart(rows) {
  const el = document.getElementById("bm20");
  const chart = echarts.init(el);

  const data = rows.map(r => [r.date, r.value]);

  chart.setOption({
    tooltip: { trigger: "axis", valueFormatter: v => (typeof v === "number" ? v.toFixed(2) : v) },
    grid: { left: 48, right: 24, top: 24, bottom: 56 },
    xAxis: { type: "time" },
    yAxis: { type: "value", scale: true, name: "Index" },
    dataZoom: [{ type: "inside" }, { type: "slider", bottom: 18 }],
    series: [{ name: "BM20", type: "line", showSymbol: false, smooth: true, data }]
  });

  // 메타: 최신값 + 간단 수익률
  const last = rows.at(-1);
  function pRet(days) {
    if (rows.length <= days) return null;
    const prev = rows[rows.length - 1 - days].value;
    return prev ? (last.value / prev - 1) : null;
  }
  const meta = document.getElementById("bm20-meta");
  const r1m = pRet(21), r3m = pRet(63), r1y = pRet(252);
  meta.innerHTML = `
    <b>Last</b>: ${last?.date ?? "-"} &nbsp; | &nbsp;
    <b>Index</b>: ${last?.value?.toFixed(2) ?? "-"} &nbsp; | &nbsp;
    <b>Updated</b>: ${last?.updated ?? "-"}
    ${r1m!==null ? ` &nbsp; | &nbsp; <b>1M</b>: ${(r1m*100).toFixed(2)}%` : ""}
    ${r3m!==null ? ` &nbsp; | &nbsp; <b>3M</b>: ${(r3m*100).toFixed(2)}%` : ""}
    ${r1y!==null ? ` &nbsp; | &nbsp; <b>1Y</b>: ${(r1y*100).toFixed(2)}%` : ""}
  `;

  addEventListener("resize", () => chart.resize());
}

// 실행
fetchCsv().then(parseCsv).then(renderChart).catch(err => {
  console.error(err);
  document.getElementById("bm20-meta").textContent = "데이터 로드 실패: " + err.message;
});
</script>
