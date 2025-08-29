---
title: "BM20 (GS) Daily"
layout: default
permalink: /indices/gs/
active: indices
---

# BM20 (GS) Daily Reports

<p style="color:#666;font-size:14px">
  GS 버전 산출물은 <code>archive_gs/</code> 폴더에 저장됩니다.<br>
  최신 날짜의 HTML/PDF/차트를 아래에서 확인할 수 있습니다.
</p>

<div id="gs-latest"></div>

<script>
async function loadGsLatest() {
  try {
    // 오늘 날짜 기준으로 확인 (단, 실제로는 최신 날짜 리스트를 읽는 게 안전)
    const today = new Date();
    const ymd = today.toISOString().slice(0,10); // YYYY-MM-DD

    // 산출물이 있는지 HEAD 요청으로 체크
    const testUrl = `/archive_gs/${ymd}/bm20_daily_${ymd}.html`;
    const resp = await fetch(testUrl, {method:'HEAD'});
    let latest = ymd;

    // 오늘 없으면 어제 확인
    if (!resp.ok) {
      const d = new Date(today.getTime() - 86400000);
      latest = d.toISOString().slice(0,10);
    }

    const root = `/archive_gs/${latest}`;
    document.getElementById('gs-latest').innerHTML = `
      <h3>Latest: ${latest}</h3>
      <ul>
        <li><a href="${root}/bm20_daily_${latest}.html">HTML Report</a></li>
        <li><a href="${root}/bm20_daily_${latest}.pdf">PDF Report</a></li>
      </ul>
      <p><img src="${root}/bm20_bar_${latest}.png" style="max-width:100%;margin:8px 0"></p>
      <p><img src="${root}/bm20_trend_${latest}.png" style="max-width:100%;margin:8px 0"></p>
    `;
  } catch(e) {
    document.getElementById('gs-latest').innerText = "No GS report found.";
  }
}
loadGsLatest();
</script>
