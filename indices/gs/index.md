---
title: "BM20 (GS) Daily"
layout: default
permalink: /indices/gs/
active: indices
---

# BM20 (GS) Daily Reports

<p style="color:#666;font-size:14px">
  GS 버전 산출물은 <code>archive_gs/</code> 폴더에 저장됩니다.<br>
  가장 최근 날짜 폴더를 자동으로 찾아 보여줍니다.
</p>

<div id="gs-latest"></div>

<script>
async function getLatestGsFolder() {
  try {
    // archive_gs 폴더의 index를 불러서 가장 최근 날짜 추출
    const resp = await fetch('/archive_gs/', {cache:'no-store'});
    if (!resp.ok) throw new Error('Cannot list archive_gs');
    const html = await resp.text();

    // YYYY-MM-DD/ 패턴 폴더를 모두 수집
    const re = /href="(\d{4}-\d{2}-\d{2})\//g;
    const dates = [];
    let m;
    while ((m = re.exec(html)) !== null) { dates.push(m[1]); }
    dates.sort();
    if (dates.length === 0) return null;
    return dates[dates.length - 1]; // 가장 최신
  } catch(e) {
    console.error(e);
    return null;
  }
}

(async () => {
  const latest = await getLatestGsFolder();
  const root = document.getElementById('gs-latest');
  if (!latest) {
    root.textContent = "No GS report found.";
    return;
  }
  const base = `/archive_gs/${latest}`;
  root.innerHTML = `
    <h3>Latest: ${latest}</h3>
    <ul>
      <li><a href="${base}/bm20_daily_${latest}.html">HTML Report</a></li>
      <li><a href="${base}/bm20_daily_${latest}.pdf">PDF Report</a></li>
    </ul>
    <p><img src="${base}/bm20_bar_${latest}.png" style="max-width:100%;margin:8px 0"></p>
    <p><img src="${base}/bm20_trend_${latest}.png" style="max-width:100%;margin:8px 0"></p>
  `;
})();
</script>
