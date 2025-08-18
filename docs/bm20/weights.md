---
title: BM20 구성·가중치
layout: default
---
{% include bm_index_nav.html active="weights" %}

# 분기별 구성·가중치

<p>분기별 최종 가중치와 출처(mcap/달러거래대금/스냅샷), KR 보너스/상한 적용 여부를 표로 제공합니다.</p>

<table id="wq" style="width:100%;border-collapse:collapse">
  <thead>
    <tr>
      <th>Quarter</th><th>Coin</th><th>Weight</th><th>Source</th><th>KR×1.3</th><th>Cap</th>
    </tr>
  </thead>
  <tbody></tbody>
</table>

<script>
const WQ_CSV = "REPLACE_WITH_bm20_weights_q_CSV_URL"; // ← TODO

async function fetchCsv(u){const r=await fetch(u+(u.includes('?')?'&':'?')+'v='+Date.now(),{cache:'no-store'});if(!r.ok)throw new Error(r.status);return r.text();}
function parseCsv(t){
  const L=t.trim().split(/\r?\n/); const H=L.shift().split(",");
  const idx = s=>H.indexOf(s);
  return L.map(l=>{
    const c=l.split(",");
    return {
      q: c[idx("quarter")], coin:c[idx("coin_id")],
      w: parseFloat(c[idx("weight")]),
      src:(c[idx("source_used")]||"").trim(),
      kb:(c[idx("kr_bonus")]||"").trim(),
      cap:(c[idx("cap_applied")]||"").trim()
    };
  }).filter(r=>Number.isFinite(r.w));
}
function renderRows(rows){
  const tb=document.querySelector('#wq tbody');
  tb.innerHTML = rows.map(r=>(
    `<tr>
      <td>${r.q}</td><td>${r.coin}</td>
      <td style="text-align:right">${(r.w*100).toFixed(2)}%</td>
      <td>${r.src}</td><td>${r.kb==='true'?'예':'-'}</td><td>${r.cap==='true'?'적용':'-'}</td>
    </tr>`
  )).join("");
}
if(!WQ_CSV.startsWith("http")) {
  document.querySelector('#wq tbody').innerHTML = `<tr><td colspan="6">CSV URL을 설정해 주세요.</td></tr>`;
} else {
  fetchCsv(WQ_CSV).then(parseCsv).then(renderRows).catch(e=>{
    document.querySelector('#wq tbody').innerHTML = `<tr><td colspan="6">로드 실패: ${e.message}</td></tr>`;
  });
}
</script>
