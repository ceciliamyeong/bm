---
title: "Weights"
layout: default
permalink: /indices/weights/
active: weights
---

{% include bm_index_nav.html active="components" %}


# 분기별 구성·가중치

<p>분기별 확정 가중치와 사용된 소스(시총/달러거래대금/스냅샷), KR 보너스/상한 적용 여부를 보여줍니다.</p>

<table id="wq" style="width:100%;border-collapse:collapse">
  <thead>
    <tr>
      <th style="text-align:left">분기</th>
      <th style="text-align:left">코인</th>
      <th style="text-align:right">가중치</th>
      <th style="text-align:left">소스</th>
      <th style="text-align:center">KR×1.3</th>
      <th style="text-align:center">상한</th>
    </tr>
  </thead>
  <tbody></tbody>
</table>

<script>
const WQ_CSV = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTndyrPd3WWwFtfzv2CZxJeDcH-l8ibQIdO5ouYS4HsaGpbeXQQbs6WEr9qPqqZbRoT6cObdFxJpief/pub?gid=1645238012&single=true&output=csv";

async function getCsv(u){const r=await fetch(u+(u.includes('?')?'&':'?')+'v='+Date.now(),{cache:'no-store'});if(!r.ok)throw new Error(r.status);return r.text();}
function parseCsv(t){
  const L=t.trim().split(/\r?\n/), H=L.shift().split(",");
  const I=k=>H.indexOf(k);
  return L.map(l=>{
    const c=l.split(",");
    return {
      q:c[I("quarter")],
      coin:c[I("coin_id")] || c[I("symbol")] || "",
      w:parseFloat(c[I("weight")]),
      src:(c[I("source_used")]||"").trim(),
      kb:(c[I("kr_bonus")]||c[I("kr_bonus_applied")]||"").toLowerCase()==="true",
      cap:(c[I("cap_applied")]||"").toLowerCase()==="true"
    };
  }).filter(r=>Number.isFinite(r.w));
}
function renderRows(rows){
  // 분기+코인 정렬
  rows.sort((a,b)=>a.q.localeCompare(b.q)||a.coin.localeCompare(b.coin));
  const tb=document.querySelector("#wq tbody");
  tb.innerHTML = rows.map(r=>`
    <tr>
      <td>${r.q}</td>
      <td>${r.coin}</td>
      <td style="text-align:right">${(r.w*100).toFixed(2)}%</td>
      <td>${r.src||"-"}</td>
      <td style="text-align:center">${r.kb?"예":"-"}</td>
      <td style="text-align:center">${r.cap?"적용":"-"}</td>
    </tr>`).join("");
}
getCsv(WQ_CSV).then(parseCsv).then(renderRows)
  .catch(e=>document.querySelector("#wq tbody").innerHTML=`<tr><td colspan="6">로드 실패: ${e.message}</td></tr>`);
</script>
