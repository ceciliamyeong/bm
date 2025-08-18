---
title: BM20 성과
layout: default
---
{% include bm_index_nav.html active="performance" %}

# 성과 요약

<div id="perf" style="display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px"></div>

<script>
const SUM_CSV="REPLACE_WITH_bm20_summary_CSV_URL"; // ← TODO

async function fetchCsv(u){const r=await fetch(u+(u.includes('?')?'&':'?')+'v='+Date.now(),{cache:'no-store'});if(!r.ok)throw new Error(r.status);return r.text();}
function parseCsv(t){
  const L=t.trim().split(/\r?\n/); const H=L.shift().split(",");
  const V=L.pop()?.split(",")||[]; const get=k=>V[H.indexOf(k)];
  return {
    as_of:get("as_of"), index:parseFloat(get("index")),
    ret_1m:parseFloat(get("ret_1m")), ret_3m:parseFloat(get("ret_3m")), ret_1y:parseFloat(get("ret_1y")),
    ytd:parseFloat(get("ytd")), ann_vol:parseFloat(get("ann_vol")), max_dd:parseFloat(get("max_dd"))
  };
}
function card(label, val, fmt){
  const v = (val==null||Number.isNaN(val))?'-':fmt(val);
  return `<div style="border:1px solid #eee;border-radius:8px;padding:12px">
    <div style="font-size:12px;color:#666">${label}</div>
    <div style="font-size:18px;font-weight:700">${v}</div>
  </div>`;
}
fetchCsv(SUM_CSV).then(parseCsv).then(d=>{
  const $=k=>document.getElementById(k);
  const box=document.getElementById('perf');
  box.innerHTML =
    card('As of', d.as_of, x=>x) +
    card('Index', d.index, x=>x.toFixed(2)) +
    card('1M', d.ret_1m, x=>(x*100).toFixed(2)+'%') +
    card('3M', d.ret_3m, x=>(x*100).toFixed(2)+'%') +
    card('1Y', d.ret_1y, x=>(x*100).toFixed(2)+'%') +
    card('YTD', d.ytd, x=>(x*100).toFixed(2)+'%') +
    card('Vol(ann.)', d.ann_vol, x=>(x*100).toFixed(2)+'%') +
    card('Max DD', d.max_dd, x=>(x*100).toFixed(2)+'%');
}).catch(e=>{
  document.getElementById('perf').innerHTML = `<div>로드 실패: ${e.message}</div>`;
});
</script>
