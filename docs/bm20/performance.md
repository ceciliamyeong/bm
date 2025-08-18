---
title: BM20 성과
layout: default
---
{% include bm_index_nav.html active="performance" %}

# 성과 요약

<div id="perf" style="display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px"></div>

<script>
const SUM_CSV = "PASTE_YOUR_bm20_summary_CSV_URL"; // ← 시트 CSV 링크

async function csv(u){const r=await fetch(u+(u.includes('?')?'&':'?')+'v='+Date.now(),{cache:'no-store'});if(!r.ok)throw new Error(r.status);return r.text();}
function parse(t){
  const L=t.trim().split(/\r?\n/), H=L.shift().split(","), last=L.pop().split(",");
  const V=k=>last[H.indexOf(k)];
  return {
    as_of:V("as_of"), index:+V("index"),
    ret_1m:+V("ret_1m"), ret_3m:+V("ret_3m"), ret_1y:+V("ret_1y"), ytd:+V("ytd"),
    ann_vol:+V("ann_vol"), max_dd:+V("max_dd")
  };
}
function card(k,v,f){const val=(v==null||Number.isNaN(v))?'-':f(v);
  return `<div style="border:1px solid #eee;border-radius:8px;padding:12px">
    <div style="font-size:12px;color:#666">${k}</div>
    <div style="font-size:18px;font-weight:700">${val}</div>
  </div>`;}
csv(SUM_CSV).then(parse).then(d=>{
  const box=document.getElementById('perf');
  box.innerHTML =
    card('As of', d.as_of, x=>x) +
    card('Index', d.index, x=>x.toFixed(2)) +
    card('1M', d.ret_1m, x=>(x*100).toFixed(2)+'%') +
    card('3M', d.ret_3m, x=>(x*100).toFixed(2)+'%') +
    card('1Y', d.ret_1y, x=>(x*100).toFixed(2)+'%') +
    card('YTD', d.ytd, x=>(x*100).toFixed(2)+'%') +
    card('Vol (ann.)', d.ann_vol, x=>(x*100).toFixed(2)+'%') +
    card('Max DD', d.max_dd, x=>(x*100).toFixed(2)+'%');
}).catch(e=>{
  document.getElementById('perf').innerHTML = `<div>로드 실패: ${e.message}</div>`;
});
</script>

