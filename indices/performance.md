---
title: BM20 성과
layout: default
permalink: /bm/indices/performance/
---
{% include bm_index_nav.html active="performance" %}


# 성과 요약

<div id="perf" style="display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px"></div>

<script>
// 시트: bm20_summary (CSV)
const SUM_CSV = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTndyrPd3WWwFtfzv2CZxJeDcH-l8ibQIdO5ouYS4HsaGpbeXQQbs6WEr9qPqqZbRoT6cObdFxJpief/pub?gid=1065627907&single=true&output=csv";

async function csv(u){const r=await fetch(u+(u.includes('?')?'&':'?')+'v='+Date.now(),{cache:'no-store'});if(!r.ok)throw new Error(r.status);return r.text();}
function parse(t){
  const L=t.trim().split(/\r?\n/); const H=L.shift().split(",");
  // 마지막 행(최신)만 사용
  const last=(L[L.length-1]||"").split(",");
  const V=k=>last[H.indexOf(k)];
  const toN=v=>{const x=parseFloat(v); return Number.isFinite(x)?x:null;};
  return {
    as_of: V("as_of"),
    index: toN(V("index")),
    ret_1m: toN(V("ret_1m")),
    ret_3m: toN(V("ret_3m")),
    ret_1y: toN(V("ret_1y")),
    ytd:    toN(V("ytd")),
    ann_vol:toN(V("ann_vol")),
    max_dd: toN(V("max_dd")),
  };
}
function card(label, val, fmt){
  const n = (val==null||Number.isNaN(val)) ? null : val;
  const txt = n==null ? '-' : fmt(n);
  const color = (n==null) ? '#222' : (n>0 ? '#0b8457' : (n<0 ? '#b00020' : '#222'));
  return `<div style="border:1px solid #eee;border-radius:8px;padding:12px">
    <div style="font-size:12px;color:#666">${label}</div>
    <div style="font-size:18px;font-weight:700;color:${color}">${txt}</div>
  </div>`;
}
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

