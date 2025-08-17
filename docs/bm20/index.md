---
title: BM20 개요
layout: default
---
{% include bm_index_nav.html active="overview" %}

<div id="bm20" style="height:380px; max-width:1000px; margin:24px 0;"></div>
<div id="bm20-meta" style="font:12px/1.6 system-ui,sans-serif;color:#666;"></div>

<script src="https://cdn.jsdelivr.net/npm/echarts@5"></script>
<script>
const CSV_URL="https://docs.google.com/spreadsheets/d/e/2PACX-1vTndyrPd3WWwFtfzv2CZxJeDcH-l8ibQIdO5ouYS4HsaGpbeXQQbs6WEr9qPqqZbRoT6cObdFxJpief/pub?gid=720141148&single=true&output=csv";
async function fetchCsv(){const u=CSV_URL+(CSV_URL.includes('?')?'&':'?')+'v='+Date.now();const r=await fetch(u,{cache:'no-store'});if(!r.ok)throw new Error(r.status);return r.text();}
function parseCsv(t){const L=t.trim().split(/\r?\n/);const h=L.shift().split(",");const iD=h.indexOf("date"),iV=h.indexOf("index"),iU=h.indexOf("updated_at");
  return L.map(l=>{const c=l.split(",");const v=parseFloat((c[iV]||"").replace(/"/g,""));return{date:c[iD],value:Number.isFinite(v)?v:null,updated:iU>=0?c[iU]:null};}).filter(r=>r.value!==null);}
function render(rows){const ch=echarts.init(document.getElementById('bm20'));ch.setOption({tooltip:{trigger:'axis',valueFormatter:v=>typeof v==='number'?v.toFixed(2):v},
  grid:{left:48,right:24,top:24,bottom:56},xAxis:{type:'time'},yAxis:{type:'value',scale:true},
  dataZoom:[{type:'inside'},{type:'slider',bottom:18}],
  series:[{type:'line',showSymbol:false,smooth:true,data:rows.map(r=>[r.date,r.value])}]});
  const last=rows.at(-1);document.getElementById('bm20-meta').textContent=`Last: ${last?.date} | Index: ${last?.value?.toFixed(2)} | Updated: ${last?.updated??'-'}`;
  addEventListener('resize',()=>ch.resize());}
fetchCsv().then(parseCsv).then(render).catch(e=>{document.getElementById('bm20-meta').textContent='로드 실패: '+e.message;});
</script>
