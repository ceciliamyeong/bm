---
title: "Performance"
layout: default
permalink: /indices/performance/
active: performance
---

# BM20 Performance

## Index Trend (Raw vs BM20)
<div id="bm20-compare-line" style="height:420px; max-width:1000px; margin:16px auto;"></div>
<div id="bm20-compare-meta" style="font:12px/1.6 system-ui,-apple-system,Segoe UI,Roboto,sans-serif;color:#666;"></div>

---

## Weight Comparison
<p style="color:#666">
  왼쪽은 시총 기반 원본 가중치, 오른쪽은 BM20 공식 메서돌로지 룰을 적용한 가중치입니다.<br>
  화면이 좁아지면 차트가 세로로 정렬됩니다.
</p>

<div style="display:flex; flex-wrap:wrap; gap:24px; justify-content:center;">
  <div style="flex:1; min-width:320px; max-width:600px;">
    <h3 style="margin-bottom:8px;">Market Cap Weights</h3>
    <div id="bm20-raw-pie" style="height:420px;"></div>
    <div id="bm20-raw-meta" style="font:12px/1.6 system-ui,-apple-system,Segoe UI,Roboto,sans-serif;color:#666;"></div>
  </div>
  <div style="flex:1; min-width:320px; max-width:600px;">
    <h3 style="margin-bottom:8px;">BM20 Methodology</h3>
    <div id="bm20-fixed-pie" style="height:420px;"></div>
    <div id="bm20-fixed-meta" style="font:12px/1.6 system-ui,-apple-system,Segoe UI,Roboto,sans-serif;color:#666;"></div>
  </div>
</div>

<div id="bm20-trend" style="height:380px;max-width:1100px;margin:12px 0;"></div>
<script src="https://cdn.jsdelivr.net/npm/echarts@5"></script>
<script>
(function(){
  // GitHub Pages baseurl 대응: _config.yml의 baseurl: /bm
  const BASE = '{{ site.baseurl | default: "" }}';
  const SERIES_URL = BASE + '/series.json';          // 예: /bm/series.json
  const RAW_URL    = BASE + '/series_raw.json';      // 있으면 2라인, 없으면 1라인만

  const el = document.getElementById('bm20-trend');
  const chart = echarts.init(el);

  function fmt(x){ return (x||0).toFixed(2); }

  fetch(SERIES_URL).then(r=>r.json()).then(bmArr=>{
    // 예상 포맷: [{date:"YYYY-MM-DD", level: 6905.28}, ...]
    const dates = bmArr.map(o=>o.date);
    const bm20  = bmArr.map(o=>+((o.level ?? o.index)));

    // RAW가 있으면 같이 그려주고, 없으면 BM20만
    fetch(RAW_URL).then(r=> r.ok ? r.json() : null).then(rawArr=>{
      const hasRaw = Array.isArray(rawArr) && rawArr.length;
      const raw = hasRaw ? rawArr.map(o=>+((o.level ?? o.index))) : null;

      const series = [{
        name: 'BM20',
        type: 'line',
        showSymbol: false,
        data: bm20
      }];
      if (hasRaw) {
        series.unshift({
          name: 'Raw',
          type: 'line',
          showSymbol: false,
          lineStyle: {type:'dashed'},
          data: raw
        });
      }

      chart.setOption({
        backgroundColor: 'transparent',
        tooltip: { trigger: 'axis', valueFormatter: v=>fmt(v) },
        legend: { top: 0 },
        grid: { left: 40, right: 20, top: 30, bottom: 40 },
        xAxis: { type: 'category', data: dates },
        yAxis: { type: 'value', scale: true },
        series
      });
      window.addEventListener('resize', ()=>chart.resize());
    });
  }).catch(err=>{
    console.error('BM20 trend load error:', err);
  });
})();
</script>

<script>
// ✅ Index history CSV (Raw vs BM20 Index)
const CSV_INDEX_COMPARE = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTndyrPd3WWwFtfzv2CZxJeDcH-l8ibQIdO5ouYS4HsaGpbeXQQbs6WEr9qPqqZbRoT6cObdFxJpief/pub?gid=1685318213&single=true&output=csv";

// ✅ Weights CSV (시총 기반)
const CSV_WEIGHTS = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTndyrPd3WWwFtfzv2CZxJeDcH-l8ibQIdO5ouYS4HsaGpbeXQQbs6WEr9qPqqZbRoT6cObdFxJpief/pub?gid=1533548287&single=true&output=csv";

async function fetchCsv(url){
  const u=url+(url.includes("?")?"&":"?")+"v="+Date.now();
  const res=await fetch(u,{cache:"no-store"});
  if(!res.ok) throw new Error("CSV fetch failed: "+res.status);
  return res.text();
}

function splitCsv(row){
  const out=[];let cur="";let q=false;
  for(let i=0;i<row.length;i++){
    const ch=row[i];
    if(ch=='"'){q=!q;continue;}
    if(ch==","&&!q){out.push(cur);cur="";continue;}
    cur+=ch;
  }
  out.push(cur);
  return out.map(s=>s.trim());
}

// ---- Index Comparison ----
function parseCompareCsv(text){
  const lines=text.trim().split(/\r?\n/);
  const header=splitCsv(lines.shift()).map(h=>h.toLowerCase());
  const iDate=header.indexOf("date");
  const iRaw=header.indexOf("raw_index");
  const iBM20=header.indexOf("bm20_index");
  const rawData=[], bm20Data=[];
  for(const l of lines){
    if(!l.trim()) continue;
    const c=splitCsv(l);
    const d=c[iDate];
    const raw=parseFloat(c[iRaw]);
    const bm20=parseFloat(c[iBM20]);
    if(Number.isFinite(raw)) rawData.push([d,raw]);
    if(Number.isFinite(bm20)) bm20Data.push([d,bm20]);
  }
  return {rawData,bm20Data};
}

function renderCompare({rawData,bm20Data}){
  const el=document.getElementById("bm20-compare-line");
  const chart=echarts.init(el);
  chart.setOption({
    tooltip:{trigger:"axis"},
    legend:{data:["Raw (MktCap)","BM20 Methodology"],top:0},
    grid:{left:48,right:24,top:48,bottom:56},
    xAxis:{type:"time"},
    yAxis:{type:"value",scale:true,name:"Index"},
    dataZoom:[{type:"inside"},{type:"slider",bottom:18}],
    series:[
      {name:"Raw (MktCap)",type:"line",showSymbol:false,data:rawData},
      {name:"BM20 Methodology",type:"line",showSymbol:false,data:bm20Data}
    ]
  });
  const meta=document.getElementById("bm20-compare-meta");
  const latestDate=rawData.at(-1)?.[0];
  const latestRaw=rawData.at(-1)?.[1];
  const latestBM20=bm20Data.at(-1)?.[1];
  meta.textContent=`Latest ${latestDate||"-"} · Raw ${latestRaw?.toFixed(2)??"-"} vs BM20 ${latestBM20?.toFixed(2)??"-"}`;
  addEventListener("resize",()=>chart.resize());
}

// ---- Weights ----
function parseWeightCsv(text){
  const lines=text.trim().split(/\r?\n/);
  const header=splitCsv(lines.shift()).map(h=>h.toLowerCase());
  const iSym=header.indexOf("symbol");
  const iCap=header.indexOf("market_cap");
  const iName=header.indexOf("name");
  const rows=[];
  for(const l of lines){
    if(!l.trim()) continue;
    const c=splitCsv(l);
    const sym=(c[iSym]||"").toUpperCase();
    const name=c[iName]||sym;
    let cap=parseFloat((c[iCap]||"0").replace(/,/g,""));
    if(Number.isFinite(cap)){
      rows.push({symbol:sym,name,market_cap:cap});
    }
  }
  return rows;
}
function normalizeMcap(rows){
  const sum=rows.reduce((a,b)=>a+b.market_cap,0)||1;
  return rows.map(r=>({symbol:r.symbol,name:r.name,weight:r.market_cap/sum}));
}
function applyBM20(rows){
  const fixed={BTC:0.30,ETH:0.20,XRP:0.05,USDT:0.05,BNB:0.05};
  const others=rows.filter(r=>!(r.symbol in fixed));
  const each=others.length?0.35/others.length:0;
  return rows.map(r=>({name:r.name,symbol:r.symbol,weight:fixed[r.symbol]??each}));
}
function renderPie(elId,metaId,rows,label){
  const el=document.getElementById(elId);
  const chart=echarts.init(el);
  const data=rows.map(r=>({name:r.symbol||r.name,value:+(r.weight*100).toFixed(2)}));
  chart.setOption({
    tooltip:{trigger:"item",formatter:p=>`${p.name}: ${p.value.toFixed(2)}%`},
    legend:{type:"scroll",orient:"vertical",right:0,top:"middle"},
    series:[{type:"pie",radius:["40%","70%"],center:["38%","50%"],label:{formatter:"{b}\n{d}%"},data}]
  });
  document.getElementById(metaId).textContent=label+" · 총 "+data.length+"개 종목";
  addEventListener("resize",()=>chart.resize());
}

// ---- Run ----
fetchCsv(CSV_INDEX_COMPARE).then(parseCompareCsv).then(renderCompare);
fetchCsv(CSV_WEIGHTS).then(parseWeightCsv).then(normalizeMcap).then(rows=>{
  renderPie("bm20-raw-pie","bm20-raw-meta",rows,"Market Cap weights (normalized)");
  const fixed=applyBM20(rows);
  renderPie("bm20-fixed-pie","bm20-fixed-meta",fixed,"BM20 methodology (BTC30, ETH20, XRP/BNB/USDT 5 each, rest equally share 35%)");
});
</script>
