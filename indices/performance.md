---
title: "Performance"
layout: default
permalink: /indices/performance/
active: performance
---

# BM20 Performance

## Index Trend
<div id="bm20-line" style="height:420px; max-width:1000px; margin:16px auto;"></div>
<div id="bm20-line-meta" style="font:12px/1.6 system-ui,-apple-system,Segoe UI,Roboto,sans-serif;color:#666;"></div>

---

## Weight Comparison
<p style="color:#666">
  왼쪽은 구글 시트 원본 가중치, 오른쪽은 BM20 공식 메서돌로지를 적용한 가중치입니다.<br>
  화면이 좁아지면 차트가 세로로 정렬됩니다.
</p>

<div style="display:flex; flex-wrap:wrap; gap:24px; justify-content:center;">
  <div style="flex:1; min-width:320px; max-width:600px;">
    <h3 style="margin-bottom:8px;">Raw Weights (from Sheet)</h3>
    <div id="bm20-raw-pie" style="height:420px;"></div>
    <div id="bm20-raw-meta" style="font:12px/1.6 system-ui,-apple-system,Segoe UI,Roboto,sans-serif;color:#666;"></div>
  </div>
  <div style="flex:1; min-width:320px; max-width:600px;">
    <h3 style="margin-bottom:8px;">BM20 Methodology</h3>
    <div id="bm20-fixed-pie" style="height:420px;"></div>
    <div id="bm20-fixed-meta" style="font:12px/1.6 system-ui,-apple-system,Segoe UI,Roboto,sans-serif;color:#666;"></div>
  </div>
</div>

<script src="https://cdn.jsdelivr.net/npm/echarts@5"></script>
<script>
// ✅ CSV (Index history)
const CSV_INDEX = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTndyrPd3WWwFtfzv2CZxJeDcH-l8ibQIdO5ouYS4HsaGpbeXQQbs6WEr9qPqqZbRoT6cObdFxJpief/pub?gid=720141148&single=true&output=csv";
// ✅ CSV (Weights)
const CSV_WEIGHTS = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTndyrPd3WWwFtfzv2CZxJeDcH-l8ibQIdO5ouYS4HsaGpbeXQQbs6WEr9qPqqZbRoT6cObdFxJpief/pub?gid=1533548287&single=true&output=csv";

// Fetch helper
async function fetchCsv(url){
  const u = url + (url.includes("?") ? "&" : "?") + "v=" + Date.now();
  const res = await fetch(u,{cache:"no-store"});
  if(!res.ok) throw new Error("CSV fetch failed: "+res.status);
  return res.text();
}

// Simple CSV row splitter
function splitCsv(row){
  const out=[]; let cur=""; let q=false;
  for(let i=0;i<row.length;i++){
    const ch=row[i];
    if(ch=='"'){q=!q;continue;}
    if(ch=="," && !q){out.push(cur);cur="";continue;}
    cur+=ch;
  }
  out.push(cur);
  return out.map(s=>s.trim());
}

// Parse Index CSV
function parseIndexCsv(text){
  const lines=text.trim().split(/\r?\n/);
  const header=splitCsv(lines.shift()).map(h=>h.toLowerCase());
  const iDate=header.indexOf("date");
  const iVal=header.indexOf("index");
  const rows=[];
  for(const l of lines){
    if(!l.trim()) continue;
    const c=splitCsv(l);
    const d=c[iDate];
    const v=parseFloat(c[iVal]);
    if(Number.isFinite(v)) rows.push([d,v]);
  }
  return rows;
}

// Render line chart
function renderLine(data){
  const el=document.getElementById("bm20-line");
  const chart=echarts.init(el);
  chart.setOption({
    tooltip:{trigger:"axis"},
    grid:{left:48,right:24,top:24,bottom:56},
    xAxis:{type:"time"},
    yAxis:{type:"value",scale:true,name:"Index"},
    dataZoom:[{type:"inside"},{type:"slider",bottom:18}],
    series:[{name:"BM20 Index",type:"line",showSymbol:false,smooth:true,data}]
  });
  const last=data.at(-1);
  document.getElementById("bm20-line-meta").textContent =
    "Latest: "+(last? last[0] : "-")+" Index "+(last? last[1].toFixed(2) : "-");
  addEventListener("resize",()=>chart.resize());
}

// Parse Weights CSV
function parseWeightCsv(text){
  const lines=text.trim().split(/\r?\n/);
  const header=splitCsv(lines.shift()).map(h=>h.toLowerCase());
  const iName=header.indexOf("name");
  const iSym=header.indexOf("symbol");
  const iW=header.indexOf("weight");
  const rows=[];
  for(const l of lines){
    if(!l.trim()) continue;
    const c=splitCsv(l);
    const sym=(c[iSym]||"").toUpperCase();
    const name=c[iName]||sym;
    let w=parseFloat((c[iW]||"0").replace(/[%"]/g,"").trim());
    if(Number.isNaN(w)) continue;
    if(w>1.0001) w=w/100;
    rows.push({name,symbol:sym,weight:w});
  }
  return rows;
}

// Apply BM20 rule
function applyBM20(rows){
  const fixed={BTC:0.30,ETH:0.20,XRP:0.05,USDT:0.05,BNB:0.05};
  const others=rows.filter(r=>!(r.symbol in fixed));
  const each=others.length?0.35/others.length:0;
  return rows.map(r=>({name:r.name,symbol:r.symbol,weight:fixed[r.symbol]??each}));
}

// Render pie chart
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

// 실행
fetchCsv(CSV_INDEX).then(parseIndexCsv).then(renderLine).catch(err=>{
  console.error(err);
  document.getElementById("bm20-line-meta").textContent="Index 로드 실패: "+err.message;
});

fetchCsv(CSV_WEIGHTS)
  .then(parseWeightCsv)
  .then(rows=>{
    const sum=rows.reduce((a,b)=>a+b.weight,0)||1;
    rows.forEach(r=>r.weight=r.weight/sum);
    renderPie("bm20-raw-pie","bm20-raw-meta",rows,"Raw weights from sheet (normalized)");
    const fixed=applyBM20(rows);
    renderPie("bm20-fixed-pie","bm20-fixed-meta",fixed,"BM20 methodology (BTC30, ETH20, XRP/BNB/USDT 5 each, rest equally share 35%)");
  })
  .catch(err=>{
    console.error(err);
    document.getElementById("bm20-raw-meta").textContent="Weights 로드 실패: "+err.message;
  });
</script>
