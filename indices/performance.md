
---
title: "Performance"
layout: default
permalink: /indices/performance/
active: performance
---

# BM20 Performance

## Index Trend (BTC vs BM20)

<!-- 위: 기준일 대비 수익률(%) / 아래: 로그스케일(리베이스 100) -->
<div id="bm20-return" style="height:340px; max-width:1000px; margin:16px auto;"></div>
<div id="bm20-log"    style="height:360px; max-width:1000px; margin:16px auto;"></div>

<div id="bm20-compare-meta" style="font:12px/1.6 system-ui,-apple-system,Segoe UI,Roboto,sans-serif;color:#666;max-width:1000px;margin:0 auto 12px;"></div>

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

<script src="https://cdn.jsdelivr.net/npm/echarts@5"></script>
<script>
// ====== 데이터 소스 ======
const CSV_INDEX = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTndyrPd3WWwFtfzv2CZxJeDcH-l8ibQIdO5ouYS4HsaGpbeXQQbs6WEr9qPqqZbRoT6cObdFxJpief/pub?gid=1685318213&single=true&output=csv";
const CSV_WEIGHTS = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTndyrPd3WWwFtfzv2CZxJeDcH-l8ibQIdO5ouYS4HsaGpbeXQQbs6WEr9qPqqZbRoT6cObdFxJpief/pub?gid=1533548287&single=true&output=csv";

// 기준일(수익률 0% / 리베이스 100). "auto"로 두면 첫 유효일을 사용
const BASE_DATE = "2024-01-01";

// ====== 헬퍼 ======
async function fetchCsv(url){
  const res = await fetch(url + (url.includes("?") ? "&" : "?") + "v=" + Date.now(), {cache:"no-store"});
  if(!res.ok) throw new Error("CSV fetch failed: " + res.status);
  return res.text();
}
function splitCsv(row){
  const out=[]; let cur=""; let q=false;
  for(let i=0;i<row.length;i++){
    const ch=row[i];
    if(ch === '"'){ q = !q; continue; }
    if(ch === "," && !q){ out.push(cur); cur=""; continue; }
    cur += ch;
  }
  out.push(cur);
  return out.map(s=>s.trim());
}

// ====== 1) Index: Return% + Log(rebased) ======
function parseIndexCsv(text){
  const lines = text.trim().split(/\r?\n/);
  const header = splitCsv(lines.shift()).map(h=>h.toLowerCase());
  const iDate = header.indexOf("date");
  const iBM20 = header.indexOf("bm20_index");
  const iBTCi = header.indexOf("btc_index"); // optional
  const iBTCp = header.indexOf("btc_price"); // optional

  // 기준일 찾기
  let baseDate = BASE_DATE === "auto" ? null : BASE_DATE;
  let baseBM=null, baseBTC=null;

  const rows = [];
  for(const l of lines){
    if(!l.trim()) continue;
    const c = splitCsv(l);
    const d = c[iDate];
    const bm = parseFloat(c[iBM20]);
    const bi = iBTCi >= 0 ? parseFloat(c[iBTCi]) : NaN;
    const bp = iBTCp >= 0 ? parseFloat(c[iBTCp]) : NaN;
    if(!d || !Number.isFinite(bm)) continue;
    rows.push({d,bm,bi,bp});

    if(!baseDate) baseDate = d;
    if(d === baseDate){
      baseBM  = bm;
      baseBTC = Number.isFinite(bi) ? 100 : bp;
    }
  }
  if(!baseBM || !baseBTC) {
    console.warn("BASE_DATE 데이터를 찾지 못했습니다.");
    return {dates:[], bmRet:[], btcRet:[], bmNorm:[], btcNorm:[], baseDate: baseDate||"-"};
  }

  const dates=[], bmRet=[], btcRet=[], bmNorm=[], btcNorm=[];
  for(const r of rows){
    const bm100  = (r.bm / baseBM) * 100.0;
    const btc100 = Number.isFinite(r.bi) ? r.bi : (Number.isFinite(r.bp) ? (r.bp / baseBTC) * 100.0 : NaN);

    dates.push(r.d);
    bmNorm.push(bm100);
    btcNorm.push(Number.isFinite(btc100) ? btc100 : null);
    bmRet.push((bm100 / 100 - 1) * 100);
    btcRet.push(Number.isFinite(btc100) ? ((btc100 / 100 - 1) * 100) : null);
  }

  return {dates, bmRet, btcRet, bmNorm, btcNorm, baseDate};
}

function renderReturnChart(ctx){
  const el = document.getElementById("bm20-return");
  const chart = echarts.init(el);
  chart.setOption({
    title: { text: "Return since Base (BTC vs BM20)" },
    tooltip: { trigger: "axis", valueFormatter: v => (v==null? "": (v>=0? "+":"") + v.toFixed(2) + "%") },
    legend: { data: ["BTC","BM20"] },
    xAxis: { type: "category", data: ctx.dates },
    yAxis: { type: "value", name: "Return (%)", axisLabel: { formatter: v => v + "%" } },
    dataZoom: [{ type: "inside" }, { type: "slider" }],
    series: [
      { name: "BTC",  type: "line", showSymbol:false, smooth:true, data: ctx.btcRet, lineStyle:{ type:"dashed" } },
      { name: "BM20", type: "line", showSymbol:false, smooth:true, data: ctx.bmRet }
    ],
    graphic: [{ type:"text", left:0, top:0, style:{ text:"Base: " + ctx.baseDate + "  (Return)", fill:"#888" } }]
  });
  addEventListener("resize",()=>chart.resize());
}

function renderLogChart(ctx){
  const el = document.getElementById("bm20-log");
  const chart = echarts.init(el);
  chart.setOption({
    title: { text: "Index (Log Scale, rebased = 100 at " + ctx.baseDate + ")" },
    tooltip: { trigger: "axis" },
    legend: { data: ["BM20 Index","BTC (normalized)"] },
    xAxis: { type: "category", data: ctx.dates },
    yAxis: { type: "log", name: "Index (log)", logBase: 10 },
    dataZoom: [{ type: "inside" }, { type: "slider" }],
    series: [
      { name:"BM20 Index",       type:"line", showSymbol:false, smooth:true, data: ctx.bmNorm },
      { name:"BTC (normalized)", type:"line", showSymbol:false, smooth:true, data: ctx.btcNorm, lineStyle:{ type:"dashed" } }
    ]
  });
  addEventListener("resize",()=>chart.resize());
}

function renderIndexMeta(ctx){
  const meta = document.getElementById("bm20-compare-meta");
  const lastBM  = ctx.bmNorm.at(-1);
  const lastBTC = ctx.btcNorm.at(-1);
  const lastDt  = ctx.dates.at(-1);
  meta.textContent = `Base ${ctx.baseDate} → Latest ${lastDt||"-"} · BM20 ${lastBM?.toFixed(2)??"-"} · BTC ${lastBTC?.toFixed(2)??"-"} (rebased)`;
}

// 실행
fetchCsv(CSV_INDEX).then(parseIndexCsv).then(ctx => {
  renderReturnChart(ctx);
  renderLogChart(ctx);
  renderIndexMeta(ctx);
});

// ====== 2) Weights Pie ======
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
    const cap=parseFloat((c[iCap]||"0").replace(/,/g,""));
    if(Number.isFinite(cap)) rows.push({symbol:sym,name,market_cap:cap});
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

// 실행
fetchCsv(CSV_WEIGHTS)
  .then(parseWeightCsv)
  .then(normalizeMcap)
  .then(rows=>{
    renderPie("bm20-raw-pie","bm20-raw-meta",rows,"Market Cap weights (normalized)");
    const fixed=applyBM20(rows);
    renderPie("bm20-fixed-pie","bm20-fixed-meta",fixed,"BM20 methodology (BTC30, ETH20, XRP/BNB/USDT 5 each, rest equally share 35%)");
  });
</script>


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

<script src="https://cdn.jsdelivr.net/npm/echarts@5"></script>
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
