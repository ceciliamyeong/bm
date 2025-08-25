---
title: "Performance"
layout: default
permalink: /indices/performance/
active: performance
---

# BM20 Performance

## Index Trend (Raw vs BM20)
<div id="bm20-compare-line" style="height:420px; max-width:1000px; margin:16px auto;"></div>
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
(async function(){
  // 🔧 데이터 경로: 현재는 루트(/bm) 기준에 있음
  const SERIES_JSON = "{{ site.baseurl }}/series.json";      // Raw/BM20 시계열
  const LATEST_JSON = "{{ site.baseurl }}/latest.json";      // (선택) 메타 표시
  const WEIGHTS_CSV = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTndyrPd3WWwFtfzv2CZxJeDcH-l8ibQIdO5ouYS4HsaGpbeXQQbs6WEr9qPqqZbRoT6cObdFxJpief/pub?gid=1533548287&single=true&output=csv";

  const bust = (u)=> u + (u.includes("?")?"&":"?") + "v=" + Date.now();

  // ==== Index Trend (Raw vs BM20) from series.json ====
  const r = await fetch(bust(SERIES_JSON), {cache:"no-store"});
  if(!r.ok){ console.error("series.json fetch failed:", r.status); return; }
  const series = await r.json();

  // 허용 키: date, raw_index, bm20_index
  const norm = s => String(s||"").toLowerCase().replace(/\s+/g,"").replace(/_/g,"");
  const get  = (o, ks) => {
    for(const k of Object.keys(o)){ if(ks.some(x=>norm(x)===norm(k))) return o[k]; }
    return undefined;
  };

  const rawData=[], bm20Data=[];
  for(const row of series){
    const d   = get(row, ["date","날짜"]);
    const raw = Number(get(row, ["raw_index","rawindex","raw"]));
    const bm  = Number(get(row, ["bm20_index","bm20index","bm20"]));
    if(d && Number.isFinite(raw) && Number.isFinite(bm)){
      rawData.push([d, raw]);
      bm20Data.push([d, bm]);
    }
  }

  const lineEl = document.getElementById("bm20-compare-line");
  const line = echarts.init(lineEl);
  line.setOption({
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
  addEventListener("resize", ()=>line.resize());

  // 메타(선택)
  try{
    const lr = await fetch(bust(LATEST_JSON), {cache:"no-store"});
    if(lr.ok){
      const lastDate = rawData.at(-1)?.[0];
      const lastRaw  = rawData.at(-1)?.[1];
      const lastBM20 = bm20Data.at(-1)?.[1];
      const meta = document.getElementById("bm20-compare-meta");
      if(meta) meta.textContent = `Latest ${lastDate||"-"} · Raw ${Number(lastRaw).toFixed(2)} vs BM20 ${Number(lastBM20).toFixed(2)}`;
    }
  }catch(e){ /* optional */ }

  // ==== Weights ====
  async function fetchCsv(url){
    const res=await fetch(bust(url), {cache:"no-store"});
    if(!res.ok) throw new Error("CSV fetch failed: "+res.status);
    return res.text();
  }
  function splitCsv(row){
    const out=[]; let cur=""; let q=false;
    for(let i=0;i<row.length;i++){
      const ch=row[i];
      if(ch === '"'){ q=!q; continue; }
      if(ch === "," && !q){ out.push(cur); cur=""; continue; }
      cur += ch;
    }
    out.push(cur);
    return out.map(s=>s.trim());
  }
  function parseWeightCsv(text){
    const lines=text.trim().split(/\r?\n/).filter(Boolean);
    const header=splitCsv(lines.shift()).map(h=>h.toLowerCase());
    const iSym=header.indexOf("symbol");
    const iCap=header.indexOf("market_cap");
    const iName=header.indexOf("name");
    const rows=[];
    for(const l of lines){
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
    const meta = document.getElementById(metaId);
    if(meta) meta.textContent = label + " · 총 " + data.length + "개 종목";
    addEventListener("resize",()=>chart.resize());
  }

  try{
    const wText = await fetchCsv(WEIGHTS_CSV);
    const rows  = normalizeMcap(parseWeightCsv(wText));
    renderPie("bm20-raw-pie","bm20-raw-meta",rows,"Market Cap weights (normalized)");
    const fixed = applyBM20(rows);
    renderPie("bm20-fixed-pie","bm20-fixed-meta",fixed,"BM20 methodology (BTC30, ETH20, XRP/BNB/USDT 5 each, rest equally share 35%)");
  }catch(err){
    console.error(err);
    const meta = document.getElementById("bm20-raw-meta");
    if(meta) meta.textContent = "Weights 로딩 실패: " + err.message;
  }
})();
</script>
