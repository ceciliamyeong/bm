---
title: "Components"
layout: default
permalink: /indices/components/
active: components
---


# BM20 구성
- (구성 테이블)
<div id="bm20-weights-heatmap" style="height:600px; max-width:900px; margin:16px auto;"></div>
<script src="https://cdn.jsdelivr.net/npm/echarts@5"></script>
<script>
(async function(){
  const CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTndyrPd3WWwFtfzv2CZxJeDcH-l8ibQIdO5ouYS4HsaGpbeXQQbs6WEr9qPqqZbRoT6cObdFxJpief/pub?gid=352245628&single=true&output=csv";
  
  // CSV fetch
  const res = await fetch(CSV_URL+"?t="+Date.now(), {cache:"no-store"});
  const text = await res.text();
  
  // CSV 파싱 (심볼, 시총, 이름)
  function splitCsv(row){
    const out=[];let cur="",q=false;
    for(let i=0;i<row.length;i++){
      const ch=row[i];
      if(ch=='"'){q=!q;continue;}
      if(ch==","&&!q){out.push(cur);cur="";continue;}
      cur+=ch;
    }
    out.push(cur);
    return out.map(s=>s.trim());
  }
  const lines = text.trim().split(/\r?\n/);
  const header = splitCsv(lines.shift()).map(h=>h.toLowerCase());
  const iSym = header.indexOf("symbol");
  const iCap = header.indexOf("market cap");
  const iName= header.indexOf("name");
  
  let rows=[];
  for(const l of lines){
    if(!l.trim()) continue;
    const c=splitCsv(l);
    const sym=c[iSym].toUpperCase();
    const name=c[iName]||sym;
    const cap=parseFloat(c[iCap].replace(/,/g,""));
    if(Number.isFinite(cap)){
      rows.push({symbol:sym,name,cap});
    }
  }
  
  // 가중치 계산
  const sum = rows.reduce((a,b)=>a+b.cap,0)||1;
  const data = rows.map(r=>({
    name: r.symbol,
    value: +(r.cap/sum*100).toFixed(2),  // %
    label: {formatter: r.symbol+"\n"+(r.cap/sum*100).toFixed(1)+"%"}
  }));
  
  const chart = echarts.init(document.getElementById("bm20-weights-heatmap"));
  chart.setOption({
    tooltip: {formatter: p=>`${p.name}: ${p.value.toFixed(2)}%`},
    series: [{
      type: 'treemap',
      data,
      roam: false,
      label: {show:true, position:'inside', fontSize:12},
      levels:[{
        itemStyle: { borderColor:'#fff', borderWidth:2, gapWidth:2 }
      }]
    }]
  });
})();

<h2>BM20 Constituents</h2>
<div style="display:flex; flex-wrap:wrap; gap:24px;">
  <div style="flex:1; min-width:320px;">
    <table id="bm20-table" border="1" cellspacing="0" cellpadding="6"
           style="border-collapse:collapse; font:14px system-ui, sans-serif; width:100%;">
      <thead style="background:#f0f0f0;">
        <tr><th>Symbol</th><th>Name</th><th>Market Cap (USD)</th><th>Weight %</th></tr>
      </thead>
      <tbody></tbody>
    </table>
  </div>
  <div id="bm20-weights-heatmap" style="flex:1; min-width:320px; height:600px; max-width:600px;"></div>
</div>

<script src="https://cdn.jsdelivr.net/npm/echarts@5"></script>
<script>
(async function(){
  const CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTndyrPd3WWwFtfzv2CZxJeDcH-l8ibQIdO5ouYS4HsaGpbeXQQbs6WEr9qPqqZbRoT6cObdFxJpief/pub?gid=352245628&single=true&output=csv";
  
  // CSV fetch
  const res = await fetch(CSV_URL+"?t="+Date.now(), {cache:"no-store"});
  const text = await res.text();
  
  function splitCsv(row){
    const out=[];let cur="",q=false;
    for(let i=0;i<row.length;i++){
      const ch=row[i];
      if(ch=='"'){q=!q;continue;}
      if(ch==","&&!q){out.push(cur);cur="";continue;}
      cur+=ch;
    }
    out.push(cur);
    return out.map(s=>s.trim());
  }
  const lines = text.trim().split(/\r?\n/);
  const header = splitCsv(lines.shift()).map(h=>h.toLowerCase());
  const iSym = header.indexOf("symbol");
  const iCap = header.indexOf("market cap");
  const iName= header.indexOf("name");
  
  let rows=[];
  for(const l of lines){
    if(!l.trim()) continue;
    const c=splitCsv(l);
    const sym=(c[iSym]||"").toUpperCase();
    const name=c[iName]||sym;
    const cap=parseFloat((c[iCap]||"0").replace(/,/g,""));
    if(Number.isFinite(cap)){
      rows.push({symbol:sym,name,cap});
    }
  }
  
  // 정렬 + 비중 계산
  const sum = rows.reduce((a,b)=>a+b.cap,0)||1;
  rows = rows.map(r=>({...r, weight: r.cap/sum*100}));
  rows.sort((a,b)=>b.weight - a.weight);
  
  // 테이블 채우기
  const tbody=document.querySelector("#bm20-table tbody");
  rows.forEach(r=>{
    const tr=document.createElement("tr");
    tr.innerHTML = `<td>${r.symbol}</td>
                    <td>${r.name}</td>
                    <td style="text-align:right">${r.cap.toLocaleString()}</td>
                    <td style="text-align:right">${r.weight.toFixed(2)}%</td>`;
    tbody.appendChild(tr);
  });
  
  // 트리맵 데이터
  const data = rows.map(r=>({
    name:r.symbol,
    value:r.weight,
    label:{formatter: r.symbol+"\n"+r.weight.toFixed(1)+"%"}
  }));
  
  const chart = echarts.init(document.getElementById("bm20-weights-heatmap"));
  chart.setOption({
    tooltip:{formatter: p=>`${p.name}: ${p.value.toFixed(2)}%`},
    series:[{
      type:'treemap',
      data,
      roam:false,
      label:{show:true,position:'inside',fontSize:12},
      levels:[{itemStyle:{borderColor:'#fff',borderWidth:2,gapWidth:2}}]
    }]
  });
})();
</script>

</script>
<!-- 히트맵 컨테이너 -->
<div id="bm20-weights-heatmap" style="height:600px; max-width:900px; margin:16px auto;"></div>

<!-- ECharts -->
<script src="https://cdn.jsdelivr.net/npm/echarts@5"></script>
<script>
(async function(){
  // ▼ CSV 주소를 원하는 탭(gid)으로 바꿔 쓰세요.
  const CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTndyrPd3WWwFtfzv2CZxJeDcH-l8ibQIdO5ouYS4HsaGpbeXQQbs6WEr9qPqqZbRoT6cObdFxJpief/pub?gid=1533548287&single=true&output=csv";

  // fetch CSV
  const res = await fetch(CSV_URL + "&t=" + Date.now(), {cache:"no-store"});
  if(!res.ok){ throw new Error("CSV fetch failed: "+res.status); }
  const text = await res.text();

  // 간단 CSV 파서
  function splitCsv(row){
    const out=[]; let cur="", q=false;
    for(let i=0;i<row.length;i++){
      const ch=row[i];
      if(ch==='"'){ q=!q; continue; }
      if(ch===',' && !q){ out.push(cur); cur=""; continue; }
      cur+=ch;
    }
    out.push(cur);
    return out.map(s=>s.trim());
  }

  const lines = text.trim().split(/\r?\n/);
  const header = splitCsv(lines.shift()).map(h=>h.toLowerCase());
  const iSym = header.indexOf("symbol");
  const iCap = header.indexOf("market cap");   // 시총 탭이면 이 헤더
  const iW   = header.indexOf("weight");       // 메서돌로지 탭이면 이 헤더
  const iName= header.indexOf("name");

  // 행 파싱 → (symbol, name, weight%)
  let rows=[];
  for(const l of lines){
    if(!l.trim()) continue;
    const c = splitCsv(l);
    const sym  = (c[iSym] || "").toUpperCase();
    const name = c[iName] || sym;
    let weightPct;

    if(iW >= 0){ // 메서돌로지 탭: weight(%)가 바로 들어있다고 가정
      weightPct = parseFloat(c[iW]);
    } else if(iCap >= 0){ // 시총 탭: 시총 합으로 나눠 비중 계산
      const cap = parseFloat((c[iCap] || "0").replace(/,/g,""));
      rows.push({sym, name, cap});
      continue;
    } else {
      continue;
    }
    if(Number.isFinite(weightPct)){
      rows.push({sym, name, weight: weightPct});
    }
  }

  // 시총 탭인 경우 비중 계산
  if(rows.length && rows[0].cap !== undefined){
    const sum = rows.reduce((a,b)=>a+(b.cap||0), 0) || 1;
    rows = rows.map(r => ({sym:r.sym, name:r.name, weight: r.cap/sum*100}));
  }

  // 트리맵 데이터
  rows.sort((a,b)=>b.weight-a.weight);
  const data = rows.map(r=>({
    name: r.sym,
    value: +r.weight.toFixed(4),
    label: { formatter: r.sym + "\n" + r.weight.toFixed(1) + "%" }
  }));

  const el = document.getElementById("bm20-weights-heatmap");
  const chart = echarts.init(el);
  chart.setOption({
    tooltip: { formatter: p => `${p.name}: ${p.value.toFixed(2)}%` },
    series: [{
      type: 'treemap',
      data,
      roam: false,
      label: { show: true, position: 'inside', fontSize: 12 },
      levels: [{ itemStyle: { borderColor: '#fff', borderWidth: 2, gapWidth: 2 } }]
      // 필요하면 색 스케일 추가:
      // visualMin: 0, visualMax: 30, colorMappingBy: 'value'
    }]
  });
  window.addEventListener('resize', ()=>chart.resize());
})().catch(err=>{
  console.error("BM20 heatmap error:", err);
  const el = document.getElementById("bm20-weights-heatmap");
  if(el) el.innerHTML = '<div style="padding:12px;color:#c00;background:#fee;border:1px solid #fcc;border-radius:8px;">히트맵 데이터를 불러오지 못했습니다. Console을 확인하세요.</div>';
});
</script>

