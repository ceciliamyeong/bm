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
  const CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTndyrPd3WWwFtfzv2CZxJeDcH-l8ibQIdO5ouYS4HsaGpbeXQQbs6WEr9qPqqZbRoT6cObdFxJpief/pub?gid=1533548287&single=true&output=csv";
  
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

