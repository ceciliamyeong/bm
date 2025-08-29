---
title: "Portfolio 2025Q3"
layout: default
permalink: /indices/portfolio-2025q3/
active: portfolio
---

# BM20 Portfolio (2025 Q3)

<div id="bm20-weights-pie" style="height:420px; max-width:1000px; margin:16px 0;"></div>
<div id="bm20-weights-meta" style="font:12px/1.6 system-ui,-apple-system,Segoe UI,Roboto,sans-serif;color:#666;"></div>

<script src="https://cdn.jsdelivr.net/npm/echarts@5"></script>
<script>
// ✅ 구글 시트 CSV (공개 퍼블리시 상태여야 합니다)
const CSV_WEIGHTS = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTndyrPd3WWwFtfzv2CZxJeDcH-l8ibQIdO5ouYS4HsaGpbeXQQbs6WEr9qPqqZbRoT6cObdFxJpief/pub?gid=1533548287&single=true&output=csv";

// 1) CSV fetch (캐시 무효화)
async function fetchCsv(url){
  const u = url + (url.includes("?") ? "&" : "?") + "v=" + Date.now();
  const res = await fetch(u, { cache: "no-store" });
  if(!res.ok) throw new Error("CSV fetch failed: " + res.status);
  return res.text();
}

// 2) CSV 파싱: 컬럼 예시 → name,symbol,weight (weight는 % 또는 0~1)
function parsePortfolioCsv(text){
  const lines = text.trim().split(/\r?\n/);
  // 간단 CSV 파서: 따옴표 포함 셀 분리 지원
  const splitCsv = (row) => {
    const out = []; let cur = ""; let q = false;
    for (let i=0;i<row.length;i++){
      const ch = row[i], nx = row[i+1];
      if (ch === '"'){ q = !q; continue; }
      if (ch === ',' && !q){ out.push(cur); cur = ""; continue; }
      cur += ch;
    }
    out.push(cur);
    return out.map(s=>s.trim());
  };

  const header = splitCsv(lines.shift()).map(h=>h.toLowerCase());
  const iName   = header.indexOf("name");
  const iSymbol = header.indexOf("symbol");
  const iWeight = header.indexOf("weight");

  const rows = [];
  for(const raw of lines){
    if(!raw.trim()) continue;
    const c = splitCsv(raw);
    const name   = (c[iName]   ?? "").replace(/"/g,"").trim();
    const symbol = (c[iSymbol] ?? "").replace(/"/g,"").trim().toUpperCase();
    let w = (c[iWeight] ?? "0").replace(/[%"]/g,"").trim();
    let weight = parseFloat(w);
    if (!Number.isFinite(weight)) continue;
    if (weight > 1.0001) weight = weight/100; // 37.5 → 0.375
    rows.push({ name: name || symbol, symbol, weight });
  }
  // 정규화 (총합=1)
  const sum = rows.reduce((a,b)=>a+b.weight,0) || 1;
  rows.forEach(r => r.weight = r.weight / sum);
  // 내림차순 정렬
  return rows.sort((a,b)=>b.weight - a.weight);
}

// 3) 파이차트 렌더
function renderPie(rows){
  const el = document.getElementById("bm20-weights-pie");
  const chart = echarts.init(el);

  // 상위 10 + Others 로 요약
  const data = rows.map(r => ({ name: r.symbol || r.name, value: +(r.weight*100).toFixed(4) }));
  const topN = data.slice(0,10);
  const rest = data.slice(10).reduce((a,b)=>a+b.value,0);
  if (rest > 0) topN.push({ name: "Others", value: +rest.toFixed(4) });

  chart.setOption({
    tooltip: { trigger: "item", formatter: p => `${p.name}: ${p.value.toFixed(2)}%` },
    legend:  { type: "scroll", orient: "vertical", right: 0, top: "middle" },
    series: [{
      name: "BM20 Weights",
      type: "pie",
      radius: ["40%","70%"],
      center: ["38%","50%"],
      avoidLabelOverlap: true,
      label: { formatter: "{b}\n{d}%" },
      data: topN
    }]
  });

  const meta = document.getElementById("bm20-weights-meta");
  meta.textContent = `총 ${data.length}개 구성 · 상위 10 + Others 표시 · 합계 100% 기준`;
  addEventListener("resize", ()=>chart.resize());
}

// 실행
fetchCsv(CSV_URL)
  .then(parsePortfolioCsv)
  .then(renderPie)
  .catch(err => {
    console.error(err);
    document.getElementById("bm20-weights-meta").textContent = "파이차트 로드 실패: " + err.message;
  });
</script>

<script>
const SNAP_URL = location.origin + "/bm20_lastest.json";   // 루트 SOT
const CSV_URL  = location.origin + "/bm20_vs_bench.csv";   // 루트 CSV (스샷 파일)

async function getJSON(u){
  const r = await fetch(u + "?v=" + Date.now(), {cache:"no-store"});
  if(!r.ok) throw new Error("HTTP " + r.status);
  return r.json();
}
async function getCSV(u){
  const r = await fetch(u + "?v=" + Date.now(), {cache:"no-store"});
  if(!r.ok) throw new Error("HTTP " + r.status);
  const text = await r.text();
  // 아주 단순 CSV 파서(쉼표 기준)
  const [header, ...rows] = text.trim().split(/\r?\n/).map(l=>l.split(","));
  const idx = (name)=> header.indexOf(name);
  return rows.map(c => ({
    date: c[idx("date")],
    bm20: parseFloat(c[idx("BM20_rel")]),
    btc:  parseFloat(c[idx("BTC_rel")]),
    eth:  parseFloat(c[idx("ETH_rel")]),
    over_btc: parseFloat(c[idx("BM20_over_BTC")]),
    over_eth: parseFloat(c[idx("BM20_over_ETH")]),
  }));
}

// ---- 렌더러(컨테이너 ID는 페이지에 맞춰 바꿔도 됨)
async function render() {
  // 1) 스냅샷 → 바 차트/Best-Worst
  try {
    const snap = await getJSON(SNAP_URL);
    const asof = snap.asof || snap.date || "";
    const barArr = (snap.bar || snap.components || []).map(o=>({
      symbol: o.symbol || o.ticker || o.name || "?",
      v: Number(o.pct_1d ?? o.change_1d ?? o.pct ?? 0)
    }));
    const barEl = echarts.init(document.getElementById("bm20-bar"));
    barEl.setOption({
      tooltip:{trigger:"axis",axisPointer:{type:"shadow"},valueFormatter:v=> (v>0?"+":"")+Number(v).toFixed(2)+"%"},
      grid:{left:48,right:24,top:24,bottom:36},
      xAxis:{type:"category",data:barArr.map(x=>x.symbol)},
      yAxis:{type:"value"},
      series:[{type:"bar",data:barArr.map(x=>x.v),barMaxWidth:22}]
    });
    document.getElementById("bm20-asof")?.replaceChildren(document.createTextNode("기준: " + asof));

    // Best/Worst 테이블 있으면 채우기
    const tbody = document.querySelector("#bm20-bw tbody");
    if (tbody){
      let top3 = snap.top3 || snap.best3 || [];
      let bottom3 = snap.bottom3 || snap.worst3 || [];
      if ((!top3.length || !bottom3.length) && barArr.length){
        const s=[...barArr].sort((a,b)=>b.v-a.v);
        top3 = s.slice(0,3).map(x=>({symbol:x.symbol,pct_1d:x.v}));
        bottom3 = s.slice(-3).reverse().map(x=>({symbol:x.symbol,pct_1d:x.v}));
      }
      tbody.innerHTML = "";
      [...top3.map(x=>({...x,_:"best"})), ...bottom3.map(x=>({...x,_:"worst"}))].forEach(o=>{
        const v = Number(o.pct_1d||o.pct||0);
        const tr = document.createElement("tr");
        tr.innerHTML = `<td>${o.symbol||o.name||"-"}</td>
                        <td style="color:${v>=0?"#059669":"#dc2626"}">${(v>0?"+":"")+v.toFixed(2)}%</td>
                        <td>${o.note||""}</td>`;
        tbody.appendChild(tr);
      });
    }
  } catch(e) {
    console.warn("snapshot fail", e);
  }

  // 2) CSV 히스토리 → 라인 차트(BM20/BTC/ETH 상대지수)
  try {
    const rows = await getCSV(CSV_URL);
    const dates = rows.map(r=>r.date);
    const bm20 = rows.map(r=>r.bm20);
    const btc  = rows.map(r=>r.btc);
    const eth  = rows.map(r=>r.eth);

    const lineEl = echarts.init(document.getElementById("bm20-trend"));
    lineEl.setOption({
      tooltip:{trigger:"axis"},
      legend:{data:["BM20","BTC","ETH"]},
      grid:{left:48,right:24,top:36,bottom:36},
      xAxis:{type:"category",data:dates},
      yAxis:{type:"value"},
      series:[
        {name:"BM20",type:"line",data:bm20,smooth:true},
        {name:"BTC", type:"line",data:btc, smooth:true},
        {name:"ETH", type:"line",data:eth, smooth:true},
      ]
    });
  } catch(e) {
    console.warn("csv fail", e);
  }
  window.addEventListener("resize", ()=> {
    echarts.getInstanceByDom(document.getElementById("bm20-bar"))?.resize();
    echarts.getInstanceByDom(document.getElementById("bm20-trend"))?.resize();
  }, {passive:true});
}

render();
</script>

