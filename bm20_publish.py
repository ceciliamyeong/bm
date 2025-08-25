#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
BM20 Publisher: latest.json → /bm/latest.html (뉴스리포트 형식)
- 날짜별 HTML 생성 안 함. latest.html 만 갱신.
- 차트 PNG가 있으면 /bm/bm20_*_latest.png 로 복사해 사용(없어도 동작).
- 데이터 우선순위:
  1) repo 루트의 latest.json / series.json
  2) out/YYYY-MM-DD/ 폴더의 파일들(가장 최신 날짜)
  3) 선택: out/YYYY-MM-DD/bm20_perf_YYYY-MM-DD.json (있으면 TOP3/Worst3 출력)
  4) 선택: out/YYYY-MM-DD/kimchi_*.json, funding_*.json (있으면 병합)
"""

from __future__ import annotations
import json, html, shutil, sys
from pathlib import Path
import datetime as dt

ROOT = Path(__file__).resolve().parent
OUT  = ROOT / "out"
BM   = ROOT / "bm"

# ---------- helpers ----------
def fmt(v, suffix=""):
    try:
        return f"{float(v):,.2f}{suffix}"
    except Exception:
        return "—"

def sign(v):
    try:
        v = float(v)
        return f"{v:+.2f}%"
    except Exception:
        return "—"

def find_latest_dir() -> Path | None:
    if not OUT.exists():
        return None
    dated = [p for p in OUT.iterdir() if p.is_dir()]
    dated = [p for p in dated if len(p.name) == 10 and p.name[:4].isdigit()]
    return sorted(dated, key=lambda p: p.name)[-1] if dated else None

def read_json(p: Path) -> dict | None:
    try:
        if p and p.exists():
            return json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[warn] JSON read fail: {p}: {e}")
    return None

# ---------- load data ----------
def load_latest_and_series():
    latest_src = series_src = None

    # 1) 루트 + bm/ 도 함께 후보에 포함  ←★추가
    latest = None
    for p in [
        ROOT / "latest.json",
        ROOT / "bm20_latest.json",
        ROOT / "bm" / "latest.json",         # ← 추가
        ROOT / "bm" / "bm20_latest.json",    # ← 추가
    ]:
        latest = read_json(p)
        if latest:
            latest_src = p.as_posix()
            break

    series = None
    for p in [
        ROOT / "series.json",
        ROOT / "bm20_series.json",
        ROOT / "bm" / "series.json",         # ← 추가
        ROOT / "bm" / "bm20_series.json",    # ← 추가
    ]:
        series = read_json(p)
        if series:
            series_src = p.as_posix()
            break

    # 2) out/YYYY-MM-DD 보조
    latest_dir = find_latest_dir()
    if latest is None and latest_dir:
        for p in [
            latest_dir / "latest.json",
            latest_dir / f"bm20_meta_{latest_dir.name}.json",
            latest_dir / f"bm20_latest.json",
        ]:
            latest = read_json(p)
            if latest:
                latest_src = p.as_posix()
                break
    if series is None and latest_dir:
        for p in [latest_dir / "series.json", latest_dir / "bm20_series.json"]:
            series = read_json(p)
            if series:
                series_src = p.as_posix()
                break

    print(f"[load] latest.json => {latest_src or 'NOT FOUND'}")
    print(f"[load] series.json => {series_src or 'NOT FOUND'}")
    return latest, series, latest_dir
  
def load_optional(latest_dir: Path):
    """perf(top/worst), kimchi/funding 있으면 읽어옴(없어도 무시)"""
    top3, worst3 = [], []
    kimchi = None
    f_btc = f_eth = None

    if latest_dir:
        ymd = latest_dir.name
        perf = read_json(latest_dir / f"bm20_perf_{ymd}.json")
        if perf and "perf_1d" in perf:
            arr = [x for x in perf["perf_1d"] if x.get("ret_1d") is not None]
            arr.sort(key=lambda x: x.get("ret_1d", 0.0), reverse=True)
            top3 = arr[:3]
            worst3 = arr[-3:][::-1] if len(arr) >= 3 else arr[::-1]

        k = read_json(latest_dir / f"kimchi_{ymd}.json")
        if k and "kimchi_premium" in k:
            kimchi = k["kimchi_premium"]

        f = read_json(latest_dir / f"funding_{ymd}.json")
        if f:
            f_btc = f.get("funding_btc")
            f_eth = f.get("funding_eth")

    return top3, worst3, kimchi, f_btc, f_eth

# ---------- html ----------
def render_html(latest: dict, date_str: str, bar_img: str | None, trend_img: str | None,
                top3, worst3, kimchi, f_btc, f_eth) -> str:

    level = latest.get("bm20Level") or latest.get("index")
    prev  = latest.get("bm20PrevLevel")
    chgpt = latest.get("bm20PointChange")
    chgp  = latest.get("bm20ChangePct") or latest.get("change_1d")
    rets  = latest.get("returns") or {}

    # returns 키 이름 정규화
    r1d  = rets.get("1D")  if rets else latest.get("ret_1d")
    r7d  = rets.get("7D")  if rets else latest.get("ret_7d")
    r30d = rets.get("30D") if rets else latest.get("ret_30d")
    rmtd = rets.get("MTD") if rets else latest.get("ret_mtd")
    rytd = rets.get("YTD") if rets else latest.get("ret_ytd")

    def rows(items, title):
        if not items:
            return f'<table><tr><th>{title}</th><th style="text-align:right">등락률</th></tr><tr><td colspan="2" style="text-align:center">—</td></tr></table>'
        tr = "".join(
            f'<tr><td>{html.escape(str(i.get("symbol") or i.get("coin_id") or "-")).upper()}</td>'
            f'<td style="text-align:right">{sign(i.get("ret_1d"))}</td></tr>'
            for i in items
        )
        return f'<table><tr><th>{title}</th><th style="text-align:right">등락률</th></tr>{tr}</table>'

    bar_tag  = f'<p class="center"><img src="{bar_img}" alt="Performance"></p>' if bar_img  else ""
    trd_tag  = f'<p class="center"><img src="{trend_img}" alt="Trend"></p>'      if trend_img else ""
    fund_txt = (f"BTC {f_btc} / ETH {f_eth}") if (f_btc or f_eth) else "—"

    return f"""<!doctype html><html lang="ko"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>BM20 Daily — {html.escape(date_str)}</title>
<style>
body{{font-family:-apple-system,BlinkMacSystemFont,"NanumGothic","Noto Sans CJK","Malgun Gothic",Arial,sans-serif;background:#fafbfc;color:#111;margin:0}}
.wrap{{max-width:760px;margin:0 auto;padding:20px}}
.card{{background:#fff;border:1px solid #e5e9f0;border-radius:12px;padding:20px;margin-bottom:16px}}
h1{{font-size:22px;margin:0 0 8px 0;text-align:center}}
h2{{font-size:15px;margin:16px 0 8px 0;color:#1A237E}}
.muted{{color:#555;text-align:center}} .center{{text-align:center}}
table{{width:100%;border-collapse:collapse;font-size:14px}} th,td{{border:1px solid #e5e9f0;padding:8px}} th{{background:#eef4ff}}
.footer{{font-size:12px;color:#666;text-align:center;margin-top:16px}}
img{{max-width:100%}}
nav{{display:flex;gap:14px;justify-content:center;margin:8px 0 16px}}
nav a{{color:#1A237E;text-decoration:none}}
nav a:hover{{text-decoration:underline}}
.badge{{display:inline-block;background:#eef4ff;border:1px solid #d6e0ff;padding:4px 8px;border-radius:8px;margin:2px}}
</style></head><body>
<div class="wrap">
  <div class="card">
    <h1><a href="https://ceciliamyeong.github.io/bm/" style="text-decoration:none;color:inherit">BM20 데일리 리포트</a></h1>
    <div class="muted">{html.escape(date_str)}</div>
    <nav>
      <a href="https://ceciliamyeong.github.io/bm/">Home</a>
      <a href="https://ceciliamyeong.github.io/bm/latest.html">Latest</a>
      <a href="https://ceciliamyeong.github.io/bm/indices/performance/">Charts</a>
      <a href="https://ceciliamyeong.github.io/bm/indices/overview/">Data</a>
      <a href="https://ceciliamyeong.github.io/bm/indices/methodology/">About</a>
    </nav>
    <p class="center">
      <span class="badge">지수 {fmt(level)}</span>
      <span class="badge">변동 {fmt(chgpt, ' pt')} · {sign(chgp)}</span>
    </p>
    <table>
      <tr><th>지수</th><td>{fmt(level)}</td></tr>
      <tr><th>전일</th><td>{fmt(prev)}</td></tr>
      <tr><th>포인트/변동률</th><td>{fmt(chgpt, ' pt')} / {sign(chgp)}</td></tr>
      <tr><th>수익률(1D/7D/30D/MTD/YTD)</th><td>{sign(r1d)} / {sign(r7d)} / {sign(r30d)} / {sign(rmtd)} / {sign(rytd)}</td></tr>
      <tr><th>김치 프리미엄</th><td>{sign(kimchi)}</td></tr>
      <tr><th>펀딩비(Binance)</th><td>{fund_txt}</td></tr>
    </table>
  </div>

  <div class="card">
    <h2>코인별 퍼포먼스 (1D, USD)</h2>
    {bar_tag}
    <h2>상승/하락 TOP3</h2>
    {rows(top3, "상승")}
    <br/>
    {rows(worst3, "하락")}
  </div>

  <div class="card">
    <h2>BTC & ETH 7일 가격 추세</h2>
    {trd_tag}
  </div>

  <div class="footer">© Blockmedia · Data: latest.json / series.json (+ optional perf/kimchi/funding)</div>
</div></body></html>
"""

# ---------- main ----------
def main():
    latest, series, latest_dir = load_latest_and_series()
    if not latest:
        print("::error::latest.json not found at root or out/")
        sys.exit(1)

    # 날짜(문자열) 추출
    date_str = latest.get("asOf") or latest.get("date") or (latest_dir.name if latest_dir else dt.date.today().isoformat())

    # 선택 데이터 로드
    top3, worst3, kimchi, f_btc, f_eth = load_optional(latest_dir)

    # 차트 PNG 최신본 복사 (/bm/bm20_*_latest.png)
    BM.mkdir(parents=True, exist_ok=True)
    bar_img = trend_img = None
    if latest_dir:
        ymd = latest_dir.name
        src_bar = latest_dir / f"bm20_bar_{ymd}.png"
        src_trd = latest_dir / f"bm20_trend_{ymd}.png"
        if src_bar.exists():
            shutil.copyfile(src_bar, BM / "bm20_bar_latest.png")
            bar_img = "bm20_bar_latest.png"
        if src_trd.exists():
            shutil.copyfile(src_trd, BM / "bm20_trend_latest.png")
            trend_img = "bm20_trend_latest.png"

    html_txt = render_html(latest, date_str, bar_img, trend_img, top3, worst3, kimchi, f_btc, f_eth)

    # /bm/latest.html 작성 + 루트 latest.html도 동일본으로 복사
    (BM / "latest.html").write_text(html_txt, encoding="utf-8")
    (ROOT / "latest.html").write_text(html_txt, encoding="utf-8")
    print(f"[OK] wrote bm/latest.html (date={date_str})")

if __name__ == "__main__":
    main()
