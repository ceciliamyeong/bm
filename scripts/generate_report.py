# scripts/generate_report.py
# BM20 데일리 리포트: api/*.json을 읽어 latest.html 생성
# - 데이터가 없어도 절대 크래시하지 않음(빈 섹션 처리)
# - perf_up/down, contrib_top, latest(김치/펀딩) 스키마 유연 대응

import os, json, datetime, glob
from pathlib import Path

ROOT = Path(".")
API  = ROOT / "api"          # 기본 데이터 경로
OUT_HTML = ROOT / "latest.html"

# ----------------- 유틸 -----------------
def load_json(path, default=None):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default if default is not None else {}

def load_first(candidates):
    """여러 후보 중 처음 발견되는 JSON을 로드"""
    for p in candidates:
        try:
            with open(p, "r", encoding="utf-8") as f:
                print(f"[DBG] using file: {p}")
                return json.load(f)
        except Exception:
            pass
    print("[DBG] none of candidates found:", candidates)
    return {}

def pick_date(*candidates):
    for d in candidates:
        if isinstance(d, str) and d:
            return d
    KST = datetime.timezone(datetime.timedelta(hours=9))
    return datetime.datetime.now(tz=KST).date().isoformat()

def normalize_perf(data, kind="up"):
    """
    perf_up.json / perf_down.json → 통일 리스트
    허용:
      {"date":"YYYY-MM-DD","top":[{"symbol":"BTC","ret_24h_pct":1.23},...]}
      {"date":"YYYY-MM-DD","bottom":[...]}
      {"list":[{"sym":"BTC","v":0.0123},...]} (구형)
    반환: (date, [{"symbol","ret_24h_pct"}...])
    """
    if not isinstance(data, dict):
        return None, []
    date = data.get("date")
    keys = ["top", "list"] if kind == "up" else ["bottom", "list"]
    items = None
    for k in keys:
        if k in data and isinstance(data[k], list):
            items = data[k]; break
    if items is None:
        return date, []

    out = []
    for it in items:
        sym = (it.get("symbol") or it.get("sym") or "").upper()
        if not sym:
            continue
        if "ret_24h_pct" in it:
            pct = float(it["ret_24h_pct"])
        elif "v" in it:
            v = float(it["v"])
            pct = v * 100 if abs(v) < 1.0 else v
        else:
            continue
        out.append({"symbol": sym, "ret_24h_pct": round(pct, 4)})

    out.sort(key=lambda r: r["ret_24h_pct"], reverse=True if kind == "up" else False)
    return date, out

def normalize_contrib(data):
    """
    contrib_top.json → {"asof":..., "MTD":[(sym,val),...], "QTD":[...], "YTD":[...]}
    입력 예: {"MTD":{"BTC":0.01,...}, ...}
    """
    if not isinstance(data, dict):
        return {"asof": None, "MTD": [], "QTD": [], "YTD": []}

    def to_pairs(x):
        if not isinstance(x, dict):
            return []
        pairs = []
        for sym, v in x.items():
            try:
                val = float(v)
                pairs.append((str(sym).upper(), val))
            except Exception:
                pass
        pairs.sort(key=lambda p: -p[1])
        return pairs[:10]

    return {
        "asof": data.get("asof"),
        "MTD": to_pairs(data.get("MTD", {})),
        "QTD": to_pairs(data.get("QTD", {})),
        "YTD": to_pairs(data.get("YTD", {})),
    }

def pct_from_any(x):
    """0.032(3.2%) / 3.2(3.2%) → '+3.20%' 문자열"""
    try:
        v = float(x)
        if abs(v) < 1: v *= 100.0
        return f"{v:+.2f}%"
    except Exception:
        return "-"

def extract_kimchi_funding(latest):
    """
    latest.json에서 김치/펀딩 추출(키 유연)
    허용:
      kimchi_premium_pct (단일 수치) 또는 kimchi:{premium_pct|pct|value}
      funding / funding_rates: {"BTC":{"binance":x,"bybit":y}, "ETH":...}
    """
    if not isinstance(latest, dict):
        return {"kimchi": None, "funding_rows": []}

    # 김치
    kim = None
    for k in ["kimchi_premium_pct", "kimchiPremiumPct", "kimchi_premium"]:
        if k in latest:
            kim = {"pct_str": pct_from_any(latest[k]), "asof": latest.get("asof") or latest.get("timestamp")}
            break
    if kim is None and isinstance(latest.get("kimchi"), dict):
        kd = latest["kimchi"]
        v = kd.get("premium_pct") or kd.get("pct") or kd.get("value")
        if v is not None:
            kim = {"pct_str": pct_from_any(v), "asof": kd.get("asof") or latest.get("asof")}

    # 펀딩
    funding_raw = latest.get("funding") or latest.get("funding_rates") or {}
    rows = []
    if isinstance(funding_raw, dict):
        for sym, mp in funding_raw.items():
            if not isinstance(mp, dict): 
                continue
            rows.append({
                "sym": str(sym).upper(),
                "binance": pct_from_any(mp.get("binance")),
                "bybit":   pct_from_any(mp.get("bybit")),
            })

    return {"kimchi": kim, "funding_rows": rows}

def esc(s):
    return (str(s) if s is not None else "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def exists(path: Path) -> bool:
    try: return path.is_file()
    except Exception: return False

# ----------------- 메인 -----------------
def main():
    # 1) 데이터 로드
    perf_up_raw   = load_json(API / "perf_up.json", {})
    perf_down_raw = load_json(API / "perf_down.json", {})
    contrib_raw   = load_json(API / "contrib_top.json", {})

    # latest.json 은 다중 후보에서 탐색 (api/가 기본이지만 안전장치)
    out_candidates = sorted(glob.glob("out/20*/latest.json")) + sorted(glob.glob("out/20*/bm20_latest.json"))
    latest_raw = load_first([
        str(API / "latest.json"),
        "latest.json",
        "site/latest.json",
        *out_candidates[-3:],
    ])

    date_up, up_list     = normalize_perf(perf_up_raw, "up")
    date_down, down_list = normalize_perf(perf_down_raw, "down")
    contrib              = normalize_contrib(contrib_raw)
    kf                   = extract_kimchi_funding(latest_raw)

    asof = pick_date(date_up, date_down, contrib.get("asof"))

    # 2) 스타일/템플릿
    style = """
    <style>
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, "Apple SD Gothic Neo", "Noto Sans KR", sans-serif; margin: 24px; color:#111; }
    header { margin-bottom: 16px; }
    h1 { font-size: 24px; margin: 0 0 4px 0; }
    .date { color:#555; font-size:14px; }
    section { margin-top: 24px; }
    h2 { font-size: 18px; margin: 0 0 12px 0; border-bottom: 1px solid #eee; padding-bottom: 6px; }
    .twocol { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
    ul { margin: 8px 0 0 18px; padding: 0; }
    li { line-height: 1.6; }
    .up  { color: #1565C0; }
    .down{ color: #C62828; }
    .imgrow { display:flex; gap:10px; flex-wrap: wrap; margin-top:10px; }
    .imgrow img { max-width: 48%; border:1px solid #eee; border-radius: 8px; }
    .muted { color:#777; }
    footer { margin-top: 32px; font-size: 12px; color:#666; }
    table { border-collapse: collapse; border:1px solid #eee; }
    th, td { padding: 6px 10px; border-bottom:1px solid #eee; text-align: right; }
    th:first-child, td:first-child { text-align: left; }
    </style>
    """

    # 이미지 자동 삽입(있을 때만)
    image_candidates = [
        ROOT / "bm20_bar_latest.png",
        ROOT / "bm20_trend_latest.png",
        ROOT / "bm20_over_btc_latest.png",
        ROOT / "bm20_over_eth_latest.png",
        ROOT / "bm20_btc_eth_line_latest.png",
        ROOT / "kimchi_premium_latest.png",
    ]
    imgs = [p for p in image_candidates if exists(p)]
    imgs_html = ""
    if imgs:
        parts = [f'<img src="{p.name}" alt="{p.name}"/>' for p in imgs]
        imgs_html = f'<div class="imgrow">{"".join(parts)}</div>'

    # 렌더 유틸
    def render_perf_block(title, rows, up=True, limit=10):
        rows = rows[:limit]
        if not rows:
            return f"<section><h2>{esc(title)}</h2><p class='muted'>데이터 없음</p></section>"
        lis = []
        for i, r in enumerate(rows, 1):
            sym = esc(r.get("symbol"))
            pct = float(r.get("ret_24h_pct", 0.0))
            cls = "up" if pct >= 0 else "down"
            lis.append(f"<li class='{cls}'>{i}. {sym} <b>{pct:+,.2f}%</b></li>")
        return f"<section><h2>{esc(title)}</h2><ul>{''.join(lis)}</ul></section>"

    def render_contrib_block(title, items, limit=10):
        items = items[:limit]
        if not items:
            return f"<section><h2>{esc(title)}</h2><p class='muted'>데이터 없음</p></section>"
        lis = []
        for i, (sym, val) in enumerate(items, 1):
            # val이 비율(0.0123)일 수 있음
            pct = val*100 if abs(val) < 1.0 else val
            lis.append(f"<li>{i}. {esc(sym)} <b>{pct:+.2f}%</b></li>")
        return f"<section><h2>{esc(title)}</h2><ul>{''.join(lis)}</ul></section>"

    def render_kimchi_block(kim):
        if not kim:
            return "<section><h2>김치 프리미엄</h2><p class='muted'>데이터 없음</p></section>"
        asof_txt = esc(str(kim.get("asof") or "N/A"))
        return f"<section><h2>김치 프리미엄</h2><p><b>{esc(kim['pct_str'])}</b> <span class='muted'>(기준: {asof_txt})</span></p></section>"

    def render_funding_block(rows):
        if not rows:
            return "<section><h2>펀딩비(8h)</h2><p class='muted'>데이터 없음</p></section>"
        th = "<tr><th>심볼</th><th>Binance</th><th>Bybit</th></tr>"
        trs = [f"<tr><td>{esc(r['sym'])}</td><td>{esc(r['binance'])}</td><td>{esc(r['bybit'])}</td></tr>" for r in rows]
        table = f"<table><thead>{th}</thead><tbody>{''.join(trs)}</tbody></table>"
        return f"<section><h2>펀딩비(8h)</h2>{table}</section>"

    # 3) HTML 조립
    html = []
    html.append("<!doctype html><html lang='ko'><meta charset='utf-8'>")
    html.append("<title>BM20 데일리 리포트</title>")
    html.append(style)
    html.append("<body>")
    html.append("<header>")
    html.append("<h1>BM20 데일리 리포트</h1>")
    html.append(f"<div class='date'>기준일: {esc(asof)}</div>")
    html.append("</header>")

    if imgs_html:
        html.append(imgs_html)

    # 김치/펀딩
    html.append(render_kimchi_block(kf.get("kimchi")))
    html.append(render_funding_block(kf.get("funding_rows", [])))

    # 퍼포먼스
    html.append("<div class='twocol'>")
    html.append(render_perf_block("코인별 퍼포먼스 (상승 TOP 10)", up_list, up=True))
    html.append(render_perf_block("코인별 퍼포먼스 (하락 TOP 10)", down_list, up=False))
    html.append("</div>")

    # 기여도
    html.append("<div class='twocol'>")
    html.append(render_contrib_block("기여도 MTD (상위 10)", contrib.get("MTD", [])))
    html.append(render_contrib_block("기여도 QTD (상위 10)", contrib.get("QTD", [])))
    html.append(render_contrib_block("기여도 YTD (상위 10)", contrib.get("YTD", [])))
    html.append("</div>")

    html.append("<footer>자동 생성: scripts/generate_report.py</footer>")
    html.append("</body></html>")

    OUT_HTML.write_text("\n".join(html), encoding="utf-8")
    print(f"[OK] wrote {OUT_HTML}")

if __name__ == "__main__":
    main()
