# scripts/generate_report.py
# BM20 데일리: bm/api/*.json을 읽어 latest.html 생성 (에러 없이 관용적으로 처리)

import os, json, datetime
from pathlib import Path

ROOT = Path(".")
API  = ROOT / "api"
OUT_HTML = ROOT / "latest.html"

# ---------- 유틸 ----------
def load_json(path, default=None):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default if default is not None else {}

def pick_date(*candidates):
    for d in candidates:
        if isinstance(d, str) and d:
            return d
    # Asia/Seoul 기준 오늘 날짜 (문자열)
    KST = datetime.timezone(datetime.timedelta(hours=9))
    return datetime.datetime.now(tz=KST).date().isoformat()

def normalize_perf(data, kind="up"):
    """
    perf_up.json / perf_down.json 포맷을 통합
    허용:
      - {"date":"YYYY-MM-DD","top":[{"symbol":"BTC","ret_24h_pct":1.23},...]}
      - {"date":"YYYY-MM-DD","bottom":[...]}
      - {"list":[{"sym":"BTC","v":0.0123},...]}  # 구형
    반환: (date, [{"symbol":"BTC","ret_24h_pct":1.23}, ...])
    """
    if not isinstance(data, dict):
        return None, []
    # 날짜 추출
    date = data.get("date")
    # 아이템 추출
    keys = ["top", "list"] if kind == "up" else ["bottom", "list"]
    items = None
    for k in keys:
        if k in data and isinstance(data[k], list):
            items = data[k]
            break
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

    # 내림차순 정렬(상승은 크게→작게, 하락은 작게→크게인데 이미 분리되어 있으니 안전하게 정렬만)
    out.sort(key=lambda r: r["ret_24h_pct"], reverse=True if kind=="up" else False)
    return date, out

def normalize_contrib(data):
    """
    contrib_top.json을 섹션별 상위 리스트로 변환
    허용:
      - {"asof":"YYYY-MM-DD","MTD":{"BTC":0.01,...},"QTD":{...},"YTD":{...}}
      - 값이 list/tuple일 경우도 대응
    반환: {"asof":str, "MTD":[(sym,val),...], "QTD":[...], "YTD":[...]}
    """
    if not isinstance(data, dict):
        return {"asof": None, "MTD": [], "QTD": [], "YTD": []}
    def to_pairs(x):
        if isinstance(x, dict):
            return list(x.items())
        if isinstance(x, list):
            # ["BTC", 0.01] 또는 {"sym":...,"v":...} 대응
            pairs = []
            for it in x:
                if isinstance(it, (list, tuple)) and len(it) == 2:
                    pairs.append((str(it[0]).upper(), float(it[1])))
                elif isinstance(it, dict):
                    sym = (it.get("symbol") or it.get("sym") or "").upper()
                    v = it.get("v"); v = float(v) if v is not None else None
                    if sym and v is not None:
                        pairs.append((sym, v))
            return pairs
        return []

    def topn(x, n=10):
        pairs = to_pairs(x)
        pairs.sort(key=lambda p: -float(p[1]) if p and p[1] is not None else 0.0)
        return pairs[:n]

    return {
        "asof": data.get("asof"),
        "MTD": topn(data.get("MTD", {})),
        "QTD": topn(data.get("QTD", {})),
        "YTD": topn(data.get("YTD", {})),
    }

def fmt_pct(p):
    try:
        return f"{float(p)*100:+.2f}%" if abs(p) < 1.0 else f"{float(p):+.2f}%"
    except Exception:
        return "-"

def esc(s):
    return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def exists(path: Path) -> bool:
    try:
        return path.is_file()
    except Exception:
        return False

# ---------- 데이터 로드/정규화 ----------
def main():
    perf_up_raw   = load_json(API / "perf_up.json", {})
    perf_down_raw = load_json(API / "perf_down.json", {})
    contrib_raw   = load_json(API / "contrib_top.json", {})

    date_up, up_list     = normalize_perf(perf_up_raw, "up")
    date_down, down_list = normalize_perf(perf_down_raw, "down")
    contrib = normalize_contrib(contrib_raw)

    asof = pick_date(date_up, date_down, contrib.get("asof"))

    # ---------- HTML 구성 ----------
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
    </style>
    """

    # 이미지 후보 자동 삽입
    image_candidates = [
        ROOT / "bm20_bar_latest.png",
        ROOT / "bm20_trend_latest.png",
        ROOT / "bm20_over_btc_latest.png",
        ROOT / "bm20_over_eth_latest.png",
        ROOT / "bm20_btc_eth_line_latest.png",
        ROOT / "kimchi_premium_latest.png",
    ]
    imgs_html = ""
    imgs = [p for p in image_candidates if exists(p)]
    if imgs:
        parts = [f'<img src="{p.name}" alt="{p.name}"/>' for p in imgs]
        imgs_html = f'<div class="imgrow">{"".join(parts)}</div>'

    # 퍼포먼스 섹션
    def render_perf_block(title, rows, up=True, limit=10):
        rows = rows[:limit]
        if not rows:
            return f"<section><h2>{esc(title)}</h2><p class='muted'>데이터 없음</p></section>"
        lis = []
        for i, r in enumerate(rows, 1):
            sym = esc(r.get("symbol"))
            pct = r.get("ret_24h_pct")
            cls = "up" if (pct is not None and float(pct) >= 0) else "down"
            lis.append(f"<li class='{cls}'>{i}. {sym} <b>{float(pct):+,.2f}%</b></li>")
        return f"<section><h2>{esc(title)}</h2><ul>{''.join(lis)}</ul></section>"

    # 기여도 섹션
    def render_contrib_block(title, items, limit=10):
        items = items[:limit]
        if not items:
            return f"<section><h2>{esc(title)}</h2><p class='muted'>데이터 없음</p></section>"
        lis = []
        for i, (sym, val) in enumerate(items, 1):
            lis.append(f"<li>{i}. {esc(sym)} <b>{fmt_pct(val)}</b></li>")
        return f"<section><h2>{esc(title)}</h2><ul>{''.join(lis)}</ul></section>"

    html = []
    html.append("<!doctype html><html lang='ko'><meta charset='utf-8'>")
    html.append("<title>BM20 데일리 리포트</title>")
    html.append(style)
    html.append("<body>")
    html.append("<header>")
    html.append("<h1>BM20 데일리 리포트</h1>")
    html.append(f"<div class='date'>기준일: {esc(asof)}</div>")
    html.append("</header>")

    # 이미지 묶음(있으면)
    if imgs_html:
        html.append(imgs_html)

    # 퍼포먼스 (상/하락)
    html.append("<div class='twocol'>")
    html.append(render_perf_block("코인별 퍼포먼스 (상승 TOP 10)", up_list, up=True))
    html.append(render_perf_block("코인별 퍼포먼스 (하락 TOP 10)", down_list, up=False))
    html.append("</div>")

    # 기여도 (MTD/QTD/YTD)
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
