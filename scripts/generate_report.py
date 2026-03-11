#!/usr/bin/env python3
# Find latest out/YYYY-MM-DD, copy to archive/YYYY-MM-DD, and update index.html
# + Publish latest: creates latest.html and bm20_bar_latest.png / bm20_btc_eth_line.png at repo root
# - Injects news preview (reads bm20_news_YYYY-MM-DD.txt)
# - Creates .nojekyll to avoid Jekyll processing
# - Robust when out/ has no dated folder (falls back to root files)

import os, re, shutil, html, csv, json
from pathlib import Path
import matplotlib.pyplot as plt
import datetime as dt


ROOT  = Path(__file__).resolve().parents[1]
OUT   = ROOT / "out"
ARCH  = ROOT / "archive"
INDEX = ROOT / "index.html"

def is_ymd(name: str) -> bool:
    try:
        dt.datetime.strptime(name, "%Y-%m-%d")
        return True
    except Exception:
        return False

def find_latest_out_dir() -> Path | None:
    if not OUT.exists():
        return None
    dated = [p for p in OUT.iterdir() if p.is_dir() and is_ymd(p.name)]
    if dated:
        return sorted(dated, key=lambda p: p.name)[-1]
    return None

def ensure_latest_dir() -> Path:
    latest = find_latest_out_dir()
    if latest:
        return latest

    # Fallback: out 루트에 파일만 떨어져있는 경우 임시 날짜 폴더 생성
    root_files = list(OUT.glob("*"))
    if not root_files:
        raise SystemExit(f"[generate_report] no dated folder and no files under {OUT}")
    ts = max(p.stat().st_mtime for p in root_files)
    ymd = dt.datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
    tmp = OUT / ymd
    tmp.mkdir(exist_ok=True)
    for p in root_files:
        if p.is_file():
            shutil.move(str(p), tmp / p.name)
    print(f"[fallback] created {tmp} from root files")
    return tmp

def copy_dir(src: Path) -> Path:
    dst = ARCH / src.name
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(src, dst)
    print("[copy]", src, "→", dst)
    return dst

def read_news_preview(latest_dir: Path, max_chars: int = 380) -> str | None:
    """bm20_news_YYYY-MM-DD.txt 읽어서 미리보기(짧은 문단) 반환"""
    ymd = latest_dir.name
    news_file = latest_dir / f"bm20_news_{ymd}.txt"
    if not news_file.exists():
        return None
    txt = news_file.read_text(encoding="utf-8").strip()
    if not txt:
        return None
    txt = txt.replace("\r\n", "\n").replace("\r", "\n")
    para = txt.split("\n")[0] if "\n" in txt else txt
    if len(para) > max_chars:
        para = para[:max_chars].rstrip() + "…"
    return html.escape(para)

def update_index(latest_dir: Path):
    """index.html의 <!--LATEST_START--> ... <!--LATEST_END--> 블록을 오늘자 프리뷰로 교체"""
    if not INDEX.exists():
        print("[warn] index.html not found; skip update")
        return

    ymd = latest_dir.name
    files = {p.name: p for p in latest_dir.iterdir() if p.is_file()}

    links = []
    if f"bm20_daily_{ymd}.html" in files:
        links.append(f'<a href="archive/{ymd}/bm20_daily_{ymd}.html">HTML</a>')
    if f"bm20_daily_{ymd}.pdf" in files:
        links.append(f'<a href="archive/{ymd}/bm20_daily_{ymd}.pdf">PDF</a>')

    img_tag = ""
    if f"bm20_bar_{ymd}.png" in files:
        img_tag = (
            f'<img src="archive/{ymd}/bm20_bar_{ymd}.png" alt="performance" '
            f'style="max-width:100%;border:1px solid #eee;border-radius:8px;margin-top:8px;" />'
        )

    news_html = ""

    block = f"""
<div>
  <strong>Latest: {ymd}</strong> — {' | '.join(links) if links else 'no files'}
  {img_tag}
  {news_html}
</div>
""".strip()

    html_src = INDEX.read_text(encoding="utf-8")
    new_html = re.sub(
        r"(<!--LATEST_START-->)(.*?)(<!--LATEST_END-->)",
        lambda m: f"{m.group(1)}\n{block}\n{m.group(3)}",
        html_src, flags=re.S
    )
    INDEX.write_text(new_html, encoding="utf-8")
    print("[update] index.html latest block updated")

# --- publish latest (fixed assets) -----------------------------------

def publish_latest(dst_daily_dir: Path):
    """
    고정 latest 파일 생성 (대시보드 안정용)
    """
    ymd = dst_daily_dir.name

    html_src  = dst_daily_dir / f"bm20_daily_{ymd}.html"
    bar_src   = dst_daily_dir / f"bm20_bar_{ymd}.png"
    trd_src   = dst_daily_dir / f"bm20_trend_{ymd}.png"
    csv_src   = dst_daily_dir / f"bm20_daily_data_{ymd}.csv"
    news_src  = dst_daily_dir / f"bm20_news_{ymd}.txt"
    kimchi_src= dst_daily_dir / f"kimchi_{ymd}.json"

    if html_src.exists():
        html_txt = html_src.read_text(encoding="utf-8")
        html_txt = html_txt.replace(f"bm20_bar_{ymd}.png", "bm20_bar_latest.png")
        html_txt = html_txt.replace(f"bm20_trend_{ymd}.png", "bm20_trend_latest.png")
        (ROOT / "latest.html").write_text(html_txt, encoding="utf-8")

    if bar_src.exists():
        shutil.copyfile(bar_src, ROOT / "bm20_bar_latest.png")

    if trd_src.exists():
        shutil.copyfile(trd_src, ROOT / "bm20_trend_latest.png")

    if csv_src.exists():
        shutil.copyfile(csv_src, ROOT / "bm20_daily_data_latest.csv")

    # NOTE: news latest disabled (market summary box removed)

    if kimchi_src.exists():
        shutil.copyfile(kimchi_src, ROOT / "kimchi_latest.json")

    print(f"[publish_latest] latest alias files created for {ymd}")



def rebuild_json_from_backfill():
    """
    SSOT: backfill_current_basket.csv를 기준으로
      - ROOT/bm20_series.json (date, level)
      - ROOT/bm20_latest.json (asOf, bm20Level, returns 등 + kimchi)
    를 재생성해서 대시보드가 항상 '연속 지수'만 읽게 만든다.
    """

    def _extract_kimchi_ratio(x):
        """
        percent(9.5) / ratio(0.095) / '9.5%' / dict 등을 ratio(0.095)로 정규화.
        규칙:
          - dict에 kimchi_pct가 있으면: 그 값은 '이미 % 단위' → /100
          - dict에 kimchi_premium_pct가 있으면: 이름상 % → /100 (단, 값이 1 미만이어도 %로 간주)
          - dict에 kimchi(또는 premium)가 있으면: 보통 ratio로 저장 → 그대로 (단, 1 이상이면 %로 보고 /100)
          - 문자열에 %가 있으면: % → /100
          - 숫자 단독이면: 1 이상이면 %로 간주(/100), 1 미만이면 ratio로 간주(그대로)
        """
        if x is None:
            return None
    
        # dict: key semantics first
        if isinstance(x, dict):
            # 1) kimchi_pct는 "퍼센트 값"으로 확정 (0.5441 = 0.5441%)
            if "kimchi_pct" in x and x["kimchi_pct"] is not None:
                try:
                    return float(x["kimchi_pct"]) / 100.0
                except Exception:
                    return None
    
            # 2) kimchi_premium_pct도 이름상 percent로 확정
            if "kimchi_premium_pct" in x and x["kimchi_premium_pct"] is not None:
                try:
                    return float(x["kimchi_premium_pct"]) / 100.0
                except Exception:
                    return None
    
            # 3) kimchi / premium는 ratio로 들어오는 경우가 많음
            for k in ["kimchi", "kimchi_premium", "premium", "rate", "value"]:
                if k in x and x[k] is not None:
                    out = _extract_kimchi_ratio(x[k])
                    if out is not None:
                        return out
    
            # fallback scan
            for v in x.values():
                out = _extract_kimchi_ratio(v)
                if out is not None:
                    return out
            return None
    
        # number
        if isinstance(x, (int, float)):
            v = float(x)
            # 단독 숫자는 기존 규칙 유지
            return v / 100.0 if v >= 1.0 else v
    
        # string
        if isinstance(x, str):
            s = x.strip()
            has_pct = "%" in s
            s = s.replace("%", "")
            try:
                v = float(s)
            except Exception:
                return None
            if has_pct:
                return v / 100.0
            return v / 100.0 if v >= 1.0 else v
    
        return None

    def _load_kimchi_for_date(ymd: str):
        """
        kimchi json 후보들을 찾아 kimchi_ratio(0.0x)와 부가정보(usdkrw 등)를 반환
        """
        candidates = [
            OUT / "latest" / "cache" / "kimchi_last.json",
            ROOT / "bm" / "out" / "latest" / "cache" / "kimchi_last.json",

            OUT / "latest" / ymd / f"kimchi_{ymd}.json",
            ROOT / "bm" / "out" / "latest" / ymd / f"kimchi_{ymd}.json",

            OUT / ymd / f"kimchi_{ymd}.json",
            ROOT / "bm" / "out" / ymd / f"kimchi_{ymd}.json",
        ]
        for p in candidates:
            if not p.exists():
                continue
            try:
                obj = json.loads(p.read_text(encoding="utf-8"))
            except Exception:
                continue

            ratio = _extract_kimchi_ratio(obj)
            meta = {}
            if isinstance(obj, dict):
                # 혹시 1450 같은 값(usdkrw)이 있으면 같이 실어둠 (디버깅용)
                for k in ["usdkrw", "USDKRW", "krw_per_usd", "usd_krw", "fx_usdkrw", "fx"]:
                    if k in obj:
                        meta["usdkrw"] = obj.get(k)
                        break
                for k in ["premium_krw", "kimchi_krw", "premium_won", "won_premium"]:
                    if k in obj:
                        meta["premium_krw"] = obj.get(k)
                        break

            return ratio, meta, str(p)
        return None, {}, None

    # 후보 경로: out/ 와 bm/out 둘 다 탐색
    candidates = [
        OUT / "backfill_current_basket.csv",
        ROOT / "bm" / "out" / "backfill_current_basket.csv",
    ]
    csv_path = next((p for p in candidates if p.exists()), None)
    if not csv_path:
        print("[rebuild_json] skip: backfill_current_basket.csv not found")
        return

    rows = []
    with csv_path.open("r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            date = (row.get("date") or "").strip()
            idx  = row.get("index") or row.get("level") or row.get("bm20Level")
            if not date or idx is None:
                continue
            try:
                level = float(idx)
            except Exception:
                continue
            rows.append({"date": date[:10], "level": level})

    if len(rows) < 2:
        print("[rebuild_json] skip: not enough rows in backfill CSV")
        return

    rows.sort(key=lambda x: x["date"])

    # 1) series.json 생성 (대시보드 라인차트 입력)
    (ROOT / "bm20_series.json").write_text(
        json.dumps(rows, ensure_ascii=False),
        encoding="utf-8"
    )

    # 2) latest.json 생성 (KPI 입력)
    last, prev = rows[-1], rows[-2]
    bm20Level = last["level"]
    bm20Prev  = prev["level"]
    bm20ChangePct = (bm20Level / bm20Prev) - 1.0 if bm20Prev else None

    # kimchi 주입 (latest에 없을 수 있으니 별도 파일에서)
    kimchi_ratio, kimchi_meta, kimchi_src = _load_kimchi_for_date(last["date"])

    # ---- returns 계산 (backfill rows 기준) ----
    def _level_on_or_before(target_ymd: str):
        for r in reversed(rows):
            if r["date"] <= target_ymd:
                return float(r["level"])
        return None

    def _ret_from(target_ymd: str, cur_level: float):
        base = _level_on_or_before(target_ymd)
        if base is None or base == 0:
            return None
        return (cur_level / base) - 1.0   # ratio

    asof = last["date"]
    cur_level = float(last["level"])

    asof_dt = dt.datetime.strptime(asof, "%Y-%m-%d")
    ymd_7d  = (asof_dt - dt.timedelta(days=7)).strftime("%Y-%m-%d")
    ymd_30d = (asof_dt - dt.timedelta(days=30)).strftime("%Y-%m-%d")
    ymd_1y  = (asof_dt - dt.timedelta(days=365)).strftime("%Y-%m-%d")
    ymd_ytd = asof_dt.replace(month=1, day=1).strftime("%Y-%m-%d")

    returns = {
        "1D": bm20ChangePct,
        "7D": _ret_from(ymd_7d,  cur_level),
        "30D": _ret_from(ymd_30d, cur_level),
        "1Y": _ret_from(ymd_1y,  cur_level),
        "YTD": _ret_from(ymd_ytd, cur_level),
    }

    latest = {
        "asOf": last["date"],
        "bm20Level": bm20Level,
        "bm20PrevLevel": bm20Prev,
        "bm20PointChange": bm20Level - bm20Prev,
        "bm20ChangePct": bm20ChangePct,

        # ✅ 여기서 returns 전체 넣는다
        "returns": returns,

        # ✅ kimchi: JS가 읽는 키들
        "kimchi": kimchi_ratio,  # ratio (0.0x)
        "kimchi_premium_pct": (kimchi_ratio * 100.0) if isinstance(kimchi_ratio, (int, float)) else None,

        # ✅ 디버깅용
        "kimchi_source": kimchi_src,
        "kimchi_meta": kimchi_meta,
    }

    (ROOT / "bm20_latest.json").write_text(
        json.dumps(latest, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    print(f"[rebuild_json] wrote bm20_series.json({len(rows)} pts) + bm20_latest.json (+kimchi) from {csv_path}")


# ---------------------------------------------------------------------

def main():
    latest = ensure_latest_dir()
    dst = copy_dir(latest)
    update_index(dst)

    publish_latest(dst)        # 🔥 이 줄 다시 살린다
    rebuild_json_from_backfill()  # ★ 연속성 SSOT → 루트 JSON 재생성
    
    (ROOT / ".nojekyll").write_text("", encoding="utf-8")
    print("[done] site updated")

if __name__ == "__main__":
    main()
