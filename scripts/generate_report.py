# scripts/generate_report.py
import os, json, datetime, glob
from pathlib import Path

ROOT = Path(".")
API  = ROOT / "api"          # ✅ 기본 경로
OUT_HTML = ROOT / "latest.html"

def load_json(path, default=None):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default if default is not None else {}

def load_first(candidates):
    for p in candidates:
        try:
            with open(p, "r", encoding="utf-8") as f:
                print(f"[DBG] using latest.json at: {p}")
                return json.load(f)
        except Exception:
            pass
    print("[DBG] latest.json not found in any candidate")
    return {}

def pick_date(*candidates):
    for d in candidates:
        if isinstance(d, str) and d:
            return d
    KST = datetime.timezone(datetime.timedelta(hours=9))
    return datetime.datetime.now(tz=KST).date().isoformat()

def normalize_perf(data, kind="up"):
    if not isinstance(data, dict): return None, []
    date = data.get("date")
    keys = ["top","list"] if kind=="up" else ["bottom","list"]
    items=None
    for k in keys:
        if k in data and isinstance(data[k], list):
            items=data[k]; break
    if items is None: return date, []
    out=[]
    for it in items:
        sym = (it.get("symbol") or it.get("sym") or "").upper()
        if not sym: continue
        if "ret_24h_pct" in it:
            pct = float(it["ret_24h_pct"])
        elif "v" in it:
            v=float(it["v"]); pct = v*100 if abs(v)<1 else v
        else:
            continue
        out.append({"symbol": sym, "ret_24h_pct": round(pct, 4)})
    out.sort(key=lambda r: r["ret_24h_pct"], reverse=True if kind=="up" else False)
    return date, out

def normalize_contrib(data):
    if not isinstance(data, dict):
        return {"asof": None, "MTD": [], "QTD": [], "YTD": []}
    def to_pairs(x):
        if isinstance(x, dict):

