#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
backfill_market_history.py
──────────────────────────
market_history.csv 과거 데이터 백필 (1회 실행용)

소스별 백필 범위:
  bm20_level / bm20_chg_pct  → bm20_series.json                    (2018-01-01 ~)
  sentiment_value/label       → alternative.me FNG API              (2018-02-01 ~)
  kimchi_pct                  → out/history/kimchi_snapshots.json   (최근 90일, 일별 평균)
  usdkrw / k_share_percent    → data/bm20_history.json              (최근 41일~)

실행:
  python scripts/backfill_market_history.py
"""

import json
import requests
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

# ── 경로 ──────────────────────────────────────────────────────
ROOT        = Path(__file__).resolve().parent.parent
HIST_DIR    = ROOT / "out" / "history"
SERIES_JSON = ROOT / "bm20_series.json"
KIMCHI_JSON = HIST_DIR / "kimchi_snapshots.json"
BM20_HIST   = ROOT / "data" / "bm20_history.json"
OUT_CSV     = HIST_DIR / "market_history.csv"


# ── 1) BM20 지수 (bm20_series.json) ───────────────────────────
def load_bm20_series() -> pd.DataFrame:
    if not SERIES_JSON.exists():
        print(f"[ERROR] {SERIES_JSON} 없음")
        return pd.DataFrame()

    with open(SERIES_JSON, encoding="utf-8") as f:
        data = json.load(f)

    # 배열 또는 {"series": [...]} 두 형식 모두 처리
    arr = data if isinstance(data, list) else (data.get("series") or [])

    df = pd.DataFrame(arr)
    df = df.rename(columns={"level": "bm20_level"})
    df["date"] = df["date"].astype(str)
    df["bm20_level"] = pd.to_numeric(df["bm20_level"], errors="coerce").round(4)
    df = df.sort_values("date").reset_index(drop=True)

    # 전일 대비 등락률
    df["bm20_chg_pct"] = (df["bm20_level"].pct_change() * 100).round(4)

    print(f"[OK] BM20 지수: {len(df)}일 ({df['date'].iloc[0]} ~ {df['date'].iloc[-1]})")
    return df[["date", "bm20_level", "bm20_chg_pct"]]


# ── 2) FNG 히스토리 (alternative.me 전체 백필) ────────────────
def load_fng() -> pd.DataFrame:
    print("[INFO] FNG 히스토리 fetch 중... (2018년부터 전체)")
    try:
        r = requests.get(
            "https://api.alternative.me/fng/?limit=0",
            timeout=30,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        r.raise_for_status()
        raw = r.json()["data"]

        rows = []
        for d in raw:
            date = datetime.fromtimestamp(
                int(d["timestamp"]), tz=timezone.utc
            ).strftime("%Y-%m-%d")
            rows.append({
                "date":            date,
                "sentiment_value": int(d["value"]),
                "sentiment_label": d["value_classification"],
            })

        df = pd.DataFrame(rows).drop_duplicates(subset="date", keep="last")
        df = df.sort_values("date").reset_index(drop=True)
        print(f"[OK] FNG: {len(df)}일 ({df['date'].iloc[0]} ~ {df['date'].iloc[-1]})")
        return df

    except Exception as e:
        print(f"[WARN] FNG fetch 실패: {e} — sentiment 컬럼 없이 진행")
        return pd.DataFrame(columns=["date", "sentiment_value", "sentiment_label"])


# ── 3) 김치 프리미엄 (kimchi_snapshots.json → 일별 평균) ───────
def load_kimchi() -> pd.DataFrame:
    if not KIMCHI_JSON.exists():
        print(f"[WARN] {KIMCHI_JSON} 없음")
        return pd.DataFrame(columns=["date", "kimchi_pct"])

    with open(KIMCHI_JSON, encoding="utf-8") as f:
        data = json.load(f)

    rows = []
    for snap in data:
        ts  = snap.get("timestamp_kst", "")
        kp  = snap.get("kimchi_premium_pct", {})
        btc = kp.get("BTC")
        if not ts or btc is None:
            continue
        rows.append({"date": ts[:10], "kimchi_pct": float(btc)})

    if not rows:
        return pd.DataFrame(columns=["date", "kimchi_pct"])

    df = pd.DataFrame(rows)
    df = df.groupby("date")["kimchi_pct"].mean().round(4).reset_index()
    df = df.sort_values("date").reset_index(drop=True)
    print(f"[OK] 김치 프리미엄: {len(df)}일 ({df['date'].iloc[0]} ~ {df['date'].iloc[-1]})")
    return df


# ── 4) bm20_history.json (usdkrw, k_share) ────────────────────
def load_bm20_history() -> pd.DataFrame:
    if not BM20_HIST.exists():
        print(f"[WARN] {BM20_HIST} 없음")
        return pd.DataFrame(columns=["date", "usdkrw", "k_share_percent"])

    with open(BM20_HIST, encoding="utf-8") as f:
        data = json.load(f)

    seen = {}
    for d in data:
        date = str(d.get("timestamp", ""))[:10]
        if date:
            seen[date] = d

    rows = []
    for date, d in seen.items():
        rows.append({
            "date":            date,
            "usdkrw":          d.get("usdkrw") or d.get("exchange_rate"),
            "k_share_percent": (d.get("k_market") or {}).get("k_share_percent"),
        })

    df = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
    print(f"[OK] bm20_history: {len(df)}일 ({df['date'].iloc[0]} ~ {df['date'].iloc[-1]})")
    return df


# ── 메인 ──────────────────────────────────────────────────────
def main():
    print("=" * 55)
    print("market_history.csv 백필 시작")
    print("=" * 55)

    df_bm20   = load_bm20_series()
    df_fng    = load_fng()
    df_kimchi = load_kimchi()
    df_hist   = load_bm20_history()

    if df_bm20.empty:
        print("[ERROR] BM20 지수 데이터 없음 — 중단")
        return

    # 순차 left join (bm20_series 기준)
    df = df_bm20.copy()
    df = df.merge(df_fng,    on="date", how="left")
    df = df.merge(df_kimchi, on="date", how="left")
    df = df.merge(df_hist,   on="date", how="left")

    # 오늘부터 쌓일 컬럼 미리 추가
    for col in ["btc_funding_bin", "eth_funding_bin",
                "btc_funding_byb", "eth_funding_byb", "btc_dominance"]:
        if col not in df.columns:
            df[col] = None

    # 컬럼 순서 고정
    COLUMNS = [
        "date", "bm20_level", "bm20_chg_pct",
        "sentiment_value", "sentiment_label",
        "kimchi_pct", "usdkrw", "k_share_percent",
        "btc_funding_bin", "eth_funding_bin",
        "btc_funding_byb", "eth_funding_byb",
        "btc_dominance",
    ]
    df = df[COLUMNS].sort_values("date").reset_index(drop=True)

    # 기존 파일 있으면 병합 (daily append 데이터 보호)
    if OUT_CSV.exists():
        existing = pd.read_csv(OUT_CSV, dtype={"date": str})
        df = pd.concat([df, existing]).drop_duplicates(
            subset="date", keep="first"
        ).sort_values("date").reset_index(drop=True)
        print(f"[INFO] 기존 {OUT_CSV.name} 와 병합")

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_CSV, index=False, encoding="utf-8")

    print()
    print("=" * 55)
    print(f"[완료] {OUT_CSV}")
    print(f"  전체 행수    : {len(df)}일")
    print(f"  기간         : {df['date'].iloc[0]} ~ {df['date'].iloc[-1]}")
    print(f"  sentiment    : {df['sentiment_value'].notna().sum()}일 채워짐")
    print(f"  kimchi_pct   : {df['kimchi_pct'].notna().sum()}일 채워짐")
    print(f"  usdkrw       : {df['usdkrw'].notna().sum()}일 채워짐")
    print(f"  btc_dominance: 오늘부터 daily 실행 시 채워짐")
    print(f"  funding      : 오늘부터 daily 실행 시 채워짐")
    print("=" * 55)


if __name__ == "__main__":
    main()
