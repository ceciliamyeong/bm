#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
backfill_market_history.py
──────────────────────────
market_history.csv 과거 데이터 백필 (1회 실행용)

소스별 백필 범위:
  bm20_level / bm20_chg_pct  → out/history/bm20_index_history.csv   (2018~)
  sentiment_value/label       → alternative.me FNG API               (2018-02~)
  kimchi_pct                  → out/history/kimchi_snapshots.json    (최근 90일, 일별 평균)
  usdkrw / k_share_percent    → data/bm20_history.json               (최근 41일~)

실행:
  python scripts/backfill_market_history.py
"""

import json
import time
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ── 경로 ──────────────────────────────────────────────────────
ROOT         = Path(__file__).resolve().parent.parent
HIST_DIR     = ROOT / "out" / "history"
INDEX_CSV    = HIST_DIR / "bm20_index_history.csv"
KIMCHI_JSON  = HIST_DIR / "kimchi_snapshots.json"
BM20_HIST    = ROOT / "data" / "bm20_history.json"
OUT_CSV      = HIST_DIR / "market_history.csv"


# ── 1) BM20 지수 히스토리 ──────────────────────────────────────
def load_bm20_index() -> pd.DataFrame:
    if not INDEX_CSV.exists():
        print(f"[WARN] {INDEX_CSV} 없음")
        return pd.DataFrame(columns=["date", "bm20_level", "bm20_chg_pct"])

    df = pd.read_csv(INDEX_CSV, dtype={"date": str})
    df = df.rename(columns={"index": "bm20_level"})
    df["bm20_level"] = pd.to_numeric(df["bm20_level"], errors="coerce")
    df = df.sort_values("date").reset_index(drop=True)

    # 전일 대비 등락률 계산
    df["bm20_chg_pct"] = df["bm20_level"].pct_change() * 100
    df["bm20_chg_pct"] = df["bm20_chg_pct"].round(4)
    df["bm20_level"]   = df["bm20_level"].round(4)

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

        df = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
        # 날짜 중복 제거 (마지막 값 유지)
        df = df.drop_duplicates(subset="date", keep="last")
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
    # 같은 날짜 여러 스냅샷 → 일별 평균
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

    # 날짜별 마지막 항목만
    seen = {}
    for d in data:
        date = str(d.get("timestamp", ""))[:10]
        if date:
            seen[date] = d

    rows = []
    for date, d in seen.items():
        rows.append({
            "date":             date,
            "usdkrw":           d.get("usdkrw") or d.get("exchange_rate"),
            "k_share_percent":  (d.get("k_market") or {}).get("k_share_percent"),
        })

    df = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
    print(f"[OK] bm20_history: {len(df)}일 ({df['date'].iloc[0]} ~ {df['date'].iloc[-1]})")
    return df


# ── 메인 ──────────────────────────────────────────────────────
def main():
    print("=" * 50)
    print("market_history.csv 백필 시작")
    print("=" * 50)

    # 소스 로드
    df_bm20    = load_bm20_index()
    df_fng     = load_fng()
    df_kimchi  = load_kimchi()
    df_hist    = load_bm20_history()

    if df_bm20.empty:
        print("[ERROR] BM20 지수 데이터 없음 — 중단")
        return

    # 순차 merge (left join 기준: bm20_index)
    df = df_bm20.copy()
    df = df.merge(df_fng,    on="date", how="left")
    df = df.merge(df_kimchi, on="date", how="left")
    df = df.merge(df_hist,   on="date", how="left")

    # 빈 컬럼 추가 (오늘부터 쌓일 것들)
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
    df = df[COLUMNS]
    df = df.sort_values("date").reset_index(drop=True)

    # 기존 파일 있으면 merge (앞으로 daily append된 데이터 보호)
    if OUT_CSV.exists():
        existing = pd.read_csv(OUT_CSV, dtype={"date": str})
        # 백필 데이터 우선, 기존에만 있는 날짜(더 최신) 보존
        df = pd.concat([df, existing]).drop_duplicates(
            subset="date", keep="first"
        ).sort_values("date").reset_index(drop=True)
        print(f"[INFO] 기존 {OUT_CSV.name} 와 병합")

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_CSV, index=False, encoding="utf-8")

    # 결과 요약
    filled = df["sentiment_value"].notna().sum()
    kimchi_filled = df["kimchi_pct"].notna().sum()
    usdkrw_filled = df["usdkrw"].notna().sum()

    print()
    print("=" * 50)
    print(f"[완료] market_history.csv 저장: {OUT_CSV}")
    print(f"  전체 행수    : {len(df)}일")
    print(f"  기간         : {df['date'].iloc[0]} ~ {df['date'].iloc[-1]}")
    print(f"  sentiment    : {filled}일 채워짐")
    print(f"  kimchi_pct   : {kimchi_filled}일 채워짐")
    print(f"  usdkrw       : {usdkrw_filled}일 채워짐")
    print(f"  btc_dominance: 오늘부터 daily 실행 시 채워짐")
    print(f"  funding      : 오늘부터 daily 실행 시 채워짐")
    print("=" * 50)


if __name__ == "__main__":
    main()
