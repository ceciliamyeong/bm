#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
append_korea_daily.py
─────────────────────
krw_24h_snapshots.json + kimchi_snapshots.json 에서
오늘 날짜 데이터를 추출해 korea_daily.csv 에 1줄 append

krw_rolling24h_8h.yml 마지막 단계에서 실행
(krw_rolling24h_8h.py → update_fx_8h.py → smart_kimchi_8h.py → 이 스크립트)
"""

import json
import pandas as pd
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ── 경로 ──────────────────────────────────────────────────────
ROOT        = Path(__file__).resolve().parent.parent
HIST_DIR    = ROOT / "out" / "history"
KRW_JSON    = HIST_DIR / "krw_24h_snapshots.json"
KIMCHI_JSON = HIST_DIR / "kimchi_snapshots.json"
OUT_CSV     = HIST_DIR / "korea_daily.csv"

KST = timezone(timedelta(hours=9))

COLUMNS = [
    "date",
    "krw_total", "upbit", "bithumb", "coinone",
    "stable_dom_pct", "usdt_vol", "usdc_vol",
    "top10_share_pct",
    "kimchi_btc", "kimchi_eth", "kimchi_xrp",
    "kimchi_driver", "kimchi_type",
    "usdkrw",
]


def safe_read_json(path: Path):
    if not path.exists():
        return []
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except Exception:
        return []


def main():
    today = datetime.now(KST).strftime("%Y-%m-%d")
    print(f"[INFO] korea_daily.csv append: {today}")

    # ── KRW: 오늘 마지막 스냅샷 ──────────────────────────────
    krw_snaps = safe_read_json(KRW_JSON)
    today_krw = [s for s in krw_snaps if s.get("timestamp_kst", "")[:10] == today]

    if today_krw:
        s = today_krw[-1]  # 가장 최신 스냅샷
        totals   = s.get("totals", {})
        stables  = s.get("stablecoins", {})
        by_asset = stables.get("by_asset", {})
        top10    = s.get("top10", {})

        krw_row = {
            "krw_total":       round(totals.get("combined_24h", 0)),
            "upbit":           round(totals.get("upbit_24h", 0)),
            "bithumb":         round(totals.get("bithumb_24h", 0)),
            "coinone":         round(totals.get("coinone_24h", 0)),
            "stable_dom_pct":  round(stables.get("stable_dominance_pct", 0), 4),
            "usdt_vol":        round(by_asset.get("USDT", 0)),
            "usdc_vol":        round(by_asset.get("USDC", 0)),
            "top10_share_pct": round(top10.get("top10_share_pct", 0), 4),
        }
        print(f"[OK] KRW: {krw_row['krw_total']:,.0f} KRW, stable={krw_row['stable_dom_pct']}%")
    else:
        print(f"[WARN] 오늘 KRW 스냅샷 없음")
        krw_row = {k: None for k in [
            "krw_total", "upbit", "bithumb", "coinone",
            "stable_dom_pct", "usdt_vol", "usdc_vol", "top10_share_pct"
        ]}

    # ── 김치: 오늘 스냅샷 평균 ───────────────────────────────
    kimchi_snaps = safe_read_json(KIMCHI_JSON)
    today_kimchi = [s for s in kimchi_snaps if s.get("timestamp_kst", "")[:10] == today]

    if today_kimchi:
        btc_vals = [s["kimchi_premium_pct"]["BTC"] for s in today_kimchi]
        eth_vals = [s["kimchi_premium_pct"]["ETH"] for s in today_kimchi]
        xrp_vals = [s["kimchi_premium_pct"]["XRP"] for s in today_kimchi]

        last = today_kimchi[-1]
        driver_dict = last.get("driver_share_pct", {})
        driver = max(driver_dict, key=driver_dict.get) if driver_dict else None
        kimchi_type = (last.get("smart_kimchi") or {}).get("type")
        usdkrw = (last.get("prices") or {}).get("fx", {}).get("USDKRW")

        kimchi_row = {
            "kimchi_btc":    round(sum(btc_vals) / len(btc_vals), 4),
            "kimchi_eth":    round(sum(eth_vals) / len(eth_vals), 4),
            "kimchi_xrp":    round(sum(xrp_vals) / len(xrp_vals), 4),
            "kimchi_driver": driver,
            "kimchi_type":   kimchi_type,
            "usdkrw":        round(float(usdkrw), 2) if usdkrw else None,
        }
        print(f"[OK] 김치: BTC={kimchi_row['kimchi_btc']}% ETH={kimchi_row['kimchi_eth']}% XRP={kimchi_row['kimchi_xrp']}%")
    else:
        print(f"[WARN] 오늘 김치 스냅샷 없음")
        kimchi_row = {k: None for k in [
            "kimchi_btc", "kimchi_eth", "kimchi_xrp",
            "kimchi_driver", "kimchi_type", "usdkrw"
        ]}

    # ── 병합 & 저장 ───────────────────────────────────────────
    row = {"date": today}
    row.update(krw_row)
    row.update(kimchi_row)

    if OUT_CSV.exists():
        df = pd.read_csv(OUT_CSV, dtype={"date": str})
        df = df[df["date"] != today]  # 오늘 중복 제거
    else:
        df = pd.DataFrame(columns=COLUMNS)

    new_row = pd.DataFrame([row], columns=COLUMNS)
    df = pd.concat([df, new_row], ignore_index=True)
    df.to_csv(OUT_CSV, index=False, encoding="utf-8")

    print(f"[OK] korea_daily.csv → {len(df)}행 ({today} 추가)")


if __name__ == "__main__":
    main()
