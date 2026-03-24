# ================== Market History CSV (append) ==================
# bm20_daily.py 맨 끝, update_market_indices() 호출 바로 위에 붙여넣기

MARKET_HIST_CSV = HIST_DIR / "market_history.csv"

def _get_btc_dominance_cmc() -> float | None:
    """
    BTC 도미넌스 — CoinMarketCap /global-metrics (CMC_API_KEY 사용)
    update_bm20_full.py 와 동일한 키 사용
    """
    api_key = os.getenv("CMC_API_KEY") or os.getenv("COINMARKETCAP_API_KEY")
    if not api_key:
        print("[WARN] CMC_API_KEY 없음 — btc_dominance 스킵")
        return None
    try:
        r = requests.get(
            "https://pro-api.coinmarketcap.com/v1/global-metrics/quotes/latest",
            headers={"X-CMC_PRO_API_KEY": api_key},
            timeout=10,
        )
        r.raise_for_status()
        pct = r.json()["data"]["btc_dominance"]
        return round(float(pct), 2)
    except Exception as e:
        print(f"[WARN] BTC dominance CMC failed: {e}")
        return None


def _append_market_history():
    """
    매일 실행될 때 market_history.csv에 한 줄 append.
    컬럼: date, bm20_level, bm20_chg_pct,
          ret_1d, ret_7d, ret_30d, ret_mtd, ret_ytd,
          kimchi_pct, usdkrw,
          btc_funding_bin, eth_funding_bin,
          btc_funding_byb, eth_funding_byb,
          btc_dominance
    """
    btc_dominance = _get_btc_dominance_cmc()

    row = {
        "date":            YMD,
        "bm20_level":      round(float(bm20_now), 4),
        "bm20_chg_pct":    round(float(bm20_chg), 4),
        "ret_1d":          round(float(RET_1D),  4) if RET_1D  is not None else None,
        "ret_7d":          round(float(RET_7D),  4) if RET_7D  is not None else None,
        "ret_30d":         round(float(RET_30D), 4) if RET_30D is not None else None,
        "ret_mtd":         round(float(RET_MTD), 4) if RET_MTD is not None else None,
        "ret_ytd":         round(float(RET_YTD), 4) if RET_YTD is not None else None,
        "kimchi_pct":      round(float(kimchi_pct), 4) if kimchi_pct is not None else None,
        "usdkrw":          round(float(kp_meta.get("usdkrw", 0)), 2) if kp_meta else None,
        "btc_funding_bin": round(float(btc_f_bin), 6) if btc_f_bin is not None else None,
        "eth_funding_bin": round(float(eth_f_bin), 6) if eth_f_bin is not None else None,
        "btc_funding_byb": round(float(btc_f_byb), 6) if btc_f_byb is not None else None,
        "eth_funding_byb": round(float(eth_f_byb), 6) if eth_f_byb is not None else None,
        "btc_dominance":   btc_dominance,
    }

    COLUMNS = list(row.keys())

    # CSV 읽기 → 오늘 날짜 중복 제거 → append → 저장
    if MARKET_HIST_CSV.exists():
        hist_df = pd.read_csv(MARKET_HIST_CSV, dtype={"date": str})
        hist_df = hist_df[hist_df["date"] != YMD]
    else:
        hist_df = pd.DataFrame(columns=COLUMNS)

    new_row_df = pd.DataFrame([row], columns=COLUMNS)
    hist_df = pd.concat([hist_df, new_row_df], ignore_index=True)
    hist_df.to_csv(MARKET_HIST_CSV, index=False, encoding="utf-8")

    print(f"[OK] market_history.csv → {len(hist_df)}행 (date={YMD}, btc_dom={btc_dominance}%)")


# DAILY_SNAPSHOT 여부와 무관하게 매일 기록
_append_market_history()
