import requests
import pandas as pd
import time
from datetime import datetime, timedelta, timezone

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
}

EXCHANGES = {
    "upbit": {
        "base_url": "https://api.upbit.com/v1/candles/minutes/60",
        "market": "KRW-PIEVERSE",
    },
    "bithumb": {
        # 빗썸 신형(v1) API - 업비트와 동일 스펙(market/to/count)
        "base_url": "https://api.bithumb.com/v1/candles/minutes/60",
        "market": "KRW-PIEVERSE",
    },
}

# ── 구간 정의 (KST 기준) ──────────────────────────────
SEGMENTS = {
    "pre_pump": ("2026-04-14 00:00", "2026-04-20 12:00"),
    "listing_spike": ("2026-04-20 00:00", "2026-04-23 00:00"),
    "post_peak_crash": ("2026-04-21 12:00", "2026-04-24 12:00"),
    "recent": (None, None),
}

now_kst = datetime.now(timezone(timedelta(hours=9)))
SEGMENTS["recent"] = (
    (now_kst - timedelta(days=7)).strftime("%Y-%m-%d %H:%M"),
    now_kst.strftime("%Y-%m-%d %H:%M"),
)

OVERALL_START = "2026-04-14 00:00"
OVERALL_END = SEGMENTS["recent"][1]


def fetch_candle_page(base_url, market, to_dt=None, count=200, label=""):
    params = {"market": market, "count": count}
    if to_dt:
        params["to"] = to_dt
    try:
        r = requests.get(base_url, params=params, headers=HEADERS, timeout=(5, 15))
    except requests.exceptions.RequestException as e:
        print(f"    [{label}] 요청 실패: {type(e).__name__}: {e}", flush=True)
        raise
    print(f"    [{label}] 응답: status={r.status_code}, {len(r.content):,} bytes", flush=True)
    r.raise_for_status()
    return r.json()


def fetch_candle_range(base_url, market, start_str, end_str, label=""):
    start = datetime.strptime(start_str, "%Y-%m-%d %H:%M")
    end = datetime.strptime(end_str, "%Y-%m-%d %H:%M")
    all_rows = []
    cursor = end
    page = 0
    while cursor > start:
        page += 1
        if page > 50:
            print(f"  [{label}] 페이지 50개 초과 - 이상 감지, 강제 종료", flush=True)
            break
        to_str = cursor.strftime("%Y-%m-%d %H:%M:%S")
        print(f"  [{label}] page {page} 요청 - to={to_str}", flush=True)
        data = fetch_candle_page(base_url, market, to_dt=to_str, count=200, label=label)
        if not data:
            print(f"  [{label}] page {page} - 빈 응답, 중단", flush=True)
            break
        all_rows.extend(data)
        oldest = datetime.strptime(data[-1]["candle_date_time_kst"], "%Y-%m-%dT%H:%M:%S")
        print(f"  [{label}] page {page} 완료 - {len(data)}건, oldest={oldest}", flush=True)
        if oldest <= start:
            break
        if oldest >= cursor:
            print(f"  [{label}] 더 이상 과거 데이터 없음 (상장/집계 시작 이전) - 수집 종료", flush=True)
            break
        cursor = oldest
        time.sleep(0.15)
    df = pd.DataFrame(all_rows)
    if df.empty:
        return df
    df["candle_date_time_kst"] = pd.to_datetime(df["candle_date_time_kst"])
    df = df[(df["candle_date_time_kst"] >= start) & (df["candle_date_time_kst"] <= end)]
    df = df.drop_duplicates(subset=["candle_date_time_kst"])
    df = df.sort_values("candle_date_time_kst").reset_index(drop=True)
    df = df.rename(columns={
        "candle_date_time_kst": "datetime_kst",
        "trade_price": "close",
        "opening_price": "open",
        "high_price": "high",
        "low_price": "low",
        "candle_acc_trade_price": "trade_value_krw",
        "candle_acc_trade_volume": "trade_volume",
    })
    return df[["datetime_kst", "open", "high", "low", "close", "trade_value_krw", "trade_volume"]]


def main():
    print(f"수집 범위: {OVERALL_START} ~ {OVERALL_END}", flush=True)

    results = {}
    for name, cfg in EXCHANGES.items():
        print(f"\n연결 테스트 중 ({name}, 최근 캔들 1개만 요청)...", flush=True)
        try:
            test = fetch_candle_page(cfg["base_url"], cfg["market"], count=1, label=name)
            print(f"연결 테스트 성공: {test}", flush=True)
        except Exception as e:
            print(f"연결 테스트 실패 ({name}) - 접근 자체가 막혀있을 수 있음: {e}", flush=True)
            results[name] = pd.DataFrame()
            continue

        print(f"{name} {cfg['market']} 전체 구간 수집 중...", flush=True)
        df = fetch_candle_range(cfg["base_url"], cfg["market"], OVERALL_START, OVERALL_END, label=name)
        results[name] = df
        df.to_csv(f"pieverse_{name}_hourly.csv", index=False, encoding="utf-8-sig")
        print(f"  -> {len(df)}행 저장 (pieverse_{name}_hourly.csv)", flush=True)

    upbit_df = results.get("upbit", pd.DataFrame())
    bithumb_df = results.get("bithumb", pd.DataFrame())

    # 구간별 요약 (업비트 기준)
    print("\n=== 구간별 요약 (업비트) ===", flush=True)
    if not upbit_df.empty:
        for name, (s, e) in SEGMENTS.items():
            s_dt, e_dt = datetime.strptime(s, "%Y-%m-%d %H:%M"), datetime.strptime(e, "%Y-%m-%d %H:%M")
            seg = upbit_df[(upbit_df["datetime_kst"] >= s_dt) & (upbit_df["datetime_kst"] <= e_dt)]
            if not seg.empty:
                chg = (seg["close"].iloc[-1] / seg["close"].iloc[0] - 1) * 100
                print(f"[{name}] {s} ~ {e}", flush=True)
                print(f"  시작가 {seg['close'].iloc[0]:.1f} -> 종가 {seg['close'].iloc[-1]:.1f} ({chg:+.1f}%)", flush=True)
                print(f"  구간 누적 거래대금: {seg['trade_value_krw'].sum():,.0f}원", flush=True)

    if not bithumb_df.empty:
        print("\n=== 구간별 요약 (빗썸) ===", flush=True)
        for name, (s, e) in SEGMENTS.items():
            s_dt, e_dt = datetime.strptime(s, "%Y-%m-%d %H:%M"), datetime.strptime(e, "%Y-%m-%d %H:%M")
            seg = bithumb_df[(bithumb_df["datetime_kst"] >= s_dt) & (bithumb_df["datetime_kst"] <= e_dt)]
            if not seg.empty:
                chg = (seg["close"].iloc[-1] / seg["close"].iloc[0] - 1) * 100
                print(f"[{name}] {s} ~ {e}", flush=True)
                print(f"  시작가 {seg['close'].iloc[0]:.1f} -> 종가 {seg['close'].iloc[-1]:.1f} ({chg:+.1f}%)", flush=True)
                print(f"  구간 누적 거래대금: {seg['trade_value_krw'].sum():,.0f}원", flush=True)

    if not upbit_df.empty and not bithumb_df.empty:
        merged = upbit_df.merge(
            bithumb_df, on="datetime_kst", how="outer", suffixes=("_upbit", "_bithumb")
        ).sort_values("datetime_kst")
        merged.to_csv("pieverse_merged_hourly.csv", index=False, encoding="utf-8-sig")
        print("\n통합 파일 저장: pieverse_merged_hourly.csv", flush=True)


if __name__ == "__main__":
    main()
