import requests
import pandas as pd
import time
from datetime import datetime, timedelta, timezone

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
}

# ── 구간 정의 (KST 기준) ──────────────────────────────
SEGMENTS = {
    "pre_pump": ("2026-04-14 00:00", "2026-04-20 12:00"),   # 상장 공지 전 선펌핑
    "listing_spike": ("2026-04-20 00:00", "2026-04-23 00:00"),  # 상장 직후 스파이크
    "post_peak_crash": ("2026-04-21 12:00", "2026-04-24 12:00"),  # 정점 이후 붕괴
    "recent": (None, None),  # 최근 7일 - 실행 시점 기준으로 채움
}

now_kst = datetime.now(timezone(timedelta(hours=9)))
SEGMENTS["recent"] = (
    (now_kst - timedelta(days=7)).strftime("%Y-%m-%d %H:%M"),
    now_kst.strftime("%Y-%m-%d %H:%M"),
)

OVERALL_START = "2026-04-14 00:00"
OVERALL_END = SEGMENTS["recent"][1]


def fetch_upbit_hourly(market="KRW-PIEVERSE", to_dt=None, count=200):
    """업비트 시간봉 캔들 조회 (최신 -> 과거, to 파라미터로 페이지네이션)"""
    url = "https://api.upbit.com/v1/candles/minutes/60"
    params = {"market": market, "count": count}
    if to_dt:
        params["to"] = to_dt
    r = requests.get(url, params=params, headers=HEADERS, timeout=10)
    r.raise_for_status()
    return r.json()


def fetch_upbit_range(market, start_str, end_str):
    start = datetime.strptime(start_str, "%Y-%m-%d %H:%M")
    end = datetime.strptime(end_str, "%Y-%m-%d %H:%M")
    all_rows = []
    cursor = end
    while cursor > start:
        to_str = cursor.strftime("%Y-%m-%d %H:%M:%S")
        data = fetch_upbit_hourly(market=market, to_dt=to_str, count=200)
        if not data:
            break
        all_rows.extend(data)
        oldest = datetime.strptime(data[-1]["candle_date_time_kst"], "%Y-%m-%dT%H:%M:%S")
        if oldest <= start:
            break
        cursor = oldest
        time.sleep(0.15)  # rate limit 여유
    df = pd.DataFrame(all_rows)
    if df.empty:
        return df
    df["candle_date_time_kst"] = pd.to_datetime(df["candle_date_time_kst"])
    df = df[(df["candle_date_time_kst"] >= start) & (df["candle_date_time_kst"] <= end)]
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


def fetch_bithumb_hourly(symbol="PIEVERSE", to_ts=None, count=200):
    """빗썸 시간봉 캔들 조회 - public candlestick API"""
    # 빗썸은 kline 형태로 특정 interval의 전체 히스토리를 반환하는 방식이라
    # 1h 데이터를 통째로 받고 이후 구간을 잘라서 씀
    url = f"https://api.bithumb.com/public/candlestick/{symbol}_KRW/1h"
    r = requests.get(url, headers=HEADERS, timeout=10)
    r.raise_for_status()
    data = r.json()
    if data.get("status") != "0000":
        raise RuntimeError(f"빗썸 API 오류: {data}")
    rows = data["data"]
    df = pd.DataFrame(rows, columns=["timestamp_ms", "open", "close", "high", "low", "volume"])
    df["timestamp_ms"] = df["timestamp_ms"].astype("int64")
    df["datetime_kst"] = pd.to_datetime(df["timestamp_ms"], unit="ms") + timedelta(hours=9)
    for col in ["open", "close", "high", "low", "volume"]:
        df[col] = df[col].astype(float)
    df["trade_value_krw"] = df["close"] * df["volume"]  # 근사치 (종가 기준)
    return df[["datetime_kst", "open", "high", "low", "close", "trade_value_krw", "volume"]].rename(
        columns={"volume": "trade_volume"}
    )


def main():
    print(f"수집 범위: {OVERALL_START} ~ {OVERALL_END}")

    print("업비트 KRW-PIEVERSE 수집 중...")
    upbit_df = fetch_upbit_range("KRW-PIEVERSE", OVERALL_START, OVERALL_END)
    upbit_df.to_csv("pieverse_upbit_hourly.csv", index=False, encoding="utf-8-sig")
    print(f"  -> {len(upbit_df)}행 저장 (pieverse_upbit_hourly.csv)")

    print("빗썸 PIEVERSE 수집 중...")
    try:
        bithumb_df = fetch_bithumb_hourly("PIEVERSE")
        start_dt = datetime.strptime(OVERALL_START, "%Y-%m-%d %H:%M")
        end_dt = datetime.strptime(OVERALL_END, "%Y-%m-%d %H:%M")
        bithumb_df = bithumb_df[
            (bithumb_df["datetime_kst"] >= start_dt) & (bithumb_df["datetime_kst"] <= end_dt)
        ].reset_index(drop=True)
        bithumb_df.to_csv("pieverse_bithumb_hourly.csv", index=False, encoding="utf-8-sig")
        print(f"  -> {len(bithumb_df)}행 저장 (pieverse_bithumb_hourly.csv)")
    except Exception as e:
        print(f"  빗썸 수집 실패: {e}")
        bithumb_df = pd.DataFrame()

    # 구간별 요약
    print("\n=== 구간별 요약 ===")
    for name, (s, e) in SEGMENTS.items():
        s_dt, e_dt = datetime.strptime(s, "%Y-%m-%d %H:%M"), datetime.strptime(e, "%Y-%m-%d %H:%M")
        seg_up = upbit_df[(upbit_df["datetime_kst"] >= s_dt) & (upbit_df["datetime_kst"] <= e_dt)]
        if not seg_up.empty:
            chg = (seg_up["close"].iloc[-1] / seg_up["close"].iloc[0] - 1) * 100
            print(f"[{name}] {s} ~ {e}")
            print(f"  업비트 시작가 {seg_up['close'].iloc[0]:.1f} -> 종가 {seg_up['close'].iloc[-1]:.1f} ({chg:+.1f}%)")
            print(f"  업비트 구간 누적 거래대금: {seg_up['trade_value_krw'].sum():,.0f}원")

    # 통합 파일
    if not bithumb_df.empty:
        merged = upbit_df.merge(
            bithumb_df, on="datetime_kst", how="outer", suffixes=("_upbit", "_bithumb")
        ).sort_values("datetime_kst")
        merged.to_csv("pieverse_merged_hourly.csv", index=False, encoding="utf-8-sig")
        print("\n통합 파일 저장: pieverse_merged_hourly.csv")


if __name__ == "__main__":
    main()
