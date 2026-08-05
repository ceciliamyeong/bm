import requests
import pandas as pd
import time
from datetime import datetime, timedelta

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
}

# ── 종목별 수집 범위 (상장 며칠 전 ~ 상장 며칠 후, KST) ──────────────────────
COINS = {
    "SPK": {
        "market": "KRW-SPK",
        "start": "2026-04-20 00:00",   # 업비트 공지 4/23 11:20, 선펌핑은 4/22 새벽부터
        "end":   "2026-04-27 00:00",   # 붕괴 구간까지 확보
    },
    "IRYS": {
        "market": "KRW-IRYS",
        "start": "2026-05-11 00:00",   # 업비트 상장 5/15, 빗썸 거래량 급증은 공지 3일 전부터
        "end":   "2026-05-19 00:00",
    },
}

EXCHANGES = {
    "upbit": "https://api.upbit.com/v1/candles/minutes/60",
    "bithumb": "https://api.bithumb.com/v1/candles/minutes/60",
}


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
        if page > 30:
            print(f"  [{label}] 페이지 30개 초과 - 이상 감지, 강제 종료", flush=True)
            break
        to_str = cursor.strftime("%Y-%m-%d %H:%M:%S")
        print(f"  [{label}] page {page} 요청 - to={to_str}", flush=True)
        try:
            data = fetch_candle_page(base_url, market, to_dt=to_str, count=200, label=label)
        except requests.exceptions.HTTPError as e:
            print(f"  [{label}] HTTP 에러 - 마켓이 없거나 상장 전일 수 있음: {e}", flush=True)
            break
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
    for coin_name, cfg in COINS.items():
        print(f"\n{'='*50}\n{coin_name} 수집 시작: {cfg['start']} ~ {cfg['end']}\n{'='*50}", flush=True)
        for ex_name, base_url in EXCHANGES.items():
            label = f"{coin_name}-{ex_name}"
            print(f"\n연결 테스트 중 ({label})...", flush=True)
            try:
                test = fetch_candle_page(base_url, cfg["market"], count=1, label=label)
                print(f"연결 테스트 성공: {test}", flush=True)
            except Exception as e:
                print(f"연결 테스트 실패 ({label}): {e}", flush=True)
                continue

            df = fetch_candle_range(base_url, cfg["market"], cfg["start"], cfg["end"], label=label)
            fname = f"{coin_name.lower()}_{ex_name}_hourly.csv"
            df.to_csv(fname, index=False, encoding="utf-8-sig")
            print(f"  -> {len(df)}행 저장 ({fname})", flush=True)


if __name__ == "__main__":
    main()
