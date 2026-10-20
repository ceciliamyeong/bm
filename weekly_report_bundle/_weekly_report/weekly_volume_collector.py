#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
국내 5대 거래소(업비트·빗썸·코인원·디지털X(구 코빗)·고팍스) 주간 누적 원화 거래대금 집계
=====================================================================================

집계 기준
---------
- 기간: 한국시간(KST) 월요일 00:00 ~ 다음 월요일 00:00 (완결된 주만)
- 원천: 1시간봉(고팍스는 30분봉)을 직접 합산 -> 일봉의 09:00 경계 문제 없음
- 업비트·빗썸·코인원: 거래소가 주는 원화 거래대금 필드를 그대로 사용
- 디지털X·고팍스: 캔들에 원화 거래대금이 없어 close × volume 으로 근사
- 대상: 실행 시점에 원화마켓에 상장된 전 종목
        (해당 주 중 상장폐지된 종목은 목록에서 빠져 누락될 수 있음)

원자료 저장 (재계산용)
----------------------
- data/hourly_<거래소>_<월요일>.csv 에 종목별 시간봉 거래대금(원)을 그대로 저장합니다.
  컬럼: coin, ts_kst, ts_ms, krw   (종목 끝에는 ts_kst="#END" 표시 행이 붙음 -> 중간에 끊긴 종목 자동 정리)
- 이 폴더는 지우지 마세요. 다음 주 실행 때 전주 데이터로 그대로 재사용하고,
  스테이블코인 분류를 바꾸거나 일별·시간대별로 다시 계산할 때도 이 파일만 있으면 됩니다.
- 네트워크 없이 data 폴더만으로 다시 계산:  --from-data

실측으로 확인된 사항 (2026-10-19, 로컬 cmd)
-------------------------------------------
- 업비트: 시간봉 `to` 파라미터의 시간대 표기가 없으면 UTC 로 해석
- 빗썸  : 같은 형태지만 `to` 를 KST 로 해석  (업비트와 9시간 차이!)
- 코인원: interval=1h, 응답은 최신순, quote_volume = 원화 거래대금, timestamp = UTC ms
- 고팍스: interval=30(분), [시각, 저가, 고가, 시가, 종가, 거래량], 체결 없는 구간은 봉 자체가 없음
- 디지털X(코빗): interval 후보 "60" -> "1h" 순서로 시도 (실행 중 자동 판별)
- 2026-10-12 주 BTC 실행 결과 코인원 전주(10/5~11) 합계가 별도 계산값(약 4,666억)과 일치

사용법 (cmd)
------------
    pip install requests

    :: 1) BTC 한 종목만 5개 거래소에서 (값 검증용, 1~2분)
    python weekly_volume_collector.py --week 2026-10-12 --btc

    :: 2) 전 종목 (첫 실행은 10~20분. 중간에 멈춰도 같은 명령으로 이어서 진행)
    python weekly_volume_collector.py --week 2026-10-12

    :: 3) 나중에 재계산 (네트워크 불필요, data 폴더 사용)
    python weekly_volume_collector.py --week 2026-10-12 --from-data

    :: 일부만
    python weekly_volume_collector.py --week 2026-10-12 --only upbit:BTC coinone

    :: --week 를 생략하면 가장 최근에 끝난 주(월~일)
    :: --refresh 는 선택한 종목만 다시 받습니다 (다른 종목 데이터는 지우지 않음)

출력 (out 폴더)
---------------
    weekly_<월요일>_by_exchange.csv  거래소별 주간 거래대금·전주 대비·점유율
    weekly_<월요일>_by_coin.csv      종목별 합계 순위 (거래소별 내역 포함) -> 거래량 상위 어디까지든
    weekly_<월요일>_daily.csv        일별(KST) 거래소별 거래대금과 스테이블코인 거래대금
    weekly_<월요일>_summary.json     보고서 생성용 요약 (TOP, 스테이블코인, 실패 목록, 주석 문구)
    weekly_<월요일>_failed.csv       수집에 실패한 종목 (있을 때만)
"""

import argparse
import csv
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import requests

KST = timezone(timedelta(hours=9))
UTC = timezone.utc

OUT_DIR = "out"
DATA_DIR = "data"

EXCHANGES = ["upbit", "bithumb", "coinone", "korbit", "gopax"]
LABELS = {
    "upbit": "업비트",
    "bithumb": "빗썸",
    "coinone": "코인원",
    "korbit": "디지털X(구 코빗)",
    "gopax": "고팍스",
}
APPROX = {"korbit", "gopax"}  # close × volume 근사 거래소
SLEEP = {"upbit": 0.12, "bithumb": 0.15, "coinone": 0.2, "korbit": 0.1, "gopax": 0.1}

# 스테이블코인 분류 (원화마켓에 없는 종목이 섞여 있어도 무방). 바꾸려면 여기만 수정하고 --from-data 로 재계산.
# 2026-10-12 주 실제 상장 종목 기준으로 점검: 달러(USDT·USDC·RLUSD·PYUSD·USD1·USDE·USDS·USDG·FRAX·FDUSD),
# 유로(EURC), 엔(JPYC) 연동. 금 토큰(XAUT·PAXG)과 이름만 STABLE 인 프로젝트 토큰(STABLE)은 제외.
STABLECOINS = {"USDT", "USDC", "RLUSD", "PYUSD", "USD1", "USDE", "USDS", "USDG", "FRAX", "FDUSD",
               "DAI", "TUSD", "BUSD", "USDD", "USDP", "GUSD", "LUSD", "EURC", "JPYC"}

# (base_url, 'to' 파라미터를 해석하는 시간대) — 위 "실측으로 확인된 사항" 참고
UPBIT_LIKE = {
    "upbit": ("https://api.upbit.com", UTC),
    "bithumb": ("https://api.bithumb.com", KST),
}
NAIVE_FMT = "%Y-%m-%dT%H:%M:%S"
HOUR_MS = 3_600_000

KORBIT_INTERVALS = ["60", "1h"]
_korbit_interval = {"value": None}

SESSION = requests.Session()
SESSION.headers.update({
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (weekly_volume_collector)",
})


# ----------------------------------------------------------------------
# 공통 유틸
# ----------------------------------------------------------------------

def get_json(url, params=None, sleep=0.1, retries=5):
    """GET -> JSON. 429/5xx/네트워크 오류는 재시도, 그 외 4xx 는 즉시 예외."""
    last = None
    for attempt in range(1, retries + 1):
        try:
            r = SESSION.get(url, params=params, timeout=(5, 20))
        except requests.exceptions.RequestException as e:
            last = e
        else:
            if r.status_code == 429:
                last = RuntimeError("429 Too Many Requests")
            elif r.status_code >= 500:
                last = RuntimeError(f"HTTP {r.status_code}")
            elif r.status_code >= 400:
                raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
            else:
                time.sleep(sleep)
                return r.json()
        time.sleep(2 * attempt)
    raise RuntimeError(f"요청 {retries}회 실패: {url} {params} / {last}")


def to_ms(dt):
    return int(dt.timestamp() * 1000)


def ms_to_kst(ms):
    return datetime.fromtimestamp(ms / 1000, tz=KST)


def kst_str_to_ms(s):
    """'2026-10-12T08:00:00' (KST 표기) -> epoch ms"""
    return to_ms(datetime.strptime(s[:19], NAIVE_FMT).replace(tzinfo=KST))


def norm_coin(s):
    return s.upper().replace("KRW-", "").replace("-KRW", "").replace("_KRW", "")


def pct(cur, prev):
    if not prev:
        return None
    return (cur / prev - 1) * 100


def fmt_krw(v):
    if v is None:
        return "-"
    if abs(v) >= 1e12:
        return f"{v / 1e12:.2f}조원"
    if abs(v) < 1e8:
        return f"{v / 1e4:,.0f}만원"
    return f"{v / 1e8:,.0f}억원"


def fmt_pct(v):
    return "-" if v is None else f"{v:+.2f}%"


# ----------------------------------------------------------------------
# 1. 원화마켓 종목 목록  ->  [{"coin": "BTC", "native": <거래소 표기>}, ...]
# ----------------------------------------------------------------------

def markets_upbit_like(ex):
    base, _ = UPBIT_LIKE[ex]
    data = get_json(f"{base}/v1/market/all", {"isDetails": "false"}, SLEEP[ex])
    return [{"coin": d["market"][4:].upper(), "native": d["market"]}
            for d in data if d["market"].startswith("KRW-")]


def markets_coinone():
    items = []
    try:
        data = get_json("https://api.coinone.co.kr/public/v2/markets/KRW", sleep=SLEEP["coinone"])
        items = data.get("markets", [])
    except Exception as e:  # 엔드포인트 구조가 바뀌었을 수 있어 대체 경로 시도
        print(f"  [안내] 코인원 markets 조회 실패({e}), ticker_new 로 대체", flush=True)
    if not items:
        data = get_json("https://api.coinone.co.kr/public/v2/ticker_new/KRW",
                        {"additional_data": "false"}, SLEEP["coinone"])
        items = data.get("tickers", [])
    out, seen = [], set()
    for i in items:
        t = i.get("target_currency")
        if t and t.upper() not in seen:
            seen.add(t.upper())
            out.append({"coin": t.upper(), "native": t})
    return out


def markets_korbit():
    data = get_json("https://api.korbit.co.kr/v2/currencyPairs", sleep=SLEEP["korbit"])
    out = []
    for p in data.get("data", []):
        s = p.get("symbol", "")
        if s.endswith("_krw") and p.get("status") == "launched":
            out.append({"coin": s[:-4].upper(), "native": s})
    return out


def markets_gopax():
    data = get_json("https://api.gopax.co.kr/trading-pairs", sleep=SLEEP["gopax"])
    out = []
    for p in data:
        name = p.get("name") or f"{p.get('baseAsset')}-{p.get('quoteAsset')}"
        if name.endswith("-KRW"):
            out.append({"coin": name[:-4].upper(), "native": name})
    return out


MARKETS = {
    "upbit": lambda: markets_upbit_like("upbit"),
    "bithumb": lambda: markets_upbit_like("bithumb"),
    "coinone": markets_coinone,
    "korbit": markets_korbit,
    "gopax": markets_gopax,
}


# ----------------------------------------------------------------------
# 2. 종목별 캔들 -> {캔들 시작 epoch ms: 원화 거래대금}  (구간 [start, end) 만)
# ----------------------------------------------------------------------

def candles_upbit_like(ex, native, start, end):
    base, naive_tz = UPBIT_LIKE[ex]
    start_ms, end_ms = to_ms(start), to_ms(end)
    to_cursor = end.astimezone(naive_tz).strftime(NAIVE_FMT)  # 'to' 는 미포함(exclusive)
    vols = {}
    for _ in range(8):
        data = get_json(f"{base}/v1/candles/minutes/60",
                        {"market": native, "count": 200, "to": to_cursor}, SLEEP[ex])
        if not data:
            break
        opens = []
        for c in data:
            ms = kst_str_to_ms(c["candle_date_time_kst"])
            opens.append(ms)
            if start_ms <= ms < end_ms:
                vols[ms] = float(c["candle_acc_trade_price"])
        oldest = min(opens)
        if oldest <= start_ms or len(data) < 200:
            break
        new_cursor = ms_to_kst(oldest).astimezone(naive_tz).strftime(NAIVE_FMT)
        if new_cursor == to_cursor:
            break
        to_cursor = new_cursor
    return vols


def candles_coinone(native, start, end):
    url = f"https://api.coinone.co.kr/public/v2/chart/KRW/{native}"
    start_ms, end_ms = to_ms(start), to_ms(end)
    vols = {}
    cursor = end_ms
    for _ in range(12):
        data = get_json(url, {"interval": "1h", "timestamp": cursor}, SLEEP["coinone"])
        if data.get("result") != "success":
            raise RuntimeError(f"코인원 오류 error_code={data.get('error_code')}")
        chart = data.get("chart", [])
        if not chart:
            break
        for c in chart:
            ms = int(c["timestamp"])
            if start_ms <= ms < end_ms:
                vols[ms] = float(c.get("quote_volume") or 0)
        oldest = min(int(c["timestamp"]) for c in chart)
        if data.get("is_last") or oldest <= start_ms or oldest >= cursor:
            break
        cursor = oldest - 1  # 커서가 포함/미포함 어느 쪽이든 안전하도록 1ms 앞으로
    return vols


def candles_korbit(native, start, end):
    url = "https://api.korbit.co.kr/v2/candles"
    start_ms, end_ms = to_ms(start), to_ms(end)
    vols = {}
    candidates = ([_korbit_interval["value"]] if _korbit_interval["value"] else KORBIT_INTERVALS)
    cursor = start_ms
    for _ in range(6):
        data = None
        last_err = None
        for iv in candidates:
            try:
                res = get_json(url, {"symbol": native, "interval": iv, "start": cursor,
                                     "end": end_ms - 1, "limit": 200}, SLEEP["korbit"])
            except RuntimeError as e:
                if "HTTP 400" in str(e):
                    last_err = e
                    continue
                raise
            if not res.get("success"):
                last_err = RuntimeError(f"디지털X 응답 오류: {str(res)[:150]}")
                continue
            _korbit_interval["value"] = iv
            data = res.get("data", [])
            break
        if data is None:
            raise RuntimeError(f"디지털X interval 후보 {candidates} 모두 실패: {last_err}")
        if not data:
            break
        for c in data:
            ms = int(c["timestamp"])
            if start_ms <= ms < end_ms:
                vols[ms] = float(c.get("close") or 0) * float(c.get("volume") or 0)
        latest = max(int(c["timestamp"]) for c in data)
        if len(data) < 200 or latest + 1 <= cursor:
            break
        cursor = latest + 1
    return vols


def candles_gopax(native, start, end):
    start_ms, end_ms = to_ms(start), to_ms(end)
    vols = {}
    day = start
    while day < end:  # 호출당 반환 한도를 몰라 하루 단위로 나눠 받음
        nxt = min(day + timedelta(days=1), end)
        data = get_json(f"https://api.gopax.co.kr/trading-pairs/{native}/candles",
                        {"start": to_ms(day), "end": to_ms(nxt), "interval": 30}, SLEEP["gopax"])
        if not isinstance(data, list):
            raise RuntimeError(f"고팍스 응답 형식 이상: {str(data)[:150]}")
        for row in data:
            ms = int(row[0])
            if start_ms <= ms < end_ms:
                vols[ms] = float(row[4]) * float(row[5])  # [시각, 저, 고, 시, 종, 거래량]
        day = nxt
    return vols


FETCH = {
    "upbit": lambda native, s, e: candles_upbit_like("upbit", native, s, e),
    "bithumb": lambda native, s, e: candles_upbit_like("bithumb", native, s, e),
    "coinone": candles_coinone,
    "korbit": candles_korbit,
    "gopax": candles_gopax,
}


# ----------------------------------------------------------------------
# 3. 사전 점검 (실제 집계 전에 가정이 맞는지 확인)
# ----------------------------------------------------------------------

MIN_BTC_CANDLES = {"upbit": 160, "bithumb": 160, "coinone": 160, "korbit": 120, "gopax": 1}


def precheck(ex, start, end, all_markets):
    # 업비트/빗썸: 'to' 해석이 예상과 다르면 주 마지막 9시간이 통째로 빠지므로 중단
    if ex in UPBIT_LIKE:
        base, naive_tz = UPBIT_LIKE[ex]
        data = get_json(f"{base}/v1/candles/minutes/60",
                        {"market": "KRW-BTC", "count": 2, "to": end.astimezone(naive_tz).strftime(NAIVE_FMT)},
                        SLEEP[ex])
        newest = max(kst_str_to_ms(c["candle_date_time_kst"]) for c in data)
        if newest != to_ms(end) - HOUR_MS:
            got = ms_to_kst(newest).strftime("%Y-%m-%d %H:%M")
            raise RuntimeError(
                f"{LABELS[ex]} 'to' 해석이 예상과 달라요. 마지막 캔들이 {got} KST 로 왔어요 "
                f"(기대: 직전 시각). UPBIT_LIKE 의 시간대 설정을 확인하세요.")
    btc = next((m for m in all_markets if m["coin"] == "BTC"), None)
    if not btc:
        print(f"  [경고] {LABELS[ex]} 목록에 BTC 가 없어 사전 점검 생략", flush=True)
        return
    vols = FETCH[ex](btc["native"], start, end)
    n, total = len(vols), sum(vols.values())
    if n == 0:
        raise RuntimeError(f"{LABELS[ex]} BTC 캔들이 0개예요. 기간·커서 동작을 확인하세요.")
    flag = "" if n >= MIN_BTC_CANDLES[ex] else "  ⚠ 캔들 수가 적어요 (커서/구간 확인 필요)"
    print(f"  [점검] BTC 주간 거래대금 {fmt_krw(total)} ({total:,.0f}원), 캔들 {n}개{flag}", flush=True)


# ----------------------------------------------------------------------
# 4. 원자료 저장소 (data/hourly_<거래소>_<월요일>.csv)
#    종목마다 [시간봉 행들 + #END 행] 을 한 덩어리로 기록 -> 끝 표시가 없거나 개수가 안 맞는 종목은 불완전으로 보고 버림
# ----------------------------------------------------------------------

HEADER = ["coin", "ts_kst", "ts_ms", "krw"]


def data_path(ex, week_start):
    return os.path.join(DATA_DIR, f"hourly_{ex}_{week_start:%Y-%m-%d}.csv")


def write_coin(writer, coin, vols):
    for ms in sorted(vols):
        writer.writerow([coin, ms_to_kst(ms).strftime("%Y-%m-%d %H:%M"), ms, f"{vols[ms]:.2f}"])
    writer.writerow([coin, "#END", len(vols), 0])  # ts_ms 칸에 행 개수


def write_data(path, coins):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(HEADER)
        for coin in sorted(coins):
            write_coin(w, coin, coins[coin])
    os.replace(tmp, path)


def load_data(path):
    """-> {coin: {ts_ms: krw}}  (완결된 종목만). 불완전 종목이 있으면 파일에서 정리."""
    if not os.path.exists(path):
        return {}
    rows, ends = defaultdict(dict), {}
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r["ts_kst"] == "#END":
                ends[r["coin"]] = int(r["ts_ms"])
            else:
                rows[r["coin"]][int(r["ts_ms"])] = float(r["krw"])
    seen = set(rows) | set(ends)
    good = {c: rows.get(c, {}) for c in seen if c in ends and len(rows.get(c, {})) == ends[c]}
    if set(good) != seen:
        print(f"  [안내] {os.path.basename(path)}: 중간에 끊긴 종목 {len(seen) - len(good)}개를 정리했어요 (다시 수집됩니다)",
              flush=True)
        write_data(path, good)
    return good


def collect(ex, markets, start, end, refresh):
    path = data_path(ex, start)
    have = load_data(path)
    if refresh:  # 선택한 종목만 다시 받음
        for m in markets:
            have.pop(m["coin"], None)
        write_data(path, have)
    todo = [m for m in markets if m["coin"] not in have]
    print(f"  {start:%m/%d}~{(end - timedelta(days=1)):%m/%d} 주: {len(markets)}종목 중 "
          f"{len(markets) - len(todo)}개 저장분 사용, {len(todo)}개 수집", flush=True)
    fails = []
    if todo:
        new_file = not os.path.exists(path)
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(path, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if new_file:
                w.writerow(HEADER)
            for i, m in enumerate(todo, 1):
                try:
                    vols = FETCH[ex](m["native"], start, end)
                except Exception as e:
                    fails.append({"coin": m["coin"], "error": str(e)[:200]})
                    print(f"    [실패] {m['coin']}: {str(e)[:120]}", flush=True)
                    continue
                have[m["coin"]] = vols
                write_coin(w, m["coin"], vols)
                f.flush()
                if i % 10 == 0 or i == len(todo):
                    print(f"    {i}/{len(todo)}", flush=True)
    return {m["coin"]: have[m["coin"]] for m in markets if m["coin"] in have}, fails


# ----------------------------------------------------------------------
# 5. 집계·출력
# ----------------------------------------------------------------------

def build_and_write(week_start, week_end, exs, data, failed, has_prev, partial):
    """data[ex][wi][coin] = {ts_ms: krw}  (wi 0 = 이번 주, 1 = 전주)"""
    os.makedirs(OUT_DIR, exist_ok=True)
    tag = f"{week_start:%Y-%m-%d}"
    sums = {ex: {wi: {c: sum(v.values()) for c, v in data[ex][wi].items()} for wi in data[ex]} for ex in exs}

    cur_by_ex = {ex: sum(sums[ex][0].values()) for ex in exs}
    prev_by_ex = {ex: (sum(sums[ex][1].values()) if has_prev else None) for ex in exs}
    total_cur = sum(cur_by_ex.values())
    total_prev = sum(prev_by_ex.values()) if has_prev else None

    ex_rows = []
    for ex in exs:
        c, p = cur_by_ex[ex], prev_by_ex[ex]
        ex_rows.append({
            "key": ex, "label": LABELS[ex], "krw": c, "prev_krw": p,
            "wow_pct": pct(c, p),
            "share_pct": c / total_cur * 100 if total_cur else None,
            "prev_share_pct": (p / total_prev * 100) if (has_prev and total_prev) else None,
            "approx": ex in APPROX, "coins": len(sums[ex][0]),
            "failed": len([x for x in failed.get(ex, []) if x["week"] == 0]),
        })

    coin_cur, coin_prev = defaultdict(float), defaultdict(float)
    by_ex = defaultdict(dict)
    for ex in exs:
        for coin, v in sums[ex][0].items():
            coin_cur[coin] += v
            by_ex[coin][ex] = v
        if has_prev:
            for coin, v in sums[ex][1].items():
                coin_prev[coin] += v
    ranked = sorted(coin_cur.items(), key=lambda kv: kv[1], reverse=True)
    coin_rows = []
    for rank, (coin, v) in enumerate(ranked, 1):
        p = coin_prev.get(coin) if has_prev else None
        coin_rows.append({
            "rank": rank, "coin": coin, "krw": v, "prev_krw": p, "wow_pct": pct(v, p),
            "share_pct": v / total_cur * 100 if total_cur else None,
            "stablecoin": coin in STABLECOINS,
            "by_exchange": {ex: by_ex[coin].get(ex, 0.0) for ex in exs},
        })

    # 스테이블코인
    s_cur = sum(r["krw"] for r in coin_rows if r["stablecoin"])
    s_prev = (sum(coin_prev.get(r["coin"], 0.0) for r in coin_rows if r["stablecoin"]) if has_prev else None)
    s_by_ex = {}
    for ex in exs:
        v = sum(x for c, x in sums[ex][0].items() if c in STABLECOINS)
        s_by_ex[ex] = {"krw": v, "share_of_exchange_pct": v / cur_by_ex[ex] * 100 if cur_by_ex[ex] else None}
    stable = {
        "definition": sorted(STABLECOINS),
        "krw": s_cur, "prev_krw": s_prev, "wow_pct": pct(s_cur, s_prev),
        "share_pct": s_cur / total_cur * 100 if total_cur else None,
        "by_exchange": s_by_ex,
        "per_coin": {r["coin"]: r["krw"] for r in coin_rows if r["stablecoin"]},
    }

    # CSV: 거래소별
    with open(os.path.join(OUT_DIR, f"weekly_{tag}_by_exchange.csv"), "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["거래소", "주간거래대금_원", "전주거래대금_원", "전주대비_%", "점유율_%",
                    "스테이블코인_원", "스테이블비중_%", "근사치여부", "집계종목수", "실패종목수"])
        for r in ex_rows:
            sx = s_by_ex[r["key"]]
            w.writerow([r["label"], f"{r['krw']:.0f}", "" if r["prev_krw"] is None else f"{r['prev_krw']:.0f}",
                        "" if r["wow_pct"] is None else f"{r['wow_pct']:.2f}",
                        "" if r["share_pct"] is None else f"{r['share_pct']:.2f}",
                        f"{sx['krw']:.0f}",
                        "" if sx["share_of_exchange_pct"] is None else f"{sx['share_of_exchange_pct']:.2f}",
                        "근사" if r["approx"] else "원천", r["coins"], r["failed"]])
    # CSV: 종목별
    with open(os.path.join(OUT_DIR, f"weekly_{tag}_by_coin.csv"), "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["순위", "종목", "스테이블", "주간거래대금_원", "전주거래대금_원", "전주대비_%", "점유율_%"] +
                   [f"{LABELS[ex]}_원" for ex in exs])
        for r in coin_rows:
            w.writerow([r["rank"], r["coin"], "Y" if r["stablecoin"] else "", f"{r['krw']:.0f}",
                        "" if r["prev_krw"] is None else f"{r['prev_krw']:.0f}",
                        "" if r["wow_pct"] is None else f"{r['wow_pct']:.2f}",
                        "" if r["share_pct"] is None else f"{r['share_pct']:.2f}"] +
                       [f"{r['by_exchange'][ex]:.0f}" for ex in exs])
    # CSV: 일별 (KST)
    daily = defaultdict(lambda: defaultdict(lambda: [0.0, 0.0]))  # 날짜 -> 거래소 -> [전체, 스테이블]
    for ex in exs:
        for wi in data[ex]:
            for coin, vols in data[ex][wi].items():
                is_st = coin in STABLECOINS
                for ms, v in vols.items():
                    a = daily[ms_to_kst(ms).strftime("%Y-%m-%d")][ex]
                    a[0] += v
                    if is_st:
                        a[1] += v
    with open(os.path.join(OUT_DIR, f"weekly_{tag}_daily.csv"), "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["날짜_KST", "거래소", "거래대금_원", "스테이블코인_원", "스테이블비중_%"])
        for d in sorted(daily):
            tot_all = tot_st = 0.0
            for ex in exs:
                a = daily[d].get(ex, [0.0, 0.0])
                tot_all += a[0]
                tot_st += a[1]
                w.writerow([d, LABELS[ex], f"{a[0]:.0f}", f"{a[1]:.0f}", f"{a[1] / a[0] * 100:.2f}" if a[0] else ""])
            w.writerow([d, "합계", f"{tot_all:.0f}", f"{tot_st:.0f}", f"{tot_st / tot_all * 100:.2f}" if tot_all else ""])

    all_failed = [{"exchange": LABELS[ex], **x} for ex in exs for x in failed.get(ex, [])]
    if all_failed:
        with open(os.path.join(OUT_DIR, f"weekly_{tag}_failed.csv"), "w", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f)
            w.writerow(["거래소", "주(0=이번,1=전주)", "종목", "오류"])
            for x in all_failed:
                w.writerow([x["exchange"], x["week"], x["coin"], x["error"]])

    notes = [
        "집계 기준: 한국시간 월요일 00:00 ~ 다음 월요일 00:00, 1시간봉(고팍스 30분봉) 합산",
        "업비트·빗썸·코인원은 거래소 제공 원화 거래대금, 디지털X(구 코빗)·고팍스는 캔들 종가×거래량 근사치",
        "집계 시점의 원화마켓 상장 종목 기준이며, 주중 상장폐지 종목은 누락될 수 있음",
        "스테이블코인: USDT·USDC·RLUSD 등 법정화폐 연동 코인 기준(목록은 summary.json의 stablecoins 참고)",
    ]
    top_no_stable = [r for r in coin_rows if not r["stablecoin"]][:3]
    summary = {
        "week_start": week_start.isoformat(), "week_end": week_end.isoformat(),
        "partial": partial, "has_prev": has_prev,
        "total_krw": total_cur, "prev_total_krw": total_prev,
        "total_wow_pct": pct(total_cur, total_prev) if has_prev else None,
        "exchanges": ex_rows, "top_coins": coin_rows[:10], "top3": coin_rows[:3],
        "top3_excluding_stablecoins": top_no_stable,
        "stablecoins": stable,
        "failed": all_failed, "notes": notes,
    }
    with open(os.path.join(OUT_DIR, f"weekly_{tag}_summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    # 콘솔 요약
    print("\n" + "=" * 64)
    print(f"주간 거래대금  {week_start:%Y-%m-%d} ~ {(week_end - timedelta(days=1)):%Y-%m-%d} (KST 월~일)"
          + ("   [부분 집계]" if partial else ""))
    print("=" * 64)
    print(f"합계 {fmt_krw(total_cur)}  (전주 대비 {fmt_pct(summary['total_wow_pct'])})")
    print("\n[거래소별]")
    for r in ex_rows:
        mark = "*" if r["approx"] else " "
        print(f" {r['label']:<12}{mark} {fmt_krw(r['krw']):>12}  전주대비 {fmt_pct(r['wow_pct']):>9}  점유율 "
              f"{'-' if r['share_pct'] is None else format(r['share_pct'], '.1f') + '%'}")
    print("  (* 근사치)")
    sp = "-" if stable["share_pct"] is None else format(stable["share_pct"], ".1f") + "%"
    print(f"\n[스테이블코인] {fmt_krw(s_cur)}  전주대비 {fmt_pct(stable['wow_pct'])}  전체 대비 {sp}")
    print("\n[종목 TOP 3]")
    for r in coin_rows[:3]:
        print(f" {r['rank']}. {r['coin']:<8} {fmt_krw(r['krw']):>12}  전주대비 {fmt_pct(r['wow_pct']):>9}")
    if [r["coin"] for r in top_no_stable] != [r["coin"] for r in coin_rows[:3]]:
        print("[종목 TOP 3, 스테이블코인 제외]")
        for i, r in enumerate(top_no_stable, 1):
            print(f" {i}. {r['coin']:<8} {fmt_krw(r['krw']):>12}  전주대비 {fmt_pct(r['wow_pct']):>9}")
    if all_failed:
        print(f"\n⚠ 수집 실패 {len(all_failed)}건 -> out/weekly_{tag}_failed.csv  (같은 명령을 다시 실행하면 실패분만 재시도)")
        print("  실패 종목은 합계에서 빠져 있으니 공개용 수치로 쓰기 전에 반드시 확인하세요.")
    print(f"\n저장: {OUT_DIR}/weekly_{tag}_*.csv, weekly_{tag}_summary.json  (원자료: {DATA_DIR}/)")


# ----------------------------------------------------------------------
# main
# ----------------------------------------------------------------------

def parse_selection(tokens, btc_only):
    if btc_only:
        return {ex: {"BTC"} for ex in EXCHANGES}
    if not tokens:
        return {ex: None for ex in EXCHANGES}
    sel = {}
    for t in tokens:
        ex, _, coin = t.partition(":")
        ex = ex.lower()
        if ex not in EXCHANGES:
            sys.exit(f"알 수 없는 거래소: {ex} (가능: {', '.join(EXCHANGES)})")
        if coin:
            if ex in sel and sel[ex] is None:
                continue
            sel.setdefault(ex, set()).add(norm_coin(coin))
        else:
            sel[ex] = None
    return sel


def main():
    ap = argparse.ArgumentParser(description="국내 5대 거래소 주간 누적 원화 거래대금 집계")
    ap.add_argument("--week", help="집계할 주의 월요일 (YYYY-MM-DD, KST). 생략하면 가장 최근에 끝난 주")
    ap.add_argument("--btc", action="store_true", help="BTC 한 종목만 5개 거래소에서 (값 검증용)")
    ap.add_argument("--only", nargs="+", metavar="EX[:COIN]",
                    help="예: upbit:BTC coinone  (거래소 또는 거래소:종목)")
    ap.add_argument("--from-data", action="store_true", help="네트워크 없이 data 폴더의 저장분으로 다시 계산")
    ap.add_argument("--no-prev", action="store_true", help="전주 데이터 수집 생략")
    ap.add_argument("--refresh", action="store_true", help="선택한 종목을 저장분 무시하고 다시 수집")
    ap.add_argument("--skip-check", action="store_true", help="사전 점검 생략")
    ap.add_argument("--allow-partial", action="store_true", help="아직 안 끝난 주도 허용 (테스트용)")
    args = ap.parse_args()

    now = datetime.now(KST)
    if args.week:
        week_start = datetime.strptime(args.week, "%Y-%m-%d").replace(tzinfo=KST)
    else:
        this_monday = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
        week_start = this_monday - timedelta(days=7)
    if week_start.weekday() != 0:
        sys.exit(f"--week 는 월요일이어야 해요 ({week_start:%Y-%m-%d} 은 월요일이 아님)")
    week_end = week_start + timedelta(days=7)
    if week_end > now and not args.allow_partial:
        sys.exit(f"{week_start:%Y-%m-%d} 주는 아직 끝나지 않았어요 (마감: {week_end:%Y-%m-%d %H:%M} KST).")

    has_prev = not args.no_prev
    weeks = [(week_start, week_end)]
    if has_prev:
        weeks.append((week_start - timedelta(days=7), week_start))

    sel = parse_selection(args.only, args.btc)
    exs = [ex for ex in EXCHANGES if ex in sel]
    partial = set(exs) != set(EXCHANGES) or any(v is not None for v in sel.values())

    data = {ex: {} for ex in exs}
    failed = {ex: [] for ex in exs}

    if args.from_data:
        for ex in exs:
            for wi, (ws, we) in enumerate(weeks):
                have = load_data(data_path(ex, ws))
                coins = sel[ex]
                data[ex][wi] = {c: v for c, v in have.items() if coins is None or c in coins}
        missing = [LABELS[ex] for ex in exs if not data[ex][0]]
        if missing:
            sys.exit(f"{', '.join(missing)}: data 폴더에 {week_start:%Y-%m-%d} 주 저장분이 없어요. 먼저 수집하세요.")
        if has_prev and any(not data[ex][1] for ex in exs):
            print("[경고] 전주 저장분이 없는 거래소가 있어 전주 대비는 생략해요.", flush=True)
            has_prev = False
            for ex in exs:
                data[ex].pop(1, None)
        build_and_write(week_start, week_end, exs, data, failed, has_prev, partial)
        return

    for ex in exs:
        print(f"\n===== {LABELS[ex]} =====", flush=True)
        all_markets = MARKETS[ex]()
        if not all_markets:
            raise RuntimeError(f"{LABELS[ex]} 원화마켓 목록이 비어 있어요. 엔드포인트를 확인하세요.")
        if not args.skip_check:
            precheck(ex, week_start, week_end, all_markets)
        coins = sel[ex]
        markets = [m for m in all_markets if coins is None or m["coin"] in coins]
        if coins is not None:
            missing = coins - {m["coin"] for m in markets}
            if missing:
                print(f"  [경고] {LABELS[ex]} 에 없는 종목: {', '.join(sorted(missing))}", flush=True)
        for wi, (ws, we) in enumerate(weeks):
            done, fails = collect(ex, markets, ws, we, args.refresh)
            data[ex][wi] = done
            failed[ex] += [{"week": wi, **x} for x in fails]

    build_and_write(week_start, week_end, exs, data, failed, has_prev, partial)


if __name__ == "__main__":
    main()
