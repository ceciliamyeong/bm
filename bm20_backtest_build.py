#!/usr/bin/env python3
"""
bm20_backtest_build.py
======================
분기별 유니버스 교체 + 일별 체인링킹으로
out/backfill_current_basket.csv 를 2018-01-01 부터 재생성합니다.

★ 기존 bm20_daily.py 및 bm20_series.json 은 절대 건드리지 않습니다.
★ 결과: out/backfill_current_basket.csv (기존 파일을 새것으로 교체)
        out/bm20_backtest_series.json (검증용 백업)

실행:
    python bm20_backtest_build.py                      # 2018-01-01 ~ 오늘
    python bm20_backtest_build.py --dry-run            # 저장 없이 결과만 출력
    python bm20_backtest_build.py --start 2020-01-01  # 시작일 지정
"""

import argparse, csv, json, os, time, sys
from datetime import datetime, timedelta, date
from pathlib import Path

import pandas as pd
import numpy as np
import yfinance as yf

# ══════════════════════════════════════════════════════════════
# 1. 분기별 유니버스 + 가중치
# ══════════════════════════════════════════════════════════════

T1 = {
    "bitcoin":     0.30,
    "ethereum":    0.20,
    "ripple":      0.05,
    "tether":      0.05,
    "binancecoin": 0.05,
}
EQ = round(0.35 / 15, 10)   # ≈ 0.023333…

def make_weights(t3: list) -> dict:
    assert len(t3) == 15, f"T3 must be 15 coins, got {len(t3)}"
    w = dict(T1)
    for c in t3:
        w[c] = EQ
    diff = 1.0 - sum(w.values())
    if abs(diff) > 1e-12:
        w[t3[-1]] = round(w[t3[-1]] + diff, 12)
    return w

Q = {
    # ── 2018 ──
    "2018-Q1": make_weights([
        "bitcoin-cash","litecoin","nem","dash","monero",
        "ethereum-classic","neo","iota","tron","cardano",
        "stellar","qtum","bitcoin-gold","lisk","icon"]),
    "2018-Q2": make_weights([
        "bitcoin-cash","litecoin","eos","cardano","stellar",
        "iota","neo","monero","dash","tron",
        "vechain","qtum","omisego","ontology","zcash"]),
    "2018-Q3": make_weights([
        "bitcoin-cash","eos","litecoin","stellar","cardano",
        "iota","tron","neo","monero","dash",
        "vechain","ethereum-classic","nem","ontology","zcash"]),
    "2018-Q4": make_weights([
        "bitcoin-cash","eos","stellar","litecoin","cardano",
        "iota","tron","monero","dash","ethereum-classic",
        "neo","nem","vechain","tezos","zcash"]),
    # ── 2019 ──
    "2019-Q1": make_weights([
        "bitcoin-cash","litecoin","eos","stellar","tron",
        "cardano","iota","monero","dash","ethereum-classic",
        "neo","maker","zcash","ontology","vechain"]),
    "2019-Q2": make_weights([
        "litecoin","bitcoin-cash","eos","tron","cardano",
        "stellar","iota","monero","neo","dash",
        "ethereum-classic","nem","maker","zcash","vechain"]),
    "2019-Q3": make_weights([
        "litecoin","bitcoin-cash","eos","tron","cardano",
        "stellar","monero","dash","neo","cosmos",
        "chainlink","iota","ethereum-classic","maker","zcash"]),
    "2019-Q4": make_weights([
        "bitcoin-cash","litecoin","eos","stellar","cardano",
        "tron","monero","chainlink","iota","huobi-token",
        "dash","tezos","ethereum-classic","zcash","nem"]),
    # ── 2020 ──
    "2020-Q1": make_weights([
        "bitcoin-cash","litecoin","eos","stellar","tron",
        "cardano","tezos","cosmos","monero","huobi-token",
        "neo","chainlink","vechain","usd-coin","ethereum-classic"]),
    "2020-Q2": make_weights([
        "bitcoin-cash","litecoin","eos","tron","cardano",
        "stellar","monero","chainlink","vechain","tezos",
        "crypto-com-chain","ethereum-classic","neo","usd-coin","cosmos"]),
    "2020-Q3": make_weights([
        "bitcoin-cash","litecoin","polkadot","chainlink","cardano",
        "stellar","crypto-com-chain","eos","tron","tezos",
        "monero","neo","ethereum-classic","vechain","usd-coin"]),
    "2020-Q4": make_weights([
        "bitcoin-cash","polkadot","chainlink","cardano","stellar",
        "eos","litecoin","crypto-com-chain","tron","monero",
        "tezos","neo","ethereum-classic","vechain","usd-coin"]),
    # ── 2021 ──
    "2021-Q1": make_weights([
        "polkadot","cardano","litecoin","chainlink","bitcoin-cash",
        "usd-coin","stellar","uniswap","theta-token","dogecoin",
        "avalanche-2","solana","vechain","tron","ethereum-classic"]),
    "2021-Q2": make_weights([
        "polkadot","cardano","litecoin","chainlink","bitcoin-cash",
        "tron","stellar","uniswap","theta-token","filecoin",
        "bittorrent","usd-coin","dogecoin","solana","klay-token"]),
    "2021-Q3": make_weights([
        "cardano","dogecoin","polkadot","usd-coin","uniswap",
        "bitcoin-cash","litecoin","solana","chainlink","matic-network",
        "algorand","shiba-inu","ethereum-classic","theta-token","internet-computer"]),
    "2021-Q4": make_weights([
        "cardano","solana","polkadot","dogecoin","usd-coin",
        "avalanche-2","near","shiba-inu","matic-network","chainlink",
        "litecoin","bitcoin-cash","algorand","uniswap","tron"]),
    # ── 2022 ──
    "2022-Q1": make_weights([
        "solana","cardano","avalanche-2","polkadot","near",
        "dogecoin","shiba-inu","matic-network","crypto-com-chain","fantom",
        "litecoin","tron","chainlink","uniswap","algorand"]),
    "2022-Q2": make_weights([
        "solana","cardano","avalanche-2","polkadot","near",
        "dogecoin","shiba-inu","matic-network","crypto-com-chain","fantom",
        "tron","litecoin","chainlink","algorand","uniswap"]),
    "2022-Q3": make_weights([
        "usd-coin","cardano","solana","dogecoin","dai",
        "polkadot","shiba-inu","litecoin","tron","avalanche-2",
        "matic-network","uniswap","chainlink","algorand","bitcoin-cash"]),
    "2022-Q4": make_weights([
        "usd-coin","cardano","dogecoin","solana","matic-network",
        "dai","litecoin","polkadot","shiba-inu","tron",
        "avalanche-2","uniswap","chainlink","bitcoin-cash","stellar"]),
    # ── 2023 ──
    "2023-Q1": make_weights([
        "usd-coin","cardano","dogecoin","matic-network","solana",
        "polkadot","shiba-inu","tron","litecoin","avalanche-2",
        "dai","chainlink","toncoin","bitcoin-cash","uniswap"]),
    "2023-Q2": make_weights([
        "usd-coin","cardano","dogecoin","solana","matic-network",
        "polkadot","litecoin","tron","shiba-inu","toncoin",
        "bitcoin-cash","avalanche-2","dai","chainlink","uniswap"]),
    "2023-Q3": make_weights([
        "usd-coin","solana","cardano","dogecoin","tron",
        "toncoin","dai","matic-network","polkadot","litecoin",
        "bitcoin-cash","chainlink","shiba-inu","avalanche-2","uniswap"]),
    "2023-Q4": make_weights([
        "usd-coin","solana","cardano","dogecoin","tron",
        "toncoin","dai","matic-network","polkadot","litecoin",
        "bitcoin-cash","chainlink","shiba-inu","avalanche-2","internet-computer"]),
    # ── 2024 ──
    "2024-Q1": make_weights([
        "usd-coin","cardano","dogecoin","avalanche-2","tron",
        "solana","shiba-inu","polkadot","toncoin","chainlink",
        "bitcoin-cash","matic-network","internet-computer","litecoin","near"]),
    "2024-Q2": make_weights([
        "solana","usd-coin","tron","dogecoin","cardano",
        "avalanche-2","toncoin","shiba-inu","polkadot","chainlink",
        "bitcoin-cash","near","matic-network","internet-computer","litecoin"]),
    "2024-Q3": make_weights([
        "solana","usd-coin","tron","dogecoin","cardano",
        "toncoin","avalanche-2","shiba-inu","polkadot","chainlink",
        "bitcoin-cash","near","matic-network","litecoin","fetch-ai"]),
    "2024-Q4": make_weights([
        "solana","usd-coin","dogecoin","cardano","tron",
        "avalanche-2","sui","chainlink","toncoin","shiba-inu",
        "bitcoin-cash","polkadot","hedera-hashgraph","litecoin","uniswap"]),
    # ── 2025 ──
    "2025-Q1": make_weights([
        "solana","usd-coin","dogecoin","tron","cardano",
        "avalanche-2","sui","chainlink","toncoin","shiba-inu",
        "stellar","bitcoin-cash","hedera-hashgraph","polkadot","litecoin"]),
    "2025-Q2": make_weights([
        "solana","usd-coin","tron","dogecoin","cardano",
        "avalanche-2","sui","chainlink","toncoin","shiba-inu",
        "stellar","hedera-hashgraph","polkadot","bitcoin-cash","uniswap"]),
    "2025-Q3": make_weights([
        "solana","usd-coin","tron","dogecoin","cardano",
        "avalanche-2","sui","chainlink","toncoin","shiba-inu",
        "stellar","hedera-hashgraph","polkadot","bitcoin-cash","uniswap"]),
    "2025-Q4": make_weights([
        "solana","usd-coin","dogecoin","tron","cardano",
        "avalanche-2","sui","chainlink","toncoin","shiba-inu",
        "stellar","hedera-hashgraph","litecoin","bitcoin-cash","uniswap"]),
}

# ══════════════════════════════════════════════════════════════
# 2. 야후파이낸스 심볼 매핑
# ══════════════════════════════════════════════════════════════

# 종목별 야후파이낸스 유효 기간 제한
# (상장폐지, 리디노미네이션, 심볼 교체 등으로 데이터 오염되는 종목)
COIN_VALID_UNTIL = {
    "terra-luna":   "2022-05-11",  # LUNA 붕괴일
    "bittorrent":   "2021-12-31",  # BTT 리디노미네이션
    "klay-token":   "2023-12-31",  # KLAY 상폐 이슈
    "huobi-token":  "2023-06-30",  # HT 상폐
    "theta-token":  "2023-12-31",  # THETA 거래량 급감
    "matic-network":"2024-09-30",  # POL 리브랜딩 이후 MATIC-USD 데이터 오염 가능
}

YF = {
    "bitcoin":"BTC-USD","ethereum":"ETH-USD","ripple":"XRP-USD",
    "tether":"USDT-USD","binancecoin":"BNB-USD","usd-coin":"USDC-USD",
    "solana":"SOL-USD","toncoin":"TON11419-USD","avalanche-2":"AVAX-USD",
    "chainlink":"LINK-USD","cardano":"ADA-USD","shiba-inu":"SHIB-USD",
    "polkadot":"DOT-USD","dogecoin":"DOGE-USD","tron":"TRX-USD",
    "near":"NEAR-USD","cosmos":"ATOM-USD","litecoin":"LTC-USD",
    "bitcoin-cash":"BCH-USD","stellar":"XLM-USD","uniswap":"UNI7083-USD",
    "matic-network":"MATIC-USD","internet-computer":"ICP-USD",
    "hedera-hashgraph":"HBAR-USD","hyperliquid":"HYPE32196-USD","polygon":"POL28321-USD","sui":"SUI20947-USD","dai":"DAI-USD",
    "maker":"MKR-USD","eos":"EOS-USD","fantom":"FTM-USD",  # terra-luna 제거 (LUNA 붕괴 후 심볼 오염)
    "neo":"NEO-USD","nem":"XEM-USD","iota":"MIOTA-USD","dash":"DASH-USD",
    "monero":"XMR-USD","tezos":"XTZ-USD","vechain":"VET-USD",
    "ethereum-classic":"ETC-USD","zcash":"ZEC-USD","ontology":"ONT-USD",
    "qtum":"QTUM-USD","lisk":"LSK-USD","icon":"ICX-USD",
    "bitcoin-gold":"BTG-USD","omisego":"OMG-USD","theta-token":"THETA-USD",
    "filecoin":"FIL-USD","bittorrent":"BTT-USD","klay-token":"KLAY-USD",
    "algorand":"ALGO-USD","crypto-com-chain":"CRO-USD",
    "huobi-token":"HT-USD","fetch-ai":"FET-USD",
}

def quarter_key(d: date) -> str:
    return f"{d.year}-Q{(d.month-1)//3+1}"

def weights_for(d: date) -> dict:
    k = quarter_key(d)
    if k in Q:
        return Q[k]
    # 이전 분기 중 가장 최근
    for prev in sorted(Q.keys(), reverse=True):
        if prev <= k:
            return Q[prev]
    return next(iter(Q.values()))

# ══════════════════════════════════════════════════════════════
# 3. 가격 다운로드
# ══════════════════════════════════════════════════════════════

def download_prices(start: str, end: str) -> pd.DataFrame:
    all_coins = set()
    for w in Q.values():
        all_coins.update(w.keys())

    tickers = sorted({YF[c] for c in all_coins if c in YF})
    print(f"[INFO] {len(tickers)}개 티커 다운로드 ({start} ~ {end})")

    frames = []
    chunk = 40
    for i in range(0, len(tickers), chunk):
        batch = tickers[i:i+chunk]
        print(f"  배치 {i//chunk+1}/{-(-len(tickers)//chunk)}: {len(batch)}개 티커")
        for attempt in range(4):
            try:
                raw = yf.download(
                    tickers=batch, start=start, end=end,
                    interval="1d", auto_adjust=True,
                    progress=False, group_by="ticker"
                )
                if raw is None or raw.empty:
                    time.sleep(5); continue
                if isinstance(raw.columns, pd.MultiIndex):
                    lvl1 = set(raw.columns.get_level_values(1))
                    col = "Close" if "Close" in lvl1 else "Adj Close"
                    close = raw.xs(col, axis=1, level=1)
                else:
                    close = raw[["Close"]] if "Close" in raw.columns else raw[["Adj Close"]]
                frames.append(close)
                break
            except Exception as e:
                print(f"  [WARN] attempt {attempt+1}: {e}")
                time.sleep(5 * (attempt+1))

    if not frames:
        raise RuntimeError("가격 다운로드 실패")

    prices = pd.concat(frames, axis=1)
    prices = prices.loc[~prices.index.duplicated(keep="last")]

    # ── 이상치 필터: 전일 대비 +1000% 초과 or -99% 이하 수익률은 NaN 처리 ──
    # LUNA 붕괴 후 LUNA2 교체, 상장폐지 후 재상장 등으로 인한 가격 폭발 방지
    MAX_DAILY_GAIN = 2.0    # 최대 +200% (3배) — 크립토 일일 최대 현실적 범위
    MIN_DAILY_RET  = -0.95  # 최대 -95%

    pct_chg = prices.pct_change()
    spike_mask = (pct_chg > MAX_DAILY_GAIN) | (pct_chg < MIN_DAILY_RET)
    if spike_mask.any().any():
        n_spikes = spike_mask.sum().sum()
        print(f"[WARN] 이상치 {n_spikes}건 감지 → 스파이크 이후 구간 전체 NaN 처리")
        for col in spike_mask.columns:
            spike_dates = spike_mask.index[spike_mask[col]]
            for sd in spike_dates:
                # 스파이크 당일부터 끝까지 NaN (완전히 다른 코인으로 교체된 경우 방지)
                prices.loc[sd:, col] = np.nan
                print(f"  {col} spike on {sd.date()}: {pct_chg.loc[sd, col]*100:+.0f}% → {sd.date()} 이후 전체 NaN")

    # ffill은 하되 bfill은 하지 않음 (미래 가격으로 과거 채우기 방지)
    prices = prices.ffill()
    print(f"[INFO] 다운로드 완료: {prices.shape[0]}일 × {prices.shape[1]}종목")
    return prices

# ══════════════════════════════════════════════════════════════
# 4. 백테스트 실행
# ══════════════════════════════════════════════════════════════

def run(start_date: str, end_date: str, dry_run: bool = False):
    out_dir = Path("backtest_output")
    out_dir.mkdir(exist_ok=True)

    prices = download_prices(start_date, end_date)

    start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_dt   = datetime.strptime(end_date,   "%Y-%m-%d").date()

    trading_days = sorted(
        d.date() for d in prices.index
        if start_dt <= d.date() <= end_dt
    )
    if not trading_days:
        print("[ERROR] 거래일 없음"); sys.exit(1)

    first_day = trading_days[0]
    cur_weights = weights_for(first_day)
    cur_quarter = quarter_key(first_day)

    # 초기 포트폴리오 가치 계산 (Base = 100)
    first_row = prices.loc[prices.index.date == first_day].iloc[0]
    base_val = sum(
        w * float(first_row.get(YF.get(c, ""), np.nan) or np.nan)
        for c, w in cur_weights.items()
        if YF.get(c) and np.isfinite(float(first_row.get(YF.get(c,""), np.nan) or np.nan))
    )
    if base_val <= 0:
        print("[ERROR] 기준 포트폴리오 가치 계산 실패"); sys.exit(1)

    print(f"[INFO] Base: {first_day} | portfolio_value={base_val:.4f} | index=100.0")

    results = []          # [{"date":..., "level":..., "ret":...}]
    prev_level  = 100.0
    prev_prices: dict = {}

    for i, today in enumerate(trading_days):
        # 분기 전환 감지 → 리밸런싱
        tq = quarter_key(today)
        if tq != cur_quarter:
            new_w = weights_for(today)
            changed = set(new_w) - set(cur_weights), set(cur_weights) - set(new_w)
            if any(changed):
                print(f"  [{today}] 리밸런싱 IN:{sorted(changed[0])} OUT:{sorted(changed[1])}")
            cur_weights = new_w
            cur_quarter = tq

        today_row = prices.loc[prices.index.date == today]
        if today_row.empty:
            results.append({"date": str(today), "level": prev_level, "ret": 0.0})
            continue
        today_px = today_row.iloc[0]

        if i == 0:
            results.append({"date": str(today), "level": 100.0, "ret": 0.0})
            prev_level = 100.0
            prev_prices = {
                c: float(today_px.get(YF.get(c,""), np.nan) or np.nan)
                for c in cur_weights
            }
            continue

        # 1D 수익률 (종목별 -99% ~ +500% 범위 초과 시 제외)
        port_ret = 0.0
        w_used   = 0.0
        for c, w in cur_weights.items():
            tkr = YF.get(c)
            if not tkr: continue
            p1 = float(today_px.get(tkr, np.nan) or np.nan)
            p0 = prev_prices.get(c, np.nan)
            if not (np.isfinite(p1) and np.isfinite(p0) and p0 > 0 and p1 > 0):
                continue
            # 종목 유효 기간 체크
            valid_until = COIN_VALID_UNTIL.get(c)
            if valid_until and str(today) > valid_until:
                continue  # 유효 기간 초과 종목 조용히 제외

            coin_ret = p1 / p0 - 1.0
            if coin_ret > 2.0 or coin_ret < -0.95:
                print(f"  [SKIP] {c} {today} ret={coin_ret*100:+.0f}% → 이상치 제외")
                continue
            port_ret += w * coin_ret
            w_used   += w

        if w_used < 0.5:
            today_level = prev_level
            daily_ret   = 0.0
        else:
            daily_ret   = port_ret / w_used
            today_level = prev_level * (1.0 + daily_ret)

        results.append({"date": str(today), "level": round(today_level, 6), "ret": round(daily_ret, 8)})
        prev_level = today_level
        prev_prices = {c: float(today_px.get(YF.get(c,""), np.nan) or np.nan) for c in cur_weights}

        if (i+1) % 200 == 0:
            print(f"  [{today}] level={today_level:.2f}  ({i+1}/{len(trading_days)})")

    # ── 연도별 요약 ──
    print("\n연도별 수익률:")
    by_year: dict = {}
    for r in results:
        y = r["date"][:4]
        by_year.setdefault(y, []).append(r["level"])

    prev_last = 100.0
    for y in sorted(by_year):
        yl = by_year[y][-1]
        ret = (yl / prev_last - 1) * 100
        print(f"  {y}: {yl:,.2f}  ({ret:+.1f}%)")
        prev_last = yl

    last = results[-1]
    print(f"\n최종 BM20: {last['level']:,.2f}  ({last['date']})")

    if dry_run:
        print("\n[DRY-RUN] 파일 저장 건너뜀.")
        return results

    # ── 저장 1: out/bm20_backtest_series.json (검증용) ──
    bt_json = out_dir / "bm20_backtest_series.json"
    series_out = [{"date": r["date"], "level": r["level"]} for r in results]
    bt_json.write_text(json.dumps(series_out, ensure_ascii=False), encoding="utf-8")
    print(f"[SAVED] {bt_json}")

    # ── 저장 2: out/backfill_current_basket.csv (검증용 — 운영파일 아님) ──
    backfill = out_dir / "bm20_backtest_backfill.csv"
    with open(backfill, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date", "index", "ret"])
        for r in results:
            w.writerow([r["date"], r["level"], r["ret"]])
    print(f"[SAVED] {backfill}  ({len(results)}행)")
    print("\n[DONE] 백테스트 완료. 결과 검증 후 수동으로 out/backfill_current_basket.csv에 복사하세요.")

    return results


# ══════════════════════════════════════════════════════════════
# 5. 진입점
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="BM20 백테스트 빌드")
    p.add_argument("--start",   default="2018-01-01")
    p.add_argument("--end",     default=datetime.today().strftime("%Y-%m-%d"))
    p.add_argument("--dry-run", action="store_true", help="파일 저장 없이 결과만 출력")
    args = p.parse_args()

    print("=" * 60)
    print("BM20 백테스트 빌드 — 분기별 유니버스 + 일별 체인링킹")
    print(f"기간: {args.start} ~ {args.end}")
    print(f"dry-run: {args.dry_run}")
    print("=" * 60)

    run(args.start, args.end, dry_run=args.dry_run)
