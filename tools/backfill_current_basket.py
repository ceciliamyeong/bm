# backfill_current_basket.py
# BM20 백필: "가용 종목만 재정규화" + "수익률 아웃라이어 가드" + "시작=base-value"
# 사용 예:
#   python backfill_current_basket.py --archive ./archive --map bm20_map_btc30.csv \
#     --start 2018-01-01 --out out/backfill_current_basket.csv \
#     --btc-cap 0.30 --eth-cap 0.20 --listed-bonus 1.3 --ret-max 3.0 --ret-min -0.95

import os, glob, argparse
import numpy as np
import pandas as pd

# 제외 규칙
STABLE = {"USDT","USDC","DAI","FDUSD","TUSD","USDE","USDP","USDL","USDS"}
DERIV  = {"WBTC","WETH","WBETH","WEETH","STETH","WSTETH","RETH","CBETH","RENBTC","HBTC","TBTC"}
EXCHANGE_TOKENS = {"LEO","WBT"}   # 교환소 토큰(예: LEO, WhiteBIT)

# ---------- 입출력 유틸 ----------
def read_latest_archive(archive_dir: str) -> pd.DataFrame:
    """archive/**/bm20_daily_data_YYYY-MM-DD.csv 중 가장 최근 파일을 읽어 구성/시가총/가중치 후보 확보"""
    files = sorted(glob.glob(os.path.join(archive_dir, "*", "bm20_daily_data_*.csv")))
    if not files:
        raise FileNotFoundError(f"No daily CSV found under {archive_dir}")
    df = pd.read_csv(files[-1])
    if "symbol" not in df.columns:
        raise ValueError("archive CSV must contain a 'symbol' column")
    df["symbol"] = df["symbol"].astype(str).str.upper()
    return df

def read_map(path: str) -> pd.DataFrame:
    """옵셔널 맵 파일(symbol,yf_ticker,listed_kr_override,include,cap_override)"""
    if not os.path.exists(path):
        return pd.DataFrame(columns=["symbol","yf_ticker","listed_kr_override","include","cap_override"])
    m = pd.read_csv(path)
    for k in ["symbol","yf_ticker","listed_kr_override","include","cap_override"]:
        if k not in m.columns:
            m[k] = np.nan
    m["symbol"] = m["symbol"].astype(str).str.upper()
    return m

# ---------- 가중치 구성 ----------
def _to_bool(s):
    return str(s).strip().lower() in {"1","true","t","y","yes"}

def build_base_weights(df: pd.DataFrame, mapping: pd.DataFrame,
                       btc_cap=0.30, eth_cap=0.20, listed_bonus=1.3) -> pd.Series:
    """BM 룰로 기본가중치 생성(스테이블/파생/익스체인지 제외 + BTC/ETH 캡 + 상장보너스)"""

    # 초기 가중치 원천: weight_ratio > market_cap > equal
    if "weight_ratio" in df.columns and pd.to_numeric(df["weight_ratio"], errors="coerce").notna().any():
        w = pd.to_numeric(df["weight_ratio"], errors="coerce").fillna(0.0)
    elif "market_cap" in df.columns and pd.to_numeric(df["market_cap"], errors="coerce").notna().any():
        w = pd.to_numeric(df["market_cap"], errors="coerce").fillna(0.0)
    else:
        w = pd.Series(1.0, index=df.index)

    w = pd.Series(w.values, index=df["symbol"]).astype(float)

    # include 오버라이드
    if "include" in mapping.columns:
        inc = mapping.set_index("symbol")["include"].map(_to_bool)
        w = w[w.index.map(lambda s: inc.get(s, True))]

    # 스테이블/파생/익스체인지 토큰 제외
    w = w[~w.index.isin(STABLE | DERIV | EXCHANGE_TOKENS)]

    # 국내상장 보너스(오버라이드만)
    if "listed_kr_override" in mapping.columns:
        listed = mapping.set_index("symbol")["listed_kr_override"].map(_to_bool)
        bonus = pd.Series(1.0, index=w.index)
        bonus.loc[bonus.index.map(lambda s: listed.get(s, False))] = float(listed_bonus)
        w = w * bonus

    # 개별 상한
    caps = {"BTC": float(btc_cap), "ETH": float(eth_cap)}
    if "cap_override" in mapping.columns:
        for sym, v in mapping[["symbol","cap_override"]].dropna().values:
            try:
                caps[str(sym).upper()] = float(v)
            except Exception:
                pass

    # 정규화 후 상한 적용 + 초과분 비례 재분배(반복)
    w = w / w.sum() if w.sum() > 0 else w
    for _ in range(16):
        over = {s: float(w[s] - caps[s]) for s in caps if s in w.index and w[s] > caps[s]}
        if not over:
            break
        excess = sum(over.values())
        for s in over:
            w[s] = caps[s]
        others = [s for s in w.index if s not in caps or w[s] <= caps.get(s, 1.0) + 1e-15]
        pool = float(w.loc[others].sum())
        if pool <= 0:
            break
        w.loc[others] += (w.loc[others] / pool) * excess
        w = w / w.sum()

    # 0 제거 후 내림차순
    w = w[w > 0].sort_values(ascending=False)
    return w

# ---------- 시세 수집 ----------
def download_prices(tickers: dict, start: str) -> pd.DataFrame:
    """yfinance에서 종가를 받아 DataFrame(날짜 x 심볼)로 리턴"""
    import yfinance as yf
    data = yf.download(list(tickers.values()), start=start, interval="1d",
                       auto_adjust=True, progress=False, group_by="ticker")
    closes = {}
    for sym, tkr in tickers.items():
        try:
            s = data[tkr]["Close"].dropna().sort_index()
            s.index = pd.to_datetime(s.index).tz_localize(None)
            if not s.empty:
                closes[sym] = s
        except Exception:
            continue
    if not closes:
        raise RuntimeError("No prices downloaded from yfinance")
    return pd.DataFrame(closes)

# ---------- 메인 ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--archive", default="archive")
    ap.add_argument("--map", default="bm20_map_btc30.csv")
    ap.add_argument("--start", default="2018-01-01")
    ap.add_argument("--out",   default="out/backfill_current_basket.csv")
    ap.add_argument("--base-value", type=float, default=100.0)
    ap.add_argument("--btc-cap", type=float, default=0.30)
    ap.add_argument("--eth-cap", type=float, default=0.20)
    ap.add_argument("--listed-bonus", type=float, default=1.3)
    # 아웃라이어 가드(일간 수익률): ret < ret_min 또는 ret > ret_max 는 NaN 처리
    ap.add_argument("--ret-max", type=float, default=3.0, help="per-asset daily return upper bound (e.g., 3.0 = +300%)")
    ap.add_argument("--ret-min", type=float, default=-0.95, help="per-asset daily return lower bound (e.g., -0.95 = -95%)")
    args = ap.parse_args()

    # 1) 현재 바스켓(최신 아카이브 + 룰)에서 기준 가중치 계산
    latest = read_latest_archive(args.archive)
    mapping = read_map(args.map)
    w0 = build_base_weights(latest, mapping,
                            btc_cap=args.btc_cap, eth_cap=args.eth_cap,
                            listed_bonus=args.listed_bonus)

    # 2) yfinance 티커 매핑
    m = mapping.set_index("symbol")["yf_ticker"].to_dict() if "yf_ticker" in mapping.columns else {}
    tickers = {s: (m.get(s) or f"{s}-USD") for s in w0.index}

    # 3) 시세 다운로드 → per-asset 일간 수익률 → 아웃라이어 가드
    prices = download_prices(tickers, args.start).sort_index()
    rets = prices.pct_change()

    if args.ret_max is not None:
        rets = rets.where(rets <= float(args.ret_max))
    if args.ret_min is not None:
        rets = rets.where(rets >= float(args.ret_min))

    # 4) "그날 가격이 있는 종목만"으로 가중치 재정규화해서 합성 수익률 계산
    #    (상장 전/결측/아웃라이어로 NaN된 종목은 그날 가중치 0)
    wvec = w0.reindex(rets.columns).fillna(0.0).astype(float).values
    rmat = rets.to_numpy(dtype="float64")
    mask = ~np.isnan(rmat)

    port = np.zeros(rmat.shape[0], dtype="float64")
    for t in range(rmat.shape[0]):
        avail = mask[t]
        sw = wvec[avail].sum()
        if sw > 0:
            port[t] = np.nansum(rmat[t, avail] * (wvec[avail] / sw))
        else:
            port[t] = 0.0  # 유효 종목 전무 → 지수 정지(전일 유지)

    # 5) 지수 레벨 누적(시작 = base-value)
    level = float(args.base_value)
    idx = np.zeros_like(port)
    for i, r in enumerate(port):
        if i == 0:
            idx[i] = level
        else:
            level *= (1.0 + float(r))
            idx[i] = level

    out = pd.DataFrame({
        "date": pd.to_datetime(prices.index).date,
        "index": idx,
        "ret": port
    })

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    out.to_csv(args.out, index=False, encoding="utf-8")

    # 요약 로그(짧게)
    flagged = (~mask & ~np.isnan(rets.to_numpy())).sum()
    print(f"[OK] backfill (avail-renorm, outlier-guard ret∈[{args.ret_min},{args.ret_max}]) "
          f"→ {args.out} rows={len(out)} symbols={len(w0)} flagged_returns={flagged}")

if __name__ == "__main__":
    main()




# ===== CSV 생성 이후에 추가 =====
import os, json, datetime as dt, tempfile
from pathlib import Path
import pandas as pd

def _write_atomic(path: Path, data: dict | list):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    # tmp에 쓰고 rename (원자적 치환)
    with tempfile.NamedTemporaryFile('w', delete=False, dir=str(path.parent), encoding='utf-8') as tmp:
        json.dump(data, tmp, ensure_ascii=False, indent=2)
        temp_name = tmp.name
    os.replace(temp_name, path)  # atomic on POSIX

def _pct(a: float, b: float):
    try:
        return a / b - 1.0
    except Exception:
        return None

def _nearest_past_value(df: pd.DataFrame, target_date: dt.date) -> float | None:
    # df: columns ['date','index'] with 'date' as string YYYY-MM-DD
    # target보다 과거(<=target) 중 가장 가까운 값
    d = pd.to_datetime(df['date']).dt.date
    mask = d <= target_date
    if not mask.any():
        return None
    i = d[mask].argmax()  # 마지막 True의 위치
    return float(df.loc[mask].iloc[-1]['index'])

def export_jsons_from_csv(csv_path: str,
                          series_out: str = "bm20_series.json",
                          latest_out: str = "bm20_latest.json"):
    csv_path = Path(csv_path)
    if not csv_path.exists():
        print(f"[WARN] CSV not found: {csv_path}")
        return

    df = pd.read_csv(csv_path, dtype={'date': str})
    # 컬럼명 표준화: index/level/value 중 하나 사용
    if 'index' not in df.columns:
        for alt in ('level', 'value', 'close'):
            if alt in df.columns:
                df = df.rename(columns={alt: 'index'})
                break
    if 'index' not in df.columns:
        raise ValueError("CSV must contain 'index' (or 'level'/'value').")

    df = df[['date', 'index']].dropna().copy()
    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
    df = df.sort_values('date')

    # --- 1) bm20_series.json ---
    series = [{'date': d, 'level': float(v)} for d, v in df[['date', 'index']].itertuples(index=False, name=None)]
    _write_atomic(Path(series_out), series)

    # --- 2) bm20_latest.json ---
    last_date = dt.datetime.strptime(df.iloc[-1]['date'], "%Y-%m-%d").date()
    last_val  = float(df.iloc[-1]['index'])
    prev_val  = float(df.iloc[-2]['index']) if len(df) >= 2 else last_val

    # helper: 과거 기준 수익률
    def ret_back(days: int):
        base = _nearest_past_value(df, last_date - dt.timedelta(days=days))
        return None if base is None else _pct(last_val, base)

    r_1d  = _pct(last_val, prev_val) if len(df) >= 2 else None
    r_7d  = ret_back(7)
    r_30d = ret_back(30)
    r_1y  = ret_back(365)

    # YTD: 올해 1월 1일(또는 그 이전 가장 가까운 값)
    y0 = dt.date(last_date.year, 1, 1)
    y0_base = _nearest_past_value(df, y0)
    r_ytd = None if y0_base is None else _pct(last_val, y0_base)

    latest = {
        "asOf": last_date.strftime("%Y-%m-%d"),
        "bm20Level": round(last_val, 6),
        "bm20PrevLevel": round(prev_val, 6),
        "bm20PointChange": round(last_val - prev_val, 6),
        "bm20ChangePct": r_1d,  # 1D
        "returns": {
            "1D":  r_1d,
            "7D":  r_7d,
            "30D": r_30d,
            "1Y":  r_1y,
            "YTD": r_ytd
        }
        # 필요하면 나중에 kimchi/funding/best3/worst3도 여기에 추가
    }
    _write_atomic(Path(latest_out), latest)

    print(f"✅ wrote {series_out} ({len(series)} pts), {latest_out} (asOf {latest['asOf']})")

# === main() 끝난 직후 호출 예시 ===
if __name__ == "__main__":
    """
    기존 main()이 백필 CSV를 out/backfill_current_basket.csv 로 저장한다고 가정합니다.
    만약 다른 경로/파일명이라면 아래 경로만 바꿔주세요.
    """
    csv_result = Path("out/backfill_current_basket.csv")
    if csv_result.exists():
        export_jsons_from_csv(str(csv_result),
                              series_out="bm20_series.json",
                              latest_out="bm20_latest.json")
    else:
        print("[WARN] backfill CSV not found; skip JSON export.")
