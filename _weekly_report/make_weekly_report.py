#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
주간 보고서(카드형 HTML) 생성기

입력
  - out/weekly_<월요일>_summary.json   (weekly_volume_collector.py 결과)
  - out/weekly_<월요일>_daily.csv      (있으면 일별 차트 추가)
  - BM20: ceciliamyeong/bm 레포의 bm20_series.json 과 구성종목 일별 csv (읽기 전용으로 내려받기만 함)

출력
  - out/weekly_<월요일>_report.html   (한 파일로 열림. 같은 폴더의 CSV 는 다운로드 링크로 연결)

사용 (cmd)
    python make_weekly_report.py --week 2026-10-12
    python make_weekly_report.py --week 2026-10-12 --bm20-dir C:\\path\\to\\bm      (로컬 레포 사본 사용)
    python make_weekly_report.py --week 2026-10-12 --no-bm20                          (BM20 카드 제외)

BM20 기준: 일별 확정 시계열(bm20_series.json)에서 해당 월요일 값 -> 다음 월요일 값 (7일 수익률).
레포는 읽기만 하고 아무것도 쓰지 않습니다.
"""
import argparse
import csv
import html
import json
import os
import sys
import urllib.request
from datetime import datetime, timedelta

RAW = "https://raw.githubusercontent.com/ceciliamyeong/bm/main/"
OUT_DIR = "out"
EX_COLORS = {"업비트": "#1d4ed8", "빗썸": "#e07a1f", "코인원": "#0f8f6b", "디지털X(구 코빗)": "#7c4dbb", "고팍스": "#8a8f98"}


SHORT = {"업비트": "업비트", "빗썸": "빗썸", "코인원": "코인원", "디지털X(구 코빗)": "디지털X", "고팍스": "고팍스"}

# 네트워크로 한글명을 못 받을 때 쓰는 기본 이름 (실행 시 업비트·빗썸 목록이 우선)
BASE_NAMES = {
    "BTC": "비트코인", "ETH": "이더리움", "XRP": "리플", "USDT": "테더", "USDC": "USD코인", "SOL": "솔라나",
    "DOGE": "도지코인", "ADA": "에이다", "TRX": "트론", "BNB": "바이낸스코인", "LINK": "체인링크",
    "AVAX": "아발란체", "XLM": "스텔라루멘", "BCH": "비트코인캐시", "LTC": "라이트코인", "NEAR": "니어프로토콜",
    "ONDO": "온도파이낸스", "WLD": "월드코인", "GTC": "깃코인", "WAL": "월러스", "SUI": "수이",
    "HYPE": "하이퍼리퀴드", "ZEC": "지캐시", "XMR": "모네로", "UNI": "유니스왑", "DOT": "폴카닷",
    "CC": "캔톤", "ENA": "에테나", "AAVE": "에이브", "SHIB": "시바이누", "PEPE": "페페", "APT": "앱토스",
    "ARB": "아비트럼", "OP": "옵티미즘", "ATOM": "코스모스", "ETC": "이더리움클래식", "RLUSD": "리플USD",
    "PYUSD": "페이팔USD", "JPYC": "JPYC", "EURC": "유로코인", "FRAX": "프랙스", "USD1": "USD1",
}


def load_names(data_dir):
    """업비트·빗썸 마켓 목록의 korean_name 으로 한글명 사전을 만들고 data/korean_names.json 에 저장."""
    path = os.path.join(data_dir, "korean_names.json")
    names = dict(BASE_NAMES)
    try:
        for base in ("https://api.bithumb.com", "https://api.upbit.com"):  # 업비트가 마지막에 덮어씀
            req = urllib.request.Request(base + "/v1/market/all?isDetails=false", headers={"User-Agent": "Mozilla/5.0"})
            for d in json.loads(urllib.request.urlopen(req, timeout=20).read().decode("utf-8")):
                if d["market"].startswith("KRW-") and d.get("korean_name"):
                    names[d["market"][4:].upper()] = d["korean_name"]
        os.makedirs(data_dir, exist_ok=True)
        json.dump(names, open(path, "w", encoding="utf-8"), ensure_ascii=False, indent=0)
    except Exception as e:
        print(f"  (한글명 내려받기 실패: {e} -> 저장본/기본 이름 사용)")
        if os.path.exists(path):
            names.update(json.load(open(path, encoding="utf-8")))
    return names


def load_coins(out_dir, tag, labels, stable_set):
    """종목별 csv -> {종목: {krw, prev, wow, ex: [상장 거래소]}}"""
    path = os.path.join(out_dir, f"weekly_{tag}_by_coin.csv")
    coins = {}
    if not os.path.exists(path):
        return coins
    for r in csv.DictReader(open(path, encoding="utf-8-sig")):
        ex = [l for l in labels if float(r.get(f"{l}_원") or 0) > 0]
        prev = float(r["전주거래대금_원"]) if r.get("전주거래대금_원") else None
        krw = float(r["주간거래대금_원"])
        coins[r["종목"]] = {"coin": r["종목"], "krw": krw, "prev_krw": prev,
                            "wow_pct": (krw / prev - 1) * 100 if prev else None,
                            "ex": ex, "stable": r["종목"] in stable_set or r.get("스테이블") == "Y"}
    return coins


# ───────────────────────── 포맷 ─────────────────────────
def esc(s):
    return html.escape(str(s))


def jo(v, d=2):
    """원 -> 조/억 표기"""
    if v is None:
        return "-"
    if abs(v) >= 1e12:
        return f"{v / 1e12:,.{d}f}조"
    return f"{v / 1e8:,.0f}억"


def pct(v, d=2, sign=True):
    if v is None:
        return "-"
    return f"{v:+.{d}f}%" if sign else f"{v:.{d}f}%"


def cls(v):
    # 한국 관행: 상승 = 빨강, 하락 = 파랑
    if v is None or abs(v) < 0.005:
        return "flat"
    return "up" if v > 0 else "down"


def arrow(v):
    if v is None or abs(v) < 0.005:
        return "–"
    return "▲" if v > 0 else "▼"


def chg(v, d=2):
    return f'<span class="chg {cls(v)}">{arrow(v)} {pct(v, d, sign=False) if v is None else f"{abs(v):.{d}f}%"}</span>'


# ───────────────────────── BM20 ─────────────────────────
def fetch(rel, bm_dir):
    if bm_dir:
        with open(os.path.join(bm_dir, rel), encoding="utf-8-sig") as f:
            return f.read()
    req = urllib.request.Request(RAW + rel, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read().decode("utf-8-sig")


def load_bm20(ws, bm_dir):
    series = {x["date"]: x["level"] for x in json.loads(fetch("bm20_series.json", bm_dir))}
    d0, d1 = ws.strftime("%Y-%m-%d"), (ws + timedelta(days=7)).strftime("%Y-%m-%d")
    dp = (ws - timedelta(days=7)).strftime("%Y-%m-%d")
    if d1 not in series or d0 not in series:
        raise RuntimeError(f"BM20 시계열에 {d0} 또는 {d1} 이 없어요 (최신 {max(series)})")
    days = [(ws + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(8)]
    out = {
        "start": series[d0], "end": series[d1],
        "ret": (series[d1] / series[d0] - 1) * 100,
        "prev_ret": (series[d0] / series[dp] - 1) * 100 if dp in series else None,
        "points": [(d, series[d]) for d in days if d in series],
        "movers": [],
    }
    try:
        def load(d):
            rows = csv.DictReader(fetch(f"out/{d}/bm20_daily_data_{d}.csv", bm_dir).splitlines())
            return {r["symbol"]: r for r in rows}
        a, b = load(d0), load(d1)
        mv = []
        for k in b:
            if k in a:
                mv.append((k, (float(b[k]["current_price"]) / float(a[k]["current_price"]) - 1) * 100,
                           float(b[k]["weight_ratio"])))
        out["movers"] = sorted(mv, key=lambda x: x[1], reverse=True)
    except Exception as e:  # 구성종목 파일이 없어도 지수 카드는 보여줌
        print(f"  (구성종목 등락 생략: {e})")
    return out


# ───────────────────────── SVG 조각 ─────────────────────────
def line_svg(points, w=960, h=190):
    ys = [p[1] for p in points]
    lo, hi = min(ys), max(ys)
    pad = (hi - lo) * 0.18 or 1
    lo, hi = lo - pad, hi + pad
    x0, x1, y0, y1 = 14, w - 14, 14, h - 26
    def X(i): return x0 + (x1 - x0) * i / (len(points) - 1)
    def Y(v): return y1 - (y1 - y0) * (v - lo) / (hi - lo)
    pts = " ".join(f"{X(i):.1f},{Y(v):.1f}" for i, (_, v) in enumerate(points))
    area = f"{X(0):.1f},{y1} " + pts + f" {X(len(points) - 1):.1f},{y1}"
    labels = "".join(
        f'<text x="{X(i):.1f}" y="{h - 8}" text-anchor="middle" class="ax">{d[5:].replace("-", "/")}</text>'
        for i, (d, _) in enumerate(points))
    ex, ey = X(len(points) - 1), Y(points[-1][1])
    sx, sy = X(0), Y(points[0][1])
    return (f'<svg viewBox="0 0 {w} {h}" role="img" aria-label="BM20 일별 지수">'
            f'<polygon points="{area}" class="area"/><polyline points="{pts}" class="ln"/>'
            f'<circle cx="{sx:.1f}" cy="{sy:.1f}" r="3.5" class="dot0"/><circle cx="{ex:.1f}" cy="{ey:.1f}" r="4.5" class="dot1"/>'
            f'<text x="{sx + 6:.1f}" y="{sy - 8:.1f}" class="vl">{points[0][1]:,.1f}</text>'
            f'<text x="{ex - 6:.1f}" y="{ey - 9:.1f}" text-anchor="end" class="vl b">{points[-1][1]:,.1f}</text>'
            f'{labels}</svg>')


def pct_share(v):
    return "0.1% 미만" if 0 < v < 0.05 else f"{v:.1f}%"


def daily_svg(daily, order, w=960, h=220):
    days = sorted(daily)
    tot = [sum(daily[d].get(e, 0) for e in order) for d in days]
    top = max(tot) * 1.12
    x0, x1, y0, y1 = 10, w - 10, 12, h - 26
    bw = (x1 - x0) / len(days)
    out = []
    for i, d in enumerate(days):
        y = y1
        for e in order:
            v = daily[d].get(e, 0)
            hh = (y1 - y0) * v / top
            y -= hh
            op = ' opacity=".38"' if i < len(days) - 7 else ""
            out.append(f'<rect x="{x0 + i * bw + bw * 0.16:.1f}" y="{y:.1f}" width="{bw * 0.68:.1f}" height="{max(hh, 0):.1f}" fill="{EX_COLORS.get(e, "#999")}"{op}/>')
        out.append(f'<text x="{x0 + i * bw + bw / 2:.1f}" y="{y - 4:.1f}" text-anchor="middle" class="vl">{tot[i] / 1e12:.2f}</text>')
        wd = "월화수목금토일"[datetime.strptime(d, "%Y-%m-%d").weekday()]
        out.append(f'<text x="{x0 + i * bw + bw / 2:.1f}" y="{h - 8}" text-anchor="middle" class="ax">{d[5:].replace("-", "/")} {wd}</text>')
    return f'<svg viewBox="0 0 {w} {h}" role="img" aria-label="일별 거래대금(조원)">{"".join(out)}</svg>'


def squarify(items, x, y, w, h):
    """items: [(key, area)] 면적 내림차순 -> {key: (x, y, w, h)}"""
    total = sum(a for _, a in items)
    items = [(k, a * w * h / total) for k, a in items]
    out = {}
    def worst(row, side):
        s = sum(a for _, a in row)
        return max(max(side * side * a / (s * s), s * s / (side * side * a)) for _, a in row)
    def lay(row, x, y, w, h):
        s = sum(a for _, a in row)
        if w >= h:  # 왼쪽에 세로 띠
            cw = s / h; cy = y
            for k, a in row:
                out[k] = (x, cy, cw, a / cw); cy += a / cw
            return x + cw, y, w - cw, h
        ch = s / w; cx = x
        for k, a in row:
            out[k] = (cx, y, a / ch, ch); cx += a / ch
        return x, y + ch, w, h - ch
    row, rest = [], list(items)
    while rest:
        side = min(w, h)
        nxt = rest[0]
        if not row or worst(row + [nxt], side) <= worst(row, side):
            row.append(nxt); rest.pop(0)
        else:
            x, y, w, h = lay(row, x, y, w, h); row = []
    if row:
        lay(row, x, y, w, h)
    return out


def tile_color(r, cap=15.0):
    """한국식: 상승 빨강, 하락 파랑. 강도는 ±cap% 에서 최대."""
    k = min(abs(r) / cap, 1.0)
    base = (232, 236, 242)
    tgt = (214, 50, 45) if r > 0 else (31, 95, 209)
    c = tuple(round(base[i] + (tgt[i] - base[i]) * (0.18 + 0.82 * k)) for i in range(3))
    return f"rgb({c[0]},{c[1]},{c[2]})", ("#ffffff" if k > 0.42 else "#141a26")


def treemap_svg(movers, w=960, h=330):
    items = sorted([(s, wt) for s, _, wt in movers], key=lambda z: -z[1])
    rets = {s: r for s, r, _ in movers}
    pos = squarify(items, 0, 0, w, h)
    out = []
    for s, (x, y, tw, th) in pos.items():
        fill, fg = tile_color(rets[s])
        out.append(f'<rect x="{x + 1:.1f}" y="{y + 1:.1f}" width="{tw - 2:.1f}" height="{th - 2:.1f}" rx="3" fill="{fill}"/>')
        cx, cy = x + tw / 2, y + th / 2
        big = tw > 150 and th > 90
        ko = NAMES.get(s, "")
        lines = ([(ko if len(ko) <= 8 else ko[:7] + "…", 13, 500)] if (tw > 92 and ko) else []) + \
                [(s, 15 if big else 12, 700), (f"{rets[s]:+.1f}%", 17 if big else 12, 600)]
        total_h = sum(sz + 4 for _, sz, _ in lines)
        yy = cy - total_h / 2
        for txt, sz, wt in lines:
            yy += sz
            out.append(f'<text x="{cx:.1f}" y="{yy:.1f}" text-anchor="middle" font-size="{sz}" font-weight="{wt}" fill="{fg}">{esc(txt)}</text>')
            yy += 4
    return f'<svg viewBox="0 0 {w} {h}" role="img" aria-label="BM20 구성종목 트리맵">{"".join(out)}</svg>'


# ───────────────────────── 카드 ─────────────────────────
def card_bm20(b):
    if not b:
        return ""
    prev = f'<span class="sub">전주 {b["prev_ret"]:+.2f}%</span>' if b["prev_ret"] is not None else ""
    tm = ""
    if b["movers"]:
        tm = (f'<h4>구성종목 주간 수익률 (면적 = 고정 비중, 색 = 수익률)</h4>'
              f'<div class="chart tm">{treemap_svg(b["movers"])}</div>'
              f'<p class="note">BM20은 블록미디어가 산출하는 한국형 디지털자산 대표 지수로, 글로벌 시가총액 상위 20개 자산을 '
              f'BTC 32%·ETH 20% 등 고정 비중으로 담습니다(2026 Q4 구성). 종목별 수익률은 월요일 값 대비 다음 월요일 값입니다.</p>')
    return f'''<section class="card wide pb">
  <header><h2>BM20 주간 성과</h2><span class="tag">확정 시계열</span></header>
  <div class="hero"><div class="big {cls(b["ret"])}">{arrow(b["ret"])} {abs(b["ret"]):.2f}%</div>
    <div class="hero-side"><div>{b["start"]:,.2f} → <b>{b["end"]:,.2f}</b></div>{prev}</div></div>
  <div class="chart">{line_svg(b["points"])}</div>{tm}
</section>'''


def card_total(s, daily, order):
    ws = datetime.fromisoformat(s["week_start"])
    d_html = ""
    if daily:
        legend = "".join(f'<i style="--c:{EX_COLORS.get(e, "#999")}"></i>{esc(e)}' for e in order)
        d_html = f'<div class="chart"><h4>일별 거래대금 (조원) · 연한 막대는 전주</h4>{daily_svg(daily, order)}<div class="legend">{legend}</div></div>'
    avg = s["total_krw"] / 7
    return f'''<section class="card wide">
  <header><h2>국내 5대 거래소 주간 거래대금</h2><span class="tag">원화마켓 · 월~일</span></header>
  <div class="hero"><div class="big">{jo(s["total_krw"])}</div>
    <div class="hero-side">{chg(s["total_wow_pct"])} <span class="sub">전주 {jo(s["prev_total_krw"])}</span>
    <div class="sub">일평균 {jo(avg)}</div></div></div>{d_html}
</section>'''


def card_exchanges(s):
    rows = ""
    mx = max(e["krw"] for e in s["exchanges"])
    for e in s["exchanges"]:
        star = '<sup title="캔들 종가×거래량 근사치">*</sup>' if e["approx"] else ""
        col = EX_COLORS.get(e["label"], "#999")
        rows += (f'<tr><td class="nm"><i style="--c:{col}"></i>{esc(e["label"])}{star}</td>'
                 f'<td class="num">{jo(e["krw"])}</td><td class="num">{chg(e["wow_pct"])}</td>'
                 f'<td class="num">{pct_share(e["share_pct"])}</td>'
                 f'<td class="bar"><span style="width:{e["krw"] / mx * 100:.1f}%;background:{col}"></span></td></tr>')
    stack = "".join(f'<span style="width:{e["share_pct"]:.2f}%;background:{EX_COLORS.get(e["label"], "#999")}" title="{esc(e["label"])} {e["share_pct"]:.1f}%"></span>'
                    for e in s["exchanges"])
    return f'''<section class="card">
  <header><h2>거래소별</h2><span class="tag">점유율</span></header>
  <div class="stack">{stack}</div>
  <div class="tw"><table><thead><tr><th></th><th>거래대금</th><th>전주 대비</th><th>점유율</th><th></th></tr></thead><tbody>{rows}</tbody></table></div>
  <p class="note">* 디지털X(구 코빗)·고팍스는 캔들 종가×거래량 근사치</p>
</section>'''


def card_stable(s):
    st = s.get("stablecoins")
    if not st:
        return ""
    per = sorted(st["per_coin"].items(), key=lambda kv: kv[1], reverse=True)[:4]
    li = "".join(f'<li><span>{esc(k)}</span><span>{jo(v)}</span></li>' for k, v in per)
    by = "".join(f'<li><span>{esc(e["label"])}</span><span>{st["by_exchange"][e["key"]]["share_of_exchange_pct"]:.1f}%</span></li>'
                 for e in s["exchanges"] if st["by_exchange"][e["key"]]["share_of_exchange_pct"] is not None)
    return f'''<section class="card">
  <header><h2>스테이블코인</h2><span class="tag">거래대금 기준</span></header>
  <div class="hero"><div class="big">{jo(st["krw"])}</div>
    <div class="hero-side">{chg(st["wow_pct"])} <div class="sub">전체의 {st["share_pct"]:.1f}%</div></div></div>
  <div class="two"><div><h4>종목별</h4><ul class="rank">{li}</ul></div><div><h4>거래소 내 비중</h4><ul class="rank">{by}</ul></div></div>
</section>'''


NAMES = {}
ALL_LABELS = []


def coin_label(sym, ex=None):
    ko = NAMES.get(sym)
    name = f'<span class="ko">{esc(ko)}</span> <b>{esc(sym)}</b>' if ko else f"<b>{esc(sym)}</b>"
    chips = ""
    if ex:
        if len(ex) == len(ALL_LABELS):
            chips = '<span class="chip all">5개 전체</span>'
        else:
            chips = "".join(f'<span class="chip">{esc(SHORT.get(e, e))}</span>' for e in ex)
    return f'<div class="cn">{name}</div>' + (f'<div class="chips">{chips}</div>' if chips else "")


def coin_list(rows, title, coins):
    li = ""
    for i, r in enumerate(rows, 1):
        ex = coins.get(r["coin"], {}).get("ex")
        li += (f'<li><span class="rk">{i}</span><div class="who">{coin_label(r["coin"], ex)}</div>'
               f'<span class="num">{jo(r["krw"])}</span><span class="num">{chg(r["wow_pct"], 1)}</span></li>')
    return f'<div><h4>{title}</h4><ul class="coins">{li}</ul></div>'


def card_coins(s, coins):
    return f'''<section class="card wide">
  <header><h2>거래대금 상위 종목</h2><span class="tag">5개 거래소 합산</span></header>
  <div class="two">{coin_list(s["top3"], "TOP 3", coins)}{coin_list(s["top3_excluding_stablecoins"], "TOP 3 (스테이블코인 제외)", coins)}</div>
</section>'''


def card_movers(coins, min_prev):
    pool = [c for c in coins.values() if c["prev_krw"] and c["prev_krw"] >= min_prev and c["wow_pct"] is not None]
    if not pool:
        return ""
    up = sorted(pool, key=lambda c: c["wow_pct"], reverse=True)[:3]
    dn = sorted(pool, key=lambda c: c["wow_pct"])[:3]
    def col(rows, title):
        li = "".join(f'<li><span class="rk">{i}</span><div class="who">{coin_label(c["coin"], c["ex"])}</div>'
                     f'<span class="num sm">{jo(c["prev_krw"])} → <b>{jo(c["krw"])}</b></span>'
                     f'<span class="num">{chg(c["wow_pct"], 1)}</span></li>' for i, c in enumerate(rows, 1))
        return f'<div><h4>{title}</h4><ul class="coins">{li}</ul></div>'
    return f'''<section class="card wide">
  <header><h2>거래대금 증감률 상위·하위</h2><span class="tag">5개 거래소 합산 · 전주 {jo(min_prev, 0)} 이상 종목</span></header>
  <div class="two">{col(up, "증가율 TOP 3")}{col(dn, "감소율 TOP 3")}</div>
</section>'''


def won(p):
    p = float(p)
    return f"{p:,.0f}" if p >= 100 else (f"{p:,.2f}" if p >= 1 else f"{p:.4g}")


def load_returns(data_dir, tag):
    path = os.path.join(data_dir, f"returns_{tag}.csv")
    if not os.path.exists(path):
        return None
    return {r["coin"]: r for r in csv.DictReader(open(path, encoding="utf-8-sig"))}


def card_returns(rets, coins, min_krw, min_listings=2):
    """수익률 상위/하위 3 (스테이블코인 제외, 완전한 한 주 거래된 종목, 주간 거래대금 하한)"""
    if rets is None:
        return ""
    pool = []
    for sym, r in rets.items():
        c = coins.get(sym)
        if not c or c["stable"] or r.get("full_week") != "Y" or not r.get("ret_pct") or c["krw"] < min_krw or len(c["ex"]) < min_listings:
            continue
        pool.append((float(r["ret_pct"]), sym, r, c))
    if len(pool) < 6:
        return ""
    pool.sort(key=lambda x: x[0], reverse=True)
    def col(rows, title):
        li = "".join(f'<li><span class="rk">{i}</span><div class="who">{coin_label(sym, c["ex"])}</div>'
                     f'<span class="num sm">{won(r["open"])} → <b>{won(r["close"])}</b>원<br>거래대금 {jo(c["krw"])}</span>'
                     f'<span class="num">{chg(ret, 1)}</span></li>' for i, (ret, sym, r, c) in enumerate(rows, 1))
        return f'<div><h4>{title}</h4><ul class="coins">{li}</ul></div>'
    return f'''<section class="card wide pb">
  <header><h2>주간 수익률 상위·하위</h2><span class="tag">원화 가격 · 주간 거래대금 {jo(min_krw, 0)} 이상{"" if min_listings <= 1 else f" · {min_listings}개 거래소 이상 거래"}</span></header>
  <div class="two">{col(pool[:3], "수익률 TOP 3")}{col(pool[::-1][:3], "수익률 하위 3")}</div>
  <p class="note">월요일 0시 첫 시가 → 일요일 24시 마지막 종가, 업비트(없으면 빗썸) 원화 가격. 스테이블코인과 주중 신규상장 종목은 제외.</p>
</section>'''


def card_story(out_dir, tag):
    """out/article_facts_<주>.json (make_article.py 결과)이 있으면 맨 위 시황 카드로 넣는다."""
    path = os.path.join(out_dir, f"article_facts_{tag}.json")
    if not os.path.exists(path):
        return ""
    try:
        paras = json.load(open(path, encoding="utf-8"))["paragraphs"]
    except Exception:
        return ""
    body = "".join(f"<p><strong>{esc(p['lead'])}.</strong> {esc(p['body'])}</p>" for p in paras)
    return f'<section class="card wide story"><header><h2>이번 주 시황</h2><span class="tag">집계 데이터 기반 · 자동 작성</span></header>{body}</section>'


def card_downloads(tag, present):
    names = [("by_exchange", "거래소별"), ("by_coin", "종목별 전체"), ("daily", "일별"), ("summary", "요약(JSON)")]
    ext = {"summary": "json"}
    links = "".join(f'<a class="dl" href="weekly_{tag}_{k}.{ext.get(k, "csv")}" download>{n}</a>'
                    for k, n in names if k in present)
    return f'<section class="card wide nodl"><header><h2>데이터 내려받기</h2></header><div class="dls">{links}</div></section>'


CSS = """
:root{--bg:#f3f5f8;--card:#fff;--ink:#141a26;--mute:#667085;--line:#e3e7ee;--accent:#0f5c8a;--up:#d6322d;--down:#1f5fd1;--soft:#eef2f7}
@media(prefers-color-scheme:dark){:root:not([data-theme=light]){--bg:#0e131b;--card:#171e29;--ink:#e8edf5;--mute:#93a0b4;--line:#263041;--accent:#5eb0e0;--up:#ff6b64;--down:#6c9dff;--soft:#1e2735;color-scheme:dark}}
:root[data-theme=dark]{--bg:#0e131b;--card:#171e29;--ink:#e8edf5;--mute:#93a0b4;--line:#263041;--accent:#5eb0e0;--up:#ff6b64;--down:#6c9dff;--soft:#1e2735;color-scheme:dark}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--ink);font:15px/1.5 "IBM Plex Sans KR","Noto Sans KR","Malgun Gothic",system-ui,sans-serif;padding-block:28px 48px;padding-left:max(16px,env(safe-area-inset-left,0px));padding-right:max(16px,env(safe-area-inset-right,0px))}
.wrap{max-width:1040px;margin:0 auto}
.top{display:flex;justify-content:space-between;align-items:flex-end;gap:12px;flex-wrap:wrap;margin-bottom:18px}
.top h1{margin:0;font-size:24px;letter-spacing:-.02em}.top p{margin:2px 0 0;color:var(--mute)}
.grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:18px 20px;min-width:0}
.wide{grid-column:1/-1}
.card header{display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:12px}
h2{margin:0;font-size:16px}h4{margin:14px 0 6px;font-size:12px;color:var(--mute);font-weight:500;letter-spacing:.04em}
.tag{font-size:12px;color:var(--mute);background:var(--soft);padding:2px 8px;border-radius:99px;white-space:nowrap}
.hero{display:flex;align-items:flex-end;gap:18px;flex-wrap:wrap}
.big{font-size:38px;font-weight:700;letter-spacing:-.03em;line-height:1.1;font-variant-numeric:tabular-nums}
.hero-side{display:flex;flex-direction:column;gap:2px;font-variant-numeric:tabular-nums}
.sub{color:var(--mute);font-size:13px}
.up{color:var(--up)}.down{color:var(--down)}.flat{color:var(--mute)}
.chg{font-variant-numeric:tabular-nums;font-weight:600}
.chart svg{width:100%;height:auto;display:block;margin-top:8px}
.tk{font-weight:600;color:var(--mute);font-size:13px}
.ln{fill:none;stroke:var(--accent);stroke-width:2.2;stroke-linejoin:round}.area{fill:var(--accent);opacity:.09}
.dot0{fill:var(--card);stroke:var(--accent);stroke-width:2}.dot1{fill:var(--accent)}
.ax{font-size:11px;fill:var(--mute)}.vl{font-size:11px;fill:var(--ink);font-variant-numeric:tabular-nums}.vl.b{font-weight:700}
.two{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:20px}
ul{list-style:none;margin:0;padding:0}
.rank li{display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid var(--line);font-variant-numeric:tabular-nums}
.coins li{display:grid;grid-template-columns:22px 1fr auto auto;gap:10px;align-items:center;padding:7px 0;border-bottom:1px solid var(--line)}
.rk{color:var(--mute)}
.who{min-width:0}.cn{font-weight:500}.ko{font-weight:600}.cn b{font-weight:600;color:var(--mute);font-size:13px;margin-left:2px}
.chips{display:flex;gap:4px;flex-wrap:wrap;margin-top:2px}.chip{font-size:11px;color:var(--mute);background:var(--soft);padding:0 6px;border-radius:99px;line-height:1.6}.chip.all{color:var(--accent)}
.num.sm{font-size:12px;color:var(--mute)}.num.sm b{color:var(--ink);font-weight:600}.nm{font-weight:600}.num{text-align:right;font-variant-numeric:tabular-nums}
.tw{overflow-x:auto}table{width:100%;border-collapse:collapse;font-size:14px}
th{font-size:12px;color:var(--mute);font-weight:500;text-align:right;padding:4px 6px}th:first-child{text-align:left}
td{padding:8px 6px;border-top:1px solid var(--line);white-space:nowrap}td.nm{text-align:left}
td.bar{width:22%}td.bar span{display:block;height:8px;border-radius:2px}
i{display:inline-block;width:9px;height:9px;border-radius:2px;background:var(--c);margin-right:6px}
.stack{display:flex;height:12px;border-radius:3px;overflow:hidden;margin-bottom:10px;gap:2px}
.legend{display:flex;gap:14px;flex-wrap:wrap;font-size:12px;color:var(--mute);margin-top:6px}
.note{margin:8px 0 0;font-size:12px;color:var(--mute)}
.dls{display:flex;gap:10px;flex-wrap:wrap}
a.dl{padding:8px 14px;border:1px solid var(--line);border-radius:6px;color:var(--accent);text-decoration:none;font-weight:600;font-size:14px}
a.dl:hover{background:var(--soft)}
.story p{margin:0 0 12px;line-height:1.75}.story p:last-child{margin-bottom:0}.story strong{font-weight:600}
.foot{margin-top:18px;font-size:12px;color:var(--mute);line-height:1.7}
@media(max-width:720px){.grid,.two{grid-template-columns:minmax(0,1fr)}.big{font-size:32px}}
.wmslot{display:none}.p3{display:contents}
@page{size:A4;margin:12mm 11mm 16mm}
@media print{
  *{-webkit-print-color-adjust:exact;print-color-adjust:exact}
  body{padding:0}
  .grid,.two{display:block}.big{font-size:32px}
  .card{margin-bottom:12px}
  .p3{display:flex;flex-direction:column;justify-content:space-evenly;min-height:252mm;break-inside:avoid}
  .p3 .card{margin-bottom:0}
  .coins li{padding:4px 0}
  .foot{margin-top:6px;font-size:11px;line-height:1.5}
  .card header,h4{break-after:avoid}
  .chart,svg,li,tr,.hero,.stack,.legend,.note,.story p{break-inside:avoid}
  .pb{break-before:page;page-break-before:always}
  .nodl{display:none}
  .wmslot{display:flex;align-items:center;justify-content:center;height:190px;margin-bottom:0;break-inside:avoid}
  .wmslot img{width:42%;opacity:.3}
}
"""


LOGO_B64 = "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"


def logo_data_uri(path=None):
    """블록미디어 로고(PNG)를 HTML 안에 직접 넣어, 파일 하나로 PDF·공유가 되게 한다. 없으면 워터마크 없이 만든다."""
    import base64
    here = os.path.dirname(os.path.abspath(__file__))
    for p in ([path] if path else []) + [os.path.join(here, "logo_dark.png"), os.path.join(here, "assets", "logo_dark.png"), "logo_dark.png"]:
        if p and os.path.exists(p):
            return "data:image/png;base64," + base64.b64encode(open(p, "rb").read()).decode()
    return "data:image/png;base64," + LOGO_B64  # 파일이 없으면 코드에 내장된 로고 사용


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--week", required=True, help="집계 주의 월요일 (YYYY-MM-DD)")
    ap.add_argument("--out-dir", default=OUT_DIR)
    ap.add_argument("--bm20-dir", help="bm 레포 로컬 사본 경로 (없으면 GitHub raw 에서 읽음)")
    ap.add_argument("--no-bm20", action="store_true")
    ap.add_argument("--min-prev-eok", type=float, default=300, help="증감률 순위에 넣을 전주 최소 거래대금(억원, 기본 300)")
    ap.add_argument("--data-dir", default="data")
    ap.add_argument("--min-krw-eok", type=float, default=100, help="수익률 순위에 넣을 주간 거래대금 하한(억원, 기본 100)")
    ap.add_argument("--min-listings", type=int, default=1, help="수익률 순위에 넣을 최소 거래소 수 (기본 1=전부, 2로 하면 단독 상장 종목 제외)")
    ap.add_argument("--logo", help="워터마크 로고 PNG 경로 (기본: 스크립트 폴더의 logo_dark.png)")
    ap.add_argument("--no-watermark", action="store_true", help="PDF 인쇄 시 1페이지 하단 로고 워터마크를 넣지 않음")
    ap.add_argument("--movers", action="store_true", help="거래대금 증감률 상위·하위 카드 포함 (기본 제외)")
    args = ap.parse_args()

    tag = args.week
    sp = os.path.join(args.out_dir, f"weekly_{tag}_summary.json")
    if not os.path.exists(sp):
        sys.exit(f"{sp} 가 없어요. 먼저 weekly_volume_collector.py --week {tag} 를 실행하세요.")
    s = json.load(open(sp, encoding="utf-8"))
    ws = datetime.fromisoformat(s["week_start"])

    global NAMES, ALL_LABELS
    order = [e["label"] for e in s["exchanges"]]
    ALL_LABELS = order
    NAMES = load_names(args.data_dir)
    stable_set = set((s.get("stablecoins") or {}).get("per_coin", {}))
    coins = load_coins(args.out_dir, tag, order, stable_set)
    daily = None
    dp = os.path.join(args.out_dir, f"weekly_{tag}_daily.csv")
    if os.path.exists(dp):
        daily = {}
        for r in csv.DictReader(open(dp, encoding="utf-8-sig")):
            if r["거래소"] != "합계":
                daily.setdefault(r["날짜_KST"], {})[r["거래소"]] = float(r["거래대금_원"])

    bm = None
    if not args.no_bm20:
        try:
            bm = load_bm20(ws.replace(tzinfo=None), args.bm20_dir)
        except Exception as e:
            print(f"BM20 불러오기 실패: {e}  (BM20 카드는 빼고 만듭니다)")

    movers_html = card_movers(coins, args.min_prev_eok * 1e8) if args.movers else ""
    present = {k for k in ("by_exchange", "by_coin", "daily", "summary")
               if os.path.exists(os.path.join(args.out_dir, f"weekly_{tag}_{k}.{'json' if k == 'summary' else 'csv'}"))}
    end = ws + timedelta(days=6)
    notes = "<br>".join(esc(n) for n in s.get("notes", []))
    logo = "" if args.no_watermark else logo_data_uri(args.logo)
    wm = f'<div class="wmslot"><img src="{logo}" alt=""></div>' if logo else ""
    if not args.no_watermark and not logo:
        print("(참고) logo_dark.png 를 못 찾아 워터마크 없이 만듭니다. 스크립트와 같은 폴더에 두거나 --logo 로 지정하세요.")
    page = f'''<!doctype html><html lang="ko"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>주간 시장 리포트 {tag}</title>
<link rel="preconnect" href="https://fonts.googleapis.com"><link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans+KR:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>{CSS}</style></head><body><div class="wrap">
<div class="top"><div><h1>주간 시장 리포트</h1><p>{ws:%Y.%m.%d} ~ {end:%m.%d} (KST 월~일)</p></div><p>블록미디어 마켓랩</p></div>
<div class="grid">
{card_story(args.out_dir, tag)}{wm}{card_bm20(bm)}{card_total(s, daily, order)}<div class="p3">{card_exchanges(s)}{card_stable(s)}</div>{card_returns(load_returns(args.data_dir, tag), coins, args.min_krw_eok * 1e8, args.min_listings)}{movers_html}{card_coins(s, coins)}
</div>
<div class="foot">{notes}<br>거래소 공시(09시 기준, 타 마켓 포함)와는 1~3% 차이가 날 수 있습니다.<br>BM20: 일별 확정 시계열 기준, 월요일 값 → 다음 월요일 값.</div>
</div></body></html>'''
    op = os.path.join(args.out_dir, f"weekly_{tag}_report.html")
    with open(op, "w", encoding="utf-8") as f:
        f.write(page)
    print(f"저장: {op}")


if __name__ == "__main__":
    main()
