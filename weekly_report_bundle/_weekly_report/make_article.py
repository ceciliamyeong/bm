#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
주간 시황 기사 초안 자동 생성 (규칙 기반 템플릿)

원칙
  - 모든 숫자는 우리 집계(out/, data/)와 BM20 확정 시계열에서만 가져옵니다. 외부 기사·해외 시황은 쓰지 않습니다.
  - 문장은 규칙(조건)으로 조립합니다. 원인 추측("심리 위축" 등)은 쓰지 않습니다.
  - 결과는 '초안'입니다. 사람이 읽고 확인한 뒤 출고합니다.

입력  out/weekly_<월요일>_summary.json, _by_coin.csv, data/returns_<월요일>.csv, data/korean_names.json
출력  out/article_weekly_<월요일>.md      (읽기용)
      out/article_weekly_<월요일>.html    (워드프레스 붙여넣기/초안 전송용 본문)
      out/article_facts_<월요일>.json     (기사에 쓰인 숫자 원천. 검증·윤문 단계에서 사용)

사용 (cmd)
    python make_article.py --week 2026-10-12
    python make_article.py --week 2026-10-12 --bm20-dir C:\\경로\\bm     (로컬 레포 사본, 없으면 GitHub raw)
"""
import argparse
import csv
import json
import os
import sys
from datetime import datetime, timedelta

from make_weekly_report import load_bm20, load_names, load_returns, won

OUT_DIR = "out"
SHORT = {"업비트": "업비트", "빗썸": "빗썸", "코인원": "코인원", "디지털X(구 코빗)": "디지털X", "고팍스": "고팍스"}
DISPLAY = {"XRP": "리플", "BTC": "비트코인", "ETH": "이더리움", "USDT": "테더", "USDC": "USD코인"}
MAJORS = {"BTC", "ETH", "XRP"}
MIN_SHARE = 1.0      # 점유율(%) 이 값 미만 거래소는 '소폭' 문장으로 따로 처리
MIN_RET_KRW = 100e8  # 수익률 순위에 넣을 주간 거래대금 하한
MIN_PREV_SURGE = 300e8


# ───────────────────────── 한글 보조 ─────────────────────────
def batchim(s):
    ch = s[-1]
    if "가" <= ch <= "힣":
        return (ord(ch) - 0xAC00) % 28 != 0
    return ch in "LMNRlmnr136780"  # 엘·엠·엔·알 / 일·삼·육·칠·팔·영


def eun(s): return s + ("은" if batchim(s) else "는")
def i_ga(s): return s + ("이" if batchim(s) else "가")
def eul(s): return s + ("을" if batchim(s) else "를")
def wa(s): return s + ("과" if batchim(s) else "와")
def ro(s): return s + ("으로" if batchim(s) else "로")


def won_amt(v):
    return f"{v / 1e12:.2f}조원" if abs(v) >= 1e12 else f"{v / 1e8:,.0f}억원"


def pct(v, sign=False, d=1):
    return f"{v:+.{d}f}%" if sign else f"{abs(v):.{d}f}%"


# ───────────────────────── 데이터 ─────────────────────────
def load_all(out_dir, data_dir, tag, labels):
    s = json.load(open(os.path.join(out_dir, f"weekly_{tag}_summary.json"), encoding="utf-8"))
    coins = {}
    for r in csv.DictReader(open(os.path.join(out_dir, f"weekly_{tag}_by_coin.csv"), encoding="utf-8-sig")):
        prev = float(r["전주거래대금_원"]) if r.get("전주거래대금_원") else None
        coins[r["종목"]] = {
            "coin": r["종목"], "krw": float(r["주간거래대금_원"]), "prev": prev,
            "rank": int(r["순위"]), "by_ex": {l: float(r.get(f"{l}_원") or 0) for l in labels},
        }
    for sym, c in coins.items():
        c["ex"] = [l for l in labels if c["by_ex"][l] > 0]
    stable = set((s.get("stablecoins") or {}).get("per_coin", {}))
    return s, coins, stable


def nm(names, sym):
    return DISPLAY.get(sym) or names.get(sym) or sym


# ───────────────────────── 문단 ─────────────────────────
def para_total(s, bm):
    tot, prev, wow = s["total_krw"], s["prev_total_krw"], s["total_wow_pct"]
    bret = bm["ret"] if bm else None
    if wow <= -10: ph = "거래가 빠르게 식었다"
    elif wow <= -3: ph = "거래가 줄었다"
    elif wow < 3: ph = "거래가 비슷한 수준을 유지했다"
    elif wow < 10: ph = "거래가 늘었다"
    else: ph = "거래가 크게 늘었다"
    pre = ""
    if bret is not None and bret < -1: pre = "하락장에서 "
    elif bret is not None and bret > 1: pre = "상승장에서 "
    if bret is not None and bret < 0 and wow < 0 and abs(wow) > abs(bret):
        lead = "하락장에서 거래가 더 빠르게 식었다"
    else:
        lead = pre + ph
    up = wow >= 0
    body = (f"국내 5대 거래소 원화마켓 주간 거래대금은 {ro(won_amt(tot))} 전주({won_amt(prev)})보다 {pct(wow)} {'증가' if up else '감소'}했다. "
            f"일평균은 {won_amt(prev / 7)}에서 {ro(won_amt(tot / 7))} {'늘었다' if up else '줄었다'}.")
    if bm:
        body += f" 같은 기간 BM20은 {pct(bm['ret'], d=2)} {'상승' if bm['ret'] >= 0 else '하락'}했다."
    return lead, body


def para_exchanges(s):
    ex = s["exchanges"]
    big = [e for e in ex if e["share_pct"] >= MIN_SHARE]
    tiny = [e for e in ex if e["share_pct"] < MIN_SHARE]
    d = lambda e: e["share_pct"] - e["prev_share_pct"]
    gain = max(big, key=d)
    loss = min(big, key=lambda e: e["wow_pct"])
    L = lambda e: SHORT[e["label"]]
    if gain["key"] != loss["key"] and d(gain) > 0:
        lead = f"{eun(L(gain))} 점유율이 오르고 {eun(L(loss))} 낙폭이 컸다"
    else:
        lead = "거래소별 점유율은 큰 변화가 없었다"
    parts = []
    for e in big[:2]:
        dd = d(e)
        mv = "올랐다" if dd > 0.05 else ("내려갔다" if dd < -0.05 else "비슷했다")
        parts.append(f"{eun(L(e))} {won_amt(e['krw'])}({pct(e['wow_pct'], True)})으로 점유율이 "
                     f"{e['prev_share_pct']:.1f}%에서 {e['share_pct']:.1f}%로 {mv}.")
    rest = big[2:]
    seg = ", ".join(f"{eun(L(e))} {won_amt(e['krw'])}({pct(e['wow_pct'], True)})" for e in rest)
    tail = ""
    if tiny:
        t = tiny[0]
        mag = "소폭 " if abs(t["wow_pct"]) < 10 else ""
        tail = f"{eun(L(t))} {won_amt(t['krw'])}({pct(t['wow_pct'], True)})으로 {mag}{'늘었다' if t['wow_pct'] >= 0 else '줄었다'}."
    if seg:
        parts.append(seg + ("이었고, " + tail if tail else "이었다."))
    elif tail:
        parts.append(tail)
    return lead, " ".join(parts).replace("  ", " ")


def para_stable(s, coins, names, stable_set):
    st = s["stablecoins"]
    wow, share = st["wow_pct"], st["share_pct"]
    prev_share = st["prev_krw"] / s["prev_total_krw"] * 100
    if wow < 0 and share > prev_share: lead, conj = "거래는 줄었지만 비중은 높아졌다", "줄었으나"
    elif wow < 0: lead, conj = "거래와 비중이 함께 줄었다", "줄었고"
    elif share > prev_share: lead, conj = "거래와 비중이 함께 늘었다", "늘었고"
    else: lead, conj = "거래는 늘었지만 비중은 낮아졌다", "늘었으나"
    mv = "올랐다" if share > prev_share else "내려갔다"
    body = (f"스테이블코인 거래대금은 {ro(won_amt(st['krw']))} {pct(wow)} {conj} 전체에서 차지하는 비중은 "
            f"{prev_share:.1f}%에서 {share:.1f}%로 {mv}.")
    top = sorted(coins.values(), key=lambda c: -c["krw"])[:3]
    if top[0]["coin"] in stable_set:
        prevs = [c for c in coins.values() if c["prev"]]
        prev_top = max(prevs, key=lambda c: c["prev"])["coin"] if prevs else None
        verb = "1위를 지켰다" if prev_top == top[0]["coin"] else "1위에 올랐다"
        body += (f" {i_ga(nm(names, top[0]['coin']))} {won_amt(top[0]['krw'])}으로 "
                 f"{nm(names, top[1]['coin'])}({won_amt(top[1]['krw'])})·{nm(names, top[2]['coin'])}({won_amt(top[2]['krw'])}){eul(nm(names, top[2]['coin']))[len(nm(names, top[2]['coin'])):]} 제치고 종목별 {verb}.")
    else:
        body += f" 종목별 1위는 {nm(names, top[0]['coin'])}({won_amt(top[0]['krw'])})였다."
    big = [e for e in s["exchanges"] if e["share_pct"] >= MIN_SHARE]
    sh = {e["label"]: st["by_exchange"][e["key"]]["share_of_exchange_pct"] for e in big}
    order = sorted(sh, key=lambda k: -sh[k])
    hi = order[:2]
    lo = [e["label"] for e in big if e["label"] not in hi]  # 거래대금 큰 순
    if len(big) >= 3:
        body += (f" 거래소 안에서 스테이블코인이 차지하는 비중은 {i_ga(SHORT[hi[0]])} {sh[hi[0]]:.1f}%, {i_ga(SHORT[hi[1]])} {sh[hi[1]]:.1f}%로 높았고, "
                 + ", ".join(f"{eun(SHORT[k])} {sh[k]:.1f}%" for k in lo) + "였다.")
    return lead, body


def para_popular(s, coins, names, stable_set, labels):
    biggest = s["exchanges"][0]
    bl = biggest["label"]
    cand = [c for c in coins.values() if c["coin"] not in stable_set]
    top_in_big = max(cand, key=lambda c: c["by_ex"][bl])
    btc = coins.get("BTC")
    if btc and top_in_big["coin"] != "BTC" and top_in_big["by_ex"][bl] > btc["by_ex"][bl]:
        lead = f"{SHORT[bl]}에서는 {i_ga(nm(names, top_in_big['coin']))} 비트코인을 앞섰다"
        body = (f"{SHORT[bl]}의 {nm(names, top_in_big['coin'])} 거래대금은 {ro(won_amt(top_in_big['by_ex'][bl]))} "
                f"비트코인({won_amt(btc['by_ex'][bl])})보다 많았다.")
    else:
        lead = "비트코인과 주요 코인이 상위권을 지켰다"
        t3 = sorted(cand, key=lambda c: -c["krw"])[:3]
        body = "비(非)스테이블 거래대금 상위는 " + ", ".join(f"{nm(names, c['coin'])}({won_amt(c['krw'])})" for c in t3) + "였다."
    top10 = sorted(coins.values(), key=lambda c: -c["krw"])[:10]
    alts = [c for c in top10 if c["coin"] not in stable_set and c["coin"] not in MAJORS][:3]
    if alts:
        body += (" " + ", ".join(f"{nm(names, c['coin'])}({won_amt(c['krw'])})" for c in alts) +
                 " 같은 알트코인이 전체 거래대금 상위 10위에 들었다.")
        for c in alts:
            other = max((l for l in labels if l != bl), key=lambda l: c["by_ex"][l])
            if c["by_ex"][other] > c["by_ex"][bl]:
                body += (f" {eun(nm(names, c['coin']))} {SHORT[other]}({won_amt(c['by_ex'][other])})이 "
                         f"{SHORT[bl]}({won_amt(c['by_ex'][bl])})보다 거래가 많았다.")
                break
    return lead, body


def ret_pools(rets, coins, stable_set):
    pool = []
    for sym, r in rets.items():
        c = coins.get(sym)
        if not c or sym in stable_set or r.get("full_week") != "Y" or not r.get("ret_pct") or c["krw"] < MIN_RET_KRW:
            continue
        pool.append((float(r["ret_pct"]), sym, r, c))
    pool.sort(key=lambda x: x[0], reverse=True)
    return pool


def only_ex(rows):
    """상위/하위 3종목 중 한 거래소에서만 거래된 종목의 거래소 이름(여러 곳이면 가장 많은 곳)과 개수"""
    ones = [c["ex"][0] for _, _, _, c in rows if len(c["ex"]) == 1]
    if not ones:
        return None, 0
    top = max(set(ones), key=ones.count)
    return top, ones.count(top)


def mult_phrase(prev, cur):
    r = cur / prev
    if r >= 2: return f"{int(r)}배 넘게 늘었다"
    if r > 1: return f"{(r - 1) * 100:.0f}% 늘었다"
    return f"{(1 - r) * 100:.0f}% 줄었다"


def para_gainers(pool, coins, names, s):
    top = pool[:3]
    ex_name, n = only_ex(top)
    if ex_name and n >= 2:
        lead = f"{SHORT[ex_name]}에서만 거래된 종목이 수익률 상위를 채웠다"
    else:
        lead = f"수익률 상위는 {nm(names, top[0][1])} 등이 차지했다"
    ret, sym, r, c = top[0]
    body = (f"{eun(nm(names, sym))} 주초 {won(r['open'])}원에서 {ro(won(r['close']) + '원')} {pct(ret, d=0 if abs(ret) % 1 < 0.05 else 1)} 올라 "
            f"거래대금 {won_amt(MIN_RET_KRW)} 이상 종목 중 수익률 1위를 기록했다.")
    if c["prev"]:
        body += f" 거래대금도 {won_amt(c['prev'])}에서 {won_amt(c['krw'])}으로 {mult_phrase(c['prev'], c['krw'])}."
    rest = top[1:]
    n0, n1 = nm(names, rest[0][1]), nm(names, rest[1][1])
    pair = f"{n0}({pct(rest[0][0], True)}){wa(n0)[len(n0):]} {n1}({pct(rest[1][0], True)}){i_ga(n1)[len(n1):]}"
    if ex_name and n == 3:
        body += f" {pair} 뒤를 이었는데, 세 종목 모두 {SHORT[ex_name]}에서만 거래됐다."
    else:
        body += f" {pair} 뒤를 이었다."
    # 거래대금 쏠림
    surge = [c for c in coins.values() if c["prev"] and c["prev"] >= MIN_PREV_SURGE and c["krw"] / c["prev"] >= 2]
    surge.sort(key=lambda c: -(c["krw"] / c["prev"]))
    surge = surge[:2]
    if surge:
        body += " 거래대금 쏠림도 컸다. " + ", ".join(
            f"{eun(nm(names, c['coin']))} {won_amt(c['prev'])}에서 {won_amt(c['krw'])}으로 {c['krw'] / c['prev']:.1f}배" for c in surge) + " 늘었다."
    return lead, body


def para_losers(pool, names):
    bot = list(reversed(pool))[:3]
    ex_name, n = only_ex(bot)
    if ex_name and n >= 2:
        lead = f"{SHORT[ex_name]}에서만 거래된 종목의 낙폭이 컸다"
    else:
        lead = f"수익률 하위는 {nm(names, bot[0][1])} 등이 차지했다"
    body = "하위 3위는 " + "·".join(f"{nm(names, x[1])}({pct(x[0], True)})" for x in bot) + "였다."
    r0 = bot[0]
    if len(r0[3]["ex"]) == 1 and r0[3]["krw"] >= 1000e8:
        body += f" {eun(nm(names, r0[1]))} {SHORT[r0[3]['ex'][0]]}에서만 거래됐는데도 거래대금이 {won_amt(r0[3]['krw'])}에 달했다."
    return lead, body


# ───────────────────────── 조립 ─────────────────────────
def main():
    ap = argparse.ArgumentParser(description="주간 시황 기사 초안 생성")
    ap.add_argument("--week", required=True)
    ap.add_argument("--out-dir", default=OUT_DIR)
    ap.add_argument("--data-dir", default="data")
    ap.add_argument("--bm20-dir")
    ap.add_argument("--no-bm20", action="store_true")
    args = ap.parse_args()
    tag = args.week
    ws = datetime.strptime(tag, "%Y-%m-%d")

    s0 = json.load(open(os.path.join(args.out_dir, f"weekly_{tag}_summary.json"), encoding="utf-8"))
    labels = [e["label"] for e in s0["exchanges"]]
    s, coins, stable_set = load_all(args.out_dir, args.data_dir, tag, labels)
    names = load_names(args.data_dir)
    rets = load_returns(args.data_dir, tag)
    bm = None
    if not args.no_bm20:
        try:
            bm = load_bm20(ws, args.bm20_dir)
        except Exception as e:
            print(f"BM20 불러오기 실패: {e} (BM20 문장은 뺍니다)")

    paras = [("거래대금", para_total(s, bm)), ("거래소별", para_exchanges(s))]
    if s.get("stablecoins"):
        paras.append(("스테이블코인", para_stable(s, coins, names, stable_set)))
    paras.append(("인기 코인", para_popular(s, coins, names, stable_set, labels)))
    if rets:
        pool = ret_pools(rets, coins, stable_set)
        if len(pool) >= 6:
            paras.append(("급등 종목", para_gainers(pool, coins, names, s)))
            paras.append(("급락 종목", para_losers(pool, names)))
        else:
            print("수익률 종목이 부족해 급등·급락 문단을 뺍니다.")
    else:
        print(f"data/returns_{tag}.csv 가 없어 급등·급락 문단을 뺍니다 (weekly_returns.py 먼저 실행).")

    end = ws + timedelta(days=6)
    title = f"[블록미디어 마켓랩] 국내 5대 거래소 주간 시황 ({ws:%m/%d}~{end:%m/%d})"
    foot = ("집계: KST 월 0시~일 24시, 원화마켓 기준. 수익률은 월요일 첫 시가→일요일 마지막 종가(업비트, 없으면 빗썸). "
            "디지털X·고팍스 거래대금은 캔들 종가×거래량 근사치.")
    md = f"**{title}**\n\n" + "\n\n".join(f"**{lead}.** {body}" for _, (lead, body) in paras) + f"\n\n*{foot}*\n"
    html = f"<h2>{title}</h2>\n" + "\n".join(f"<p><strong>{lead}.</strong> {body}</p>" for _, (lead, body) in paras) + f"\n<p><em>{foot}</em></p>\n"

    os.makedirs(args.out_dir, exist_ok=True)
    open(os.path.join(args.out_dir, f"article_weekly_{tag}.md"), "w", encoding="utf-8").write(md)
    open(os.path.join(args.out_dir, f"article_weekly_{tag}.html"), "w", encoding="utf-8").write(html)
    facts = {"week": tag, "title": title, "paragraphs": [{"key": k, "lead": l, "body": b} for k, (l, b) in paras],
             "bm20": {k: bm[k] for k in ("start", "end", "ret", "prev_ret")} if bm else None, "summary": s0}
    json.dump(facts, open(os.path.join(args.out_dir, f"article_facts_{tag}.json"), "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    print(md)
    print(f"저장: out/article_weekly_{tag}.md / .html / article_facts_{tag}.json")


if __name__ == "__main__":
    main()
