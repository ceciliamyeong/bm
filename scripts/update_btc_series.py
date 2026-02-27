#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
import json

ROOT = Path(__file__).resolve().parent.parent

BTC_SERIES = ROOT / "out/history/btc_usd_series.json"
BM20_JSON  = ROOT / "bm20_latest.json"
KIMCHI_LAST = ROOT / "out/latest/cache/kimchi_last.json"  # btc_usd가 여기에 있는 경우가 많음

def load_json(p: Path):
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))

def pick(d: dict, keys: list[str]):
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return None

def update():
    series = load_json(BTC_SERIES) or []
    bm20 = load_json(BM20_JSON)
    if bm20 is None:
        raise FileNotFoundError(f"Missing {BM20_JSON}")

    # ✅ 너 포맷: asOf
    asof = pick(bm20, ["asOf", "asof", "date", "timestamp"])
    if asof is None:
        raise KeyError("bm20_latest.json missing date key (expected asOf/asof/date/timestamp)")

    # ✅ bm20_latest.json에는 BTC가 없으니 kimchi_last에서 가져오기
    kimchi = load_json(KIMCHI_LAST) or {}
    btc_price = pick(kimchi, ["btc_usd", "btcPriceUsd", "btc_price_usd"])
    if btc_price is None:
        raise KeyError("BTC price not found. Provide out/latest/cache/kimchi_last.json with btc_usd OR store btc_usd into bm20_latest.json")

    asof = str(asof)

    if series and str(series[-1].get("date")) == asof:
        print("BTC already updated.")
        return

    series.append({"date": asof, "price": float(btc_price)})

    BTC_SERIES.parent.mkdir(parents=True, exist_ok=True)
    BTC_SERIES.write_text(json.dumps(series, ensure_ascii=False, indent=2), encoding="utf-8")
    print("BTC series updated.")

if __name__ == "__main__":
    update()
