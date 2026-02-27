from pathlib import Path
import json

ROOT = Path(__file__).resolve().parent.parent
BTC_SERIES = ROOT / "out/history/btc_usd_series.json"
BM20_JSON = ROOT / "bm20_latest.json"

def load_json(p):
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))

def pick(d, keys):
    for k in keys:
        if k in d and d[k]:
            return d[k]
    return None

def update():
    series = load_json(BTC_SERIES) or []
    bm20 = load_json(BM20_JSON)
    if bm20 is None:
        raise FileNotFoundError("bm20_latest.json missing")

    # 날짜 키 여러 개 대응
    asof = pick(bm20, ["asof", "asOf", "date", "timestamp"])
    if asof is None:
        raise KeyError("bm20_latest.json missing date key")

    # BTC 가격 키 여러 개 대응
    btc_price = pick(bm20, ["btc_price_usd", "btc_usd", "btcPriceUsd"])
    if btc_price is None:
        raise KeyError("bm20_latest.json missing BTC price")

    if series and str(series[-1].get("date")) == str(asof):
        print("BTC already updated.")
        return

    series.append({
        "date": str(asof),
        "price": float(btc_price)
    })

    BTC_SERIES.parent.mkdir(parents=True, exist_ok=True)
    BTC_SERIES.write_text(json.dumps(series, indent=2), encoding="utf-8")
    print("BTC series updated.")

if __name__ == "__main__":
    update()
