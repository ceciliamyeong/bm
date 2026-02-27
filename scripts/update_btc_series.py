from pathlib import Path
import json

ROOT = Path(__file__).resolve().parent.parent
BTC_SERIES = ROOT / "out/history/btc_usd_series.json"
BM20_JSON = ROOT / "bm20_latest.json"

def read_json(path: Path, default):
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))

def update():
    series = read_json(BTC_SERIES, default=[])
    bm20 = read_json(BM20_JSON, default={})

    asof = bm20.get("asof")
    if not asof:
        raise KeyError("bm20_latest.json missing 'asof'")

    btc_price = (
        bm20.get("btc_price_usd")
        or bm20.get("btc_usd")
        or bm20.get("btc_price")
        or bm20.get("btcPriceUsd")
    )
    if btc_price is None:
        raise KeyError("bm20_latest.json missing BTC price (btc_price_usd/btc_usd/btc_price/btcPriceUsd)")

    btc_price = float(btc_price)

    if series and series[-1].get("date") == asof:
        print("BTC already updated.")
        return

    series.append({"date": asof, "price": btc_price})
    BTC_SERIES.parent.mkdir(parents=True, exist_ok=True)
    BTC_SERIES.write_text(json.dumps(series, ensure_ascii=False, indent=2), encoding="utf-8")
    print("BTC series updated.")

if __name__ == "__main__":
    update()
