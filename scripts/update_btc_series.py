from pathlib import Path
import json
from datetime import datetime

ROOT = Path(__file__).resolve().parent.parent
BTC_SERIES = ROOT / "out/history/btc_usd_series.json"
BM20_JSON = ROOT / "bm20_latest.json"

def update():
    series = json.loads(BTC_SERIES.read_text(encoding="utf-8"))
    bm20 = json.loads(BM20_JSON.read_text(encoding="utf-8"))

    asof = bm20["asof"]
    btc_price = bm20["btc_price_usd"]  # ← bm20에서 BTC 가격 가져오는 구조라면

    # 중복 방지
    if series and series[-1]["date"] == asof:
        print("BTC already updated.")
        return

    series.append({
        "date": asof,
        "price": btc_price
    })

    BTC_SERIES.write_text(json.dumps(series, indent=2), encoding="utf-8")
    print("BTC series updated.")

if __name__ == "__main__":
    update()
