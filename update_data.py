import requests
import json
from datetime import datetime

def fetch_and_save():
    # 1. 대상 코인 (블록미디어 주요 감시 종목)
    tickers = ["BTC", "ETH", "SOL", "XRP", "ADA", "DOGE", "TRX"]
    
    # 2. 업비트 가격 (KRW)
    upbit_url = f"https://api.upbit.com/v1/ticker?markets={','.join(['KRW-' + t for t in tickers])}"
    upbit_data = requests.get(upbit_url).json()
    upbit_prices = {d['market'].split('-')[1]: d['trade_price'] for d in upbit_data}

    # 3. 바이낸스 가격 (USDT)
    exchange_rate = 1420 # 실제 환경에선 환율 API 사용 권장
    result_data = []

    for t in tickers:
        try:
            b_url = f"https://api.binance.com/api/v3/ticker/price?symbol={t}USDT"
            b_price = float(requests.get(b_url).json()['price'])
            global_krw = b_price * exchange_rate
            premium = ((upbit_prices[t] / global_krw) - 1) * 100
            
            result_data.append({
                "coin": t,
                "upbit": upbit_prices[t],
                "premium": round(premium, 2)
            })
        except:
            continue

    # 4. 결과 저장 (경로 주의: labs/data.json)
    output = {
        "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "data": result_data
    }
    
    with open('labs/data.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=4)

if __name__ == "__main__":
    fetch_and_save()
