import requests
import json
import time
from datetime import datetime

def get_kimchi_data():
    # 1. 대상 코인 리스트
    tickers = ["BTC", "ETH", "SOL", "XRP", "ADA", "DOGE"]
    
    # 2. 업비트 가격 가져오기 (KRW) [cite: 20]
    upbit_url = f"https://api.upbit.com/v1/ticker?markets={','.join(['KRW-' + t for t in tickers])}"
    upbit_data = requests.get(upbit_url).json()
    upbit_prices = {d['market'].split('-')[1]: d['trade_price'] for d in upbit_data}

    # 3. 바이낸스 가격 가져오기 (USDT) [cite: 20]
    binance_prices = {}
    for t in tickers:
        try:
            b_url = f"https://api.binance.com/api/v3/ticker/price?symbol={t}USDT"
            binance_prices[t] = float(requests.get(b_url).json()['price'])
        except:
            binance_prices[t] = 0

    # 4. 실시간 환율 가져오기 (가상 API 또는 고정값 사용 가능) [cite: 27]
    # 실제로는 실시간 FX API 연동이 필요하나, 여기선 예시로 1400원을 사용합니다.
    exchange_rate = 1400 

    # 5. 김치 프리미엄 계산 [cite: 20]
    result = []
    for t in tickers:
        global_krw = binance_prices[t] * exchange_rate
        premium = ((upbit_prices[t] / global_krw) - 1) * 100 if global_krw > 0 else 0
        result.append({
            "coin": t,
            "upbit": upbit_prices[t],
            "premium": round(premium, 2)
        })

    return {
        "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "data": result
    }

# 데이터 저장
if __name__ == "__main__":
    final_data = get_kimchi_data()
    with open('labs/data.json', 'w', encoding='utf-8') as f:
        json.dump(final_data, f, ensure_ascii=False, indent=4)
    print("Data updated successfully!")
