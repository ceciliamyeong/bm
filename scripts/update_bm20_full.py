import requests
import json
import os
from datetime import datetime

def get_fear_and_greed():
    """심리 지수 가져오기"""
    try:
        url = "https://api.alternative.me/fng/?limit=1"
        res = requests.get(url, timeout=10).json()
        return {"value": int(res['data'][0]['value']), "status": res['data'][0]['value_classification']}
    except:
        return {"value": None, "status": "Error"}

def get_k_share(api_key, my_krw_volume):
    """한국 시장 점유율 계산 (환율 1,450원)"""
    usd_krw_rate = 1450 
    my_vol_usd = my_krw_volume / usd_krw_rate
    
    try:
        url = "https://pro-api.coinmarketcap.com/v1/global-metrics/quotes/latest"
        headers = {'X-CMC_PRO_API_KEY': api_key}
        res = requests.get(url, headers=headers, timeout=10).json()
        global_vol_usd = res['data']['quote']['usd']['total_volume_24h']
        
        return {
            "global_vol_usd": round(global_vol_usd, 2),
            "krw_vol_usd": round(my_vol_usd, 2),
            "k_share_percent": round((my_vol_usd / global_vol_usd) * 100, 2)
        }
    except Exception as e:
        print(f"CMC API Error: {e}")
        return {"global_vol_usd": 0, "krw_vol_usd": 0, "k_share_percent": 0}

def main():
    # 깃허브 시크릿에서 API 키 로드
    CMC_API_KEY = os.environ.get('CMC_API_KEY')
    
    # 1. 원화 데이터 읽기 (data/ 폴더 내 파일 기준)
    # 수집 스크립트가 이 경로에 숫자를 저장한다고 가정합니다.
    vol_file_path = 'data/current_krw_vol.txt'
    try:
        with open(vol_file_path, 'r') as f:
            current_krw_total = float(f.read().strip())
    except:
        print("Warning: 원화 거래량 파일을 찾을 수 없어 0으로 처리합니다.")
        current_krw_total = 0

    # 2. 신규 데이터 생성
    new_entry = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "sentiment": get_fear_and_greed(),
        "k_market": get_k_share(CMC_API_KEY, current_krw_total),
        "exchange_rate_used": 1450
    }

    # 3. 히스토리 파일 누적 (data/bm20_history.json)
    history_file = 'data/bm20_history.json'
    history_data = []
    
    if os.path.exists(history_file):
        with open(history_file, 'r', encoding='utf-8') as f:
            try:
                history_data = json.load(f)
            except: history_data = []

    history_data.append(new_entry)
    
    # 4. 저장
    os.makedirs('data', exist_ok=True)
    with open(history_file, 'w', encoding='utf-8') as f:
        json.dump(history_data, f, indent=4, ensure_ascii=False)
    
    print(f"Update Success: {new_entry['timestamp']}")

if __name__ == "__main__":
    main()
