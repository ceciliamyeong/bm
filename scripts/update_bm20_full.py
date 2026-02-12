import requests
import json
import os
from datetime import datetime
from pathlib import Path

# 1,450원 환율 고정
EXCHANGE_RATE = 1450

def get_fear_and_greed():
    try:
        url = "https://api.alternative.me/fng/?limit=1"
        res = requests.get(url, timeout=10).json()
        return {"value": int(res['data'][0]['value']), "status": res['data'][0]['value_classification']}
    except:
        return {"value": None, "status": "Error"}

def get_k_share(api_key, krw_total_24h):
    """원화 총 거래량(KRW)을 받아 글로벌 점유율 계산"""
    my_vol_usd = krw_total_24h / EXCHANGE_RATE
    
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
    CMC_API_KEY = os.environ.get('CMC_API_KEY')
    
    # 1. 사용자님의 기존 코드 출력물 경로 설정
    # 구조: root/out/history/krw_24h_latest.json
    base_dir = Path(__file__).resolve().parent.parent
    latest_vol_path = base_dir / "out" / "history" / "krw_24h_latest.json"
    
    krw_total_24h = 0
    if latest_vol_path.exists():
        with open(latest_vol_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            # 기존 코드의 'totals' -> 'combined_24h' 값을 가져옴
            krw_total_24h = data.get("totals", {}).get("combined_24h", 0)
    else:
        print(f"Warning: {latest_vol_path} 파일을 찾을 수 없습니다.")

    # 2. 신규 데이터 수집 및 계산
    sentiment = get_fear_and_greed()
    k_market = get_k_share(CMC_API_KEY, krw_total_24h)

    # 3. 통합 히스토리 생성
    new_entry = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "sentiment": sentiment,
        "k_market": k_market,
        "exchange_rate": EXCHANGE_RATE
    }

    # 4. 히스토리 파일 누적 저장 (data/bm20_history.json)
    history_dir = base_dir / "data"
    history_dir.mkdir(exist_ok=True)
    history_file = history_dir / "bm20_history.json"
    
    history_list = []
    if history_file.exists():
        with open(history_file, 'r', encoding='utf-8') as f:
            try:
                history_list = json.load(f)
            except: history_list = []

    history_list.append(new_entry)
    
    with open(history_file, 'w', encoding='utf-8') as f:
        json.dump(history_list, f, indent=4, ensure_ascii=False)
    
    print(f"Update Success: {new_entry['timestamp']} | K-Share: {k_market['k_share_percent']}%")

if __name__ == "__main__":
    main()
