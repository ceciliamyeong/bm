import requests
import json
import os
from datetime import datetime
from pathlib import Path

# 1,450원 환율 고정
EXCHANGE_RATE = 1450

def get_fear_and_greed():
    """심리 지수 가져오기"""
    try:
        url = "https://api.alternative.me/fng/?limit=1"
        res = requests.get(url, timeout=10).json()
        return {"value": int(res['data'][0]['value']), "status": res['data'][0]['value_classification']}
    except Exception as e:
        print(f"[DEBUG] Fear & Greed API Error: {e}")
        return {"value": 5, "status": "Extreme Fear"}

def get_k_share(api_key, krw_total_24h):
    """한국 시장 점유율 계산"""
    my_vol_usd = krw_total_24h / EXCHANGE_RATE
    
    if not api_key:
        print("[DEBUG] CMC_API_KEY missing")
        return {"global_vol_usd": 0, "krw_vol_usd": my_vol_usd, "k_share_percent": 0}

    try:
        url = "https://pro-api.coinmarketcap.com/v1/global-metrics/quotes/latest"
        headers = {'X-CMC_PRO_API_KEY': api_key}
        response = requests.get(url, headers=headers, timeout=15)
        res = response.json()
        
        # CMC 데이터 추출 (대문자/소문자 모두 대응)
        data_root = res.get('data', {}).get('quote', {})
        # USD 또는 usd 키를 모두 확인
        usd_data = data_root.get('USD') or data_root.get('usd') or {}
        
        global_vol_usd = usd_data.get('total_volume_24h', 0)
        k_share = (my_vol_usd / global_vol_usd) * 100 if global_vol_usd > 0 else 0
        
        return {
            "global_vol_usd": round(global_vol_usd, 2),
            "krw_vol_usd": round(my_vol_usd, 2),
            "k_share_percent": round(k_share, 2)
        }
    except Exception as e:
        print(f"[DEBUG] CMC API parsing error: {e}")
        return {"global_vol_usd": 0, "krw_vol_usd": my_vol_usd, "k_share_percent": 0}

def main():
    CMC_API_KEY = os.environ.get('CMC_API_KEY')
    
    # 이미지에서 확인된 성공 경로 사용
    latest_vol_path = Path("out/history/krw_24h_latest.json")
    
    krw_total_24h = 0
    if latest_vol_path.exists():
        try:
            with open(latest_vol_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                krw_total_24h = data.get("totals", {}).get("combined_24h", 0)
                print(f"[SUCCESS] 거래량 추출 성공: {krw_total_24h}")
        except Exception as e:
            print(f"[DEBUG] File read error: {e}")
    else:
        print("[ERROR] 파일을 찾을 수 없습니다.")

    # 함수 정의 순서에 따른 오류 방지를 위해 main 내부에서 호출
    sentiment = get_fear_and_greed()
    k_market = get_k_share(CMC_API_KEY, krw_total_24h)

    new_entry = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "sentiment": sentiment,
        "k_market": k_market,
        "exchange_rate": EXCHANGE_RATE
    }

    # 히스토리 누적 저장
    history_file = Path("data/bm20_history.json")
    history_file.parent.mkdir(parents=True, exist_ok=True)
    
    history_list = []
    if history_file.exists():
        with open(history_file, 'r', encoding='utf-8') as f:
            try:
                history_list = json.load(f)
            except: history_list = []

    history_list.append(new_entry)
    with open(history_file, 'w', encoding='utf-8') as f:
        json.dump(history_list, f, indent=4, ensure_ascii=False)
    
    print(f"[FINAL] 업데이트 완료 - K-Share: {k_market['k_share_percent']}%")

if __name__ == "__main__":
    main()
