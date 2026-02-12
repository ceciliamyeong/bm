import requests
import json
import os
from datetime import datetime
from pathlib import Path

EXCHANGE_RATE = 1450

def get_k_share(api_key, krw_total_24h):
    my_vol_usd = krw_total_24h / EXCHANGE_RATE
    
    if not api_key:
        return {"global_vol_usd": 0, "krw_vol_usd": my_vol_usd, "k_share_percent": 0}

    try:
        url = "https://pro-api.coinmarketcap.com/v1/global-metrics/quotes/latest"
        headers = {'X-CMC_PRO_API_KEY': api_key}
        response = requests.get(url, headers=headers, timeout=10)
        res = response.json()
        
        # 데이터 추출 경로를 더 유연하게 설정 (무료 플랜 대응)
        # 보통 data -> quote -> usd 순서지만, 혹시 모를 상황 대비
        try:
            quote_data = res.get('data', {}).get('quote', {}).get('USD', {}) # 대문자 USD 시도
            if not quote_data:
                quote_data = res.get('data', {}).get('quote', {}).get('usd', {}) # 소문자 usd 시도
                
            global_vol_usd = quote_data.get('total_volume_24h', 0)
        except:
            global_vol_usd = 0

        # 만약 여전히 0이라면 다른 경로(시가총액 등)에서 추출 시도
        if global_vol_usd == 0:
            print("[DEBUG] CMC 응답 구조:", res) # 구조 확인을 위한 로그 출력
            
        k_share = (my_vol_usd / global_vol_usd) * 100 if global_vol_usd > 0 else 0
        
        return {
            "global_vol_usd": round(global_vol_usd, 2),
            "krw_vol_usd": round(my_vol_usd, 2),
            "k_share_percent": round(k_share, 2)
        }
    except Exception as e:
        print(f"[ERROR] {e}")
        return {"global_vol_usd": 0, "krw_vol_usd": my_vol_usd, "k_share_percent": 0}



def main():
    CMC_API_KEY = os.environ.get('CMC_API_KEY')
    
    # 1. 경로 강제 수정
    # GitHub Actions 실행 시 작업 디렉토리는 보통 저장소의 루트(/home/runner/work/bm/bm)입니다.
    # 따라서 scripts/ 에서 실행되더라도 루트 기준 상대 경로를 시도합니다.
    
    current_path = Path.cwd() # 현재 실행 경로 확인
    print(f"[DEBUG] 현재 작업 디렉토리: {current_path}")
    
    # 여러 경로 후보군을 순차적으로 확인 (절대 실패하지 않도록)
    possible_paths = [
        Path("out/history/krw_24h_latest.json"),                # 루트 기준
        Path("../out/history/krw_24h_latest.json"),             # scripts/ 안에서 실행 시
        current_path / "out" / "history" / "krw_24h_latest.json" # 절대 경로
    ]
    
    latest_vol_path = None
    for p in possible_paths:
        if p.exists():
            latest_vol_path = p
            print(f"[DEBUG] 파일 발견! 사용 경로: {p}")
            break
            
    krw_total_24h = 0
    if latest_vol_path:
        with open(latest_vol_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            krw_total_24h = data.get("totals", {}).get("combined_24h", 0)
            print(f"[SUCCESS] 거래량 추출 성공: {krw_total_24h}")
    else:
        print("[ERROR] krw_24h_latest.json 파일을 어떤 경로에서도 찾을 수 없습니다.")
        # 디버깅을 위해 현재 폴더 구조 출력
        print(f"[DEBUG] 현재 폴더 리스트: {os.listdir('.')}")

    # 데이터 생성 및 저장
    sentiment = get_fear_and_greed()
    k_market = get_k_share(CMC_API_KEY, krw_total_24h)

    new_entry = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "sentiment": sentiment,
        "k_market": k_market,
        "exchange_rate": EXCHANGE_RATE
    }

    # 저장 경로도 루트 기준 data/ 폴더로 강제
    history_file = Path("data/bm20_history.json")
    history_file.parent.mkdir(parents=True, exist_ok=True)
    
    history_list = []
    if history_file.exists():
        with open(history_file, 'r', encoding='utf-8') as f:
            try: history_list = json.load(f)
            except: history_list = []

    history_list.append(new_entry)
    with open(history_file, 'w', encoding='utf-8') as f:
        json.dump(history_list, f, indent=4, ensure_ascii=False)
    
    print(f"[FINAL] K-Share: {k_market['k_share_percent']}%")

if __name__ == "__main__":
    main()
