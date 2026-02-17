import json
from pathlib import Path

# ===== 입력 파일 (네가 업로드한 JBBitcoin JSON 경로) =====
JB_FILE = Path("jpbitcoin (3).json")   # 필요하면 파일명만 수정

# ===== 출력 파일 =====
OUT_GLOBAL = Path("out/global/global_btc_liquidity_monthly.json")
OUT_SHARE  = Path("out/global/k_btc_share_monthly.json")


def main():
    if not JB_FILE.exists():
        print(f"[ERROR] JB 파일을 찾을 수 없습니다: {JB_FILE}")
        return

    # JB JSON 로드
    with open(JB_FILE, "r", encoding="utf-8") as f:
        jb = json.load(f)

    """
    JB 파일 구조는 보통 이렇게 생김:
    [
      {"month":"2025-01", "JPY":..., "USD":..., "KRW":...},
      ...
    ]
    또는
    {
      "data":[ ... ]
    }
    """

    if isinstance(jb, dict) and "data" in jb:
        rows = jb["data"]
    elif isinstance(jb, list):
        rows = jb
    else:
        print("[ERROR] JB JSON 구조를 해석할 수 없습니다.")
        return

    global_out = []
    share_out = []

    for r in rows:
        month = r.get("month") or r.get("date")
        if not month:
            continue

        # 통화별 BTC 거래량
        krw = float(r.get("KRW", 0) or 0)
        jpy = float(r.get("JPY", 0) or 0)
        usd = float(r.get("USD", 0) or 0)
        eur = float(r.get("EUR", 0) or 0)
        cny = float(r.get("CNY", 0) or 0)

        # 글로벌 합 (JB 표본 기준)
        global_btc = krw + jpy + usd + eur + cny

        # 한국 점유율
        share_pct = (krw / global_btc) * 100 if global_btc > 0 else 0

        # 1) 글로벌 유동성 파일
        global_out.append({
            "month": month,
            "global_btc_volume": round(global_btc, 2),
            "krw_btc_volume": round(krw, 2),
            "notes": "JBBitcoin sample-based monthly spot BTC volume (BTC units)"
        })

        # 2) 한국 점유율 파일
        share_out.append({
            "month": month,
            "krw_btc_volume": round(krw, 2),
            "global_btc_volume": round(global_btc, 2),
            "share_pct": round(share_pct, 2),
            "notes": "KRW BTC volume share among JBBitcoin covered currencies"
        })

    # 저장
    OUT_GLOBAL.parent.mkdir(parents=True, exist_ok=True)

    with open(OUT_GLOBAL, "w", encoding="utf-8") as f:
        json.dump(global_out, f, indent=2, ensure_ascii=False)

    with open(OUT_SHARE, "w", encoding="utf-8") as f:
        json.dump(share_out, f, indent=2, ensure_ascii=False)

    print("[OK] 생성 완료")
    print(" -", OUT_GLOBAL)
    print(" -", OUT_SHARE)


if __name__ == "__main__":
    main()
