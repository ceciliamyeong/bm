# BM20 – Daily Table → Index (Quarterly Rebalance)
구조: archive/**/bm20_daily_data_YYYY-MM-DD.csv → bm20_from_daily_csv.py → export_json.py → docs/ (Pages)
- 분기 리밸런스, BTC 30%/ETH 20% 상한, KR 상장 보너스 1.3, 스테이블/랩트 토큰 제외
- 필요시 --rebalance monthly / --weights-source csv 등으로 조정

## 실행
python bm20_from_daily_csv.py --archive ./archive --out ./out --rebalance quarterly --weights-source rules --listed-bonus 1.3 --cap BTC:0.30 --cap ETH:0.20 --map bm20_map_btc30.csv
python export_json.py out/bm20_index_from_csv.csv site
