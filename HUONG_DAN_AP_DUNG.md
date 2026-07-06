# HƯỚNG DẪN ÁP DỤNG — Tính năng P2P Spread + Tax Estimate

## Cách áp dụng

Giải nén zip này, copy từng file vào ĐÚNG vị trí tương ứng trong project
`C:\Users\ASUS\Desktop\bigdata\`, ghi đè (overwrite) các file đã có sẵn:

```
config/settings.py          → GHI ĐÈ (đã thêm field P2P/Tax)
src/ingestion/p2p_client.py → FILE MỚI
src/processing/tax_calculator.py → FILE MỚI
src/storage/schemas.py      → GHI ĐÈ (đã thêm SCHEMA_P2P_SPREAD)
src/storage/p2p_storage.py  → FILE MỚI
src/pipeline/orchestrator.py → GHI ĐÈ (đã tích hợp gọi P2P mỗi lần update)
backend/main.py             → GHI ĐÈ (đã thêm 2 endpoint mới)
```

Không cần sửa gì thêm ở `requirements.txt` — toàn bộ tính năng mới chỉ
dùng lại `requests`, `pandas`, `google-cloud-bigquery`, `tenacity`,
`fastapi` đã có sẵn trong project.

## Kiểm tra sau khi copy xong

```powershell
cd C:\Users\ASUS\Desktop\bigdata
python -c "from src.ingestion.p2p_client import P2PIngestion; p=P2PIngestion(); [print(r) for r in p.fetch_spread()]"
```

- `fetch_spread()` giờ trả về **list gồm 2 dòng** (SELL và BUY) trong 1 lần
  gọi, mỗi dòng đã lấy trung bình 3 mẫu (mặc định, cách nhau 3 giây) —
  bạn sẽ thấy log chạy mất ~9-10 giây (do phải chờ giữa các mẫu), đây
  là điều BÌNH THƯỜNG, không phải bug.
- Nếu in ra 2 dict, mỗi dict có `p2p_price`, `p2p_price_min`,
  `p2p_price_max`, `samples`, `market_price`, `spread_pct` → THÀNH CÔNG

```powershell
python -c "from src.processing.tax_calculator import TaxCalculator; print(TaxCalculator.calc_tax_vn(100_000_000))"
```
→ nên in ra ngay, không cần mạng, không cần BigQuery.

## Chạy pipeline update thử (cần đã cấu hình BigQuery)

```powershell
python scripts/run_pipeline.py --mode update
```
Log sẽ có thêm dòng `[p2p] ...` — nếu P2P lỗi, pipeline OHLCV chính
VẪN chạy tiếp bình thường (đã cố tình tách try/except riêng).

## Chạy Backend thử endpoint mới

```powershell
uvicorn backend.main:app --reload --port 8000
```
Rồi mở trình duyệt:
- http://localhost:8000/api/p2p-spread
- http://localhost:8000/api/tax-estimate?amount=100000000&country=VN
- http://localhost:8000/api/tax-estimate?amount=60000&country=US&holding_days=400

## (Tuỳ chọn) Thêm vào file .env nếu muốn tuỳ chỉnh

Không bắt buộc — tất cả đã có giá trị mặc định hợp lý. Chỉ thêm nếu
muốn đổi:

```env
P2P_ASSET=USDT
P2P_FIAT=VND
P2P_ROWS=10
FX_API_URL=https://open.er-api.com/v6/latest/USD
TAX_VN_RATE=0.001
```

## Lưu ý quan trọng

1. **Binance P2P là API không chính thức** — chưa test được thật trong
   sandbox của Claude do giới hạn network egress whitelist. Bạn PHẢI tự
   chạy thử trên máy mình để xác nhận. Đã thêm sẵn header User-Agent/
   Referer/Origin để né anti-bot cơ bản của Binance.
2. Nếu Binance chặn IP khi deploy lên Render (server nước ngoài) — y hệt
   lý do project đã né `api.binance.com` cho phần OHLCV — báo lại để đổi
   nguồn dữ liệu khác (VD: CoinGecko, hoặc API tổng hợp giá P2P khác).
3. Lỗi P2P KHÔNG làm gãy pipeline OHLCV chính — đã cố ý tách riêng
   try/except trong `orchestrator.py`.
4. `calc_tax_us()` chỉ mang tính THAM KHẢO/SO SÁNH — bracket US dùng số
   liệu 2025, và short-term chỉ là ước tính đơn giản hoá, không chính
   xác tuyệt đối như luật thuế thật (cần nêu rõ giới hạn này trong báo
   cáo môn học).
