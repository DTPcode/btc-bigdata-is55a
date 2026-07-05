# BTC Big Data Pipeline
### Môn IS55A — Công nghệ Dịch vụ Tài chính

Hệ thống thu thập, xử lý và lưu trữ dữ liệu Bitcoin theo kiến trúc
**Layered Architecture** (Ingestion → Processing → Storage), tối ưu
để chạy hoàn toàn miễn phí trên Binance API + Google BigQuery.

---

## 1. Cây thư mục

```
bigdata/
│
├── config/
│   ├── __init__.py
│   └── settings.py              # ⚙️ CẤU HÌNH TẬP TRUNG — mọi thông số
│                                 #    (symbol, project_id...) đọc từ đây
│
├── src/
│   ├── ingestion/                # 📥 TẦNG 1: Thu thập dữ liệu thô
│   │   ├── __init__.py
│   │   └── binance_client.py     #    Gọi Binance API, có retry
│   │
│   ├── processing/                # ⚙️ TẦNG 2: Xử lý & tính chỉ báo
│   │   ├── __init__.py
│   │   └── indicators.py         #    DataQualityChecker + IndicatorEngine
│   │
│   ├── storage/                   # 🗄️ TẦNG 3: Lưu trữ (Data Warehouse)
│   │   ├── __init__.py
│   │   ├── schemas.py             #    Định nghĩa schema BigQuery
│   │   └── bigquery_client.py     #    Upload/query, partition, chunking
│   │
│   ├── pipeline/                  # 🔗 TẦNG ĐIỀU PHỐI
│   │   ├── __init__.py
│   │   └── orchestrator.py        #    Nối 3 tầng trên lại, 3 mode chạy
│   │
│   └── utils/                     # 🔧 TIỆN ÍCH DÙNG CHUNG
│       ├── __init__.py
│       ├── logger.py              #    Logging tập trung (console + file)
│       └── retry.py               #    Retry decorator (exponential backoff)
│
├── scripts/
│   └── run_pipeline.py            # 🚀 ĐIỂM VÀO — chạy pipeline từ đây
│
├── tests/
│   ├── __init__.py
│   └── test_indicators.py         # ✅ Unit test tầng Processing
│
├── .github/workflows/
│   └── update_pipeline.yml        # 🤖 GitHub Actions — cron mỗi 1 giờ
│
├── logs/                          # Log file tự sinh khi chạy (gitignored)
├── data/                          # Nơi lưu file tạm nếu cần (gitignored)
│
├── .env.example                   # Template biến môi trường
├── .gitignore
├── requirements.txt
└── README.md                      # File này
```

### Vì sao chia theo layer như vậy?

| Tầng | Trách nhiệm | KHÔNG làm gì |
|---|---|---|
| `ingestion/` | Lấy dữ liệu thô từ Binance | Không tính toán, không biết BigQuery tồn tại |
| `processing/` | Làm sạch + tính chỉ báo kỹ thuật | Không biết dữ liệu từ đâu tới, không biết lưu đi đâu |
| `storage/` | Lưu và truy vấn BigQuery | Không biết dữ liệu được tính toán như thế nào |
| `pipeline/` | Nối 3 tầng trên theo đúng thứ tự | Không chứa logic nghiệp vụ chi tiết |

**Lợi ích:** khi có lỗi, nhìn log biết ngay lỗi ở tầng nào. Khi cần mở
rộng (VD: thêm nguồn dữ liệu Coinbase, hoặc đổi sang PostgreSQL), chỉ
sửa 1 tầng, không ảnh hưởng các tầng khác.

---

## 2. Cài đặt môi trường ảo (Virtual Environment)

### Vì sao cần venv?
Virtual environment tạo ra một "hộp cách ly" chứa Python + package
riêng cho project này, không ảnh hưởng đến Python hệ thống hoặc các
project khác trên máy. Bắt buộc phải dùng khi làm việc nhóm để đảm bảo
mọi người dùng đúng version package giống nhau.

### Windows

```powershell
# 1. Di chuyển vào thư mục project
cd bigdata

# 2. Tạo virtual environment (chỉ làm 1 lần)
python -m venv venv

# 3. Kích hoạt venv (làm mỗi khi mở terminal mới)
venv\Scripts\activate

# Thấy (venv) xuất hiện đầu dòng lệnh là đã kích hoạt thành công
# (venv) PS C:\...\bigdata>

# 4. Cài đặt toàn bộ thư viện
pip install -r requirements.txt

# 5. Khi xong việc, tắt venv
deactivate
```

### macOS / Linux

```bash
# 1. Di chuyển vào thư mục project
cd bigdata

# 2. Tạo virtual environment (chỉ làm 1 lần)
python3 -m venv venv

# 3. Kích hoạt venv (làm mỗi khi mở terminal mới)
source venv/bin/activate

# Thấy (venv) xuất hiện đầu dòng lệnh là đã kích hoạt thành công
# (venv) user@machine bigdata %

# 4. Cài đặt toàn bộ thư viện
pip install -r requirements.txt

# 5. Khi xong việc, tắt venv
deactivate
```

### Kiểm tra cài đặt thành công

```bash
# Vẫn trong venv đã activate
python -c "import pandas, pandas_ta, google.cloud.bigquery, binance; print('✅ Tất cả thư viện đã cài đúng')"
```

Nếu báo lỗi `ModuleNotFoundError`, chạy lại `pip install -r requirements.txt`
và đảm bảo đã activate venv (thấy `(venv)` ở đầu dòng lệnh).

---

## 3. Cấu hình

```bash
# Copy file template
cp .env.example .env      # Mac/Linux
copy .env.example .env    # Windows

# Mở .env và điền:
# - GCP_PROJECT_ID (từ Google Cloud Console)
# - GOOGLE_APPLICATION_CREDENTIALS (đường dẫn tới credentials.json)
```

Cách lấy `credentials.json`:
1. Vào [console.cloud.google.com](https://console.cloud.google.com) → tạo project mới
2. **APIs & Services** → Enable **BigQuery API**
3. **IAM & Admin** → **Service Accounts** → **Create Service Account**
   → Role: **BigQuery Admin**
4. Vào service account vừa tạo → **Keys** → **Add Key** → **JSON** → tải về
5. Đổi tên file tải về thành `credentials.json`, đặt vào thư mục gốc `bigdata/`

---

## 4. Chạy hệ thống

```bash
# Đảm bảo đã activate venv trước!

# Bước 1: Test — kiểm tra Ingestion + Processing, KHÔNG cần BigQuery
python scripts/run_pipeline.py --mode test

# Bước 2: Init — load toàn bộ lịch sử lên BigQuery (chỉ chạy 1 LẦN)
python scripts/run_pipeline.py --mode init

# Bước 3: Update — mô phỏng cron job (dùng để test trước khi đẩy GitHub Actions)
python scripts/run_pipeline.py --mode update
```

### Chạy unit test

```bash
pip install pytest    # nếu chưa có (đã có trong requirements.txt)
pytest tests/ -v
```

---

## 5. Tự động hóa với GitHub Actions

Sau khi `--mode init` chạy thành công trên máy local, đẩy code lên
GitHub và cấu hình secrets để pipeline tự chạy mỗi giờ:

**Settings → Secrets and variables → Actions → New repository secret**

| Secret | Giá trị |
|---|---|
| `GCP_PROJECT_ID` | Project ID trên Google Cloud |
| `GOOGLE_CREDENTIALS_JSON` | Toàn bộ nội dung file `credentials.json` |

Workflow tại `.github/workflows/update_pipeline.yml` sẽ tự chạy
`--mode update` mỗi đầu giờ.

---

## 6. Debug & Mở rộng hệ thống

### Khi gặp lỗi, tìm đúng file theo log prefix

Log luôn ghi rõ **tên module** gây ra log đó (xem `logs/pipeline_YYYYMMDD.log`):

| Log hiện tên module | File cần soát |
|---|---|
| `src.ingestion.binance_client` | `src/ingestion/binance_client.py` |
| `src.processing.indicators` | `src/processing/indicators.py` |
| `src.storage.bigquery_client` | `src/storage/bigquery_client.py` |
| `src.pipeline.orchestrator` | `src/pipeline/orchestrator.py` |

### Muốn thêm chỉ báo kỹ thuật mới (VD: OBV)
1. Mở `src/processing/indicators.py` → thêm vào `IndicatorEngine.calculate()`
2. Mở `src/storage/schemas.py` → thêm `bigquery.SchemaField("obv", "FLOAT64")`
3. Chạy `pytest tests/ -v` để đảm bảo không phá vỡ logic cũ

### Muốn đổi coin (VD: BTC → ETH)
Chỉ cần sửa `.env`:
```
SYMBOL=ETHUSDT
```
Không cần sửa code — vì `table_ohlcv` trong `config/settings.py` tự
sinh tên table theo symbol.

### Muốn đổi timeframe (VD: 1h → 4h)
Sửa `.env`:
```
INTERVAL=4h
```
(Đã hỗ trợ sẵn: 1m, 5m, 15m, 1h, 4h, 1d — xem `_INTERVAL_MAP` trong
`src/ingestion/binance_client.py`)

### Muốn thêm nguồn dữ liệu mới (VD: Coinbase)
Tạo file mới `src/ingestion/coinbase_client.py` theo cùng pattern của
`binance_client.py` (có `fetch_historical()` và `fetch_incremental()`
trả về DataFrame cùng format), rồi đổi import trong
`src/pipeline/orchestrator.py`. Các tầng Processing/Storage không cần
sửa gì.

---

## 7. Các điểm tối ưu hóa đã áp dụng

| Kỹ thuật | Áp dụng ở đâu | Lợi ích |
|---|---|---|
| **Retry + Exponential Backoff** | `src/utils/retry.py` | Không fail pipeline vì lỗi mạng tạm thời |
| **Batch Load (không Streaming)** | `src/storage/bigquery_client.py` | Miễn phí hoàn toàn trên BigQuery |
| **Chunked Upload** | `BigQueryStorage.upload()` | Tránh timeout khi upload dữ liệu lớn |
| **Incremental Load** | `BigQueryStorage.get_latest_timestamp()` | Không tải lại dữ liệu đã có |
| **Partitioning theo ngày** | `BigQueryStorage.ensure_dataset_and_table()` | Giảm lượng dữ liệu scan mỗi query |
| **Clustering theo timestamp** | `src/storage/schemas.py` | Query filter/sort theo thời gian nhanh hơn |
| **Lazy initialization** | `PipelineOrchestrator._get_storage()` | Mode `test` không cần credentials BigQuery |
| **Centralized config** | `config/settings.py` | Đổi coin/timeframe không cần sửa code |
| **Centralized logging** | `src/utils/logger.py` | Debug dễ qua file log, không dùng print() rời rạc |
| **Data Quality Checks** | `src/processing/indicators.py` | Phát hiện dữ liệu lỗi trước khi tính chỉ báo sai |

---

## 8. Giới hạn Free Tier cần nhớ

| Dịch vụ | Free tier | Dự án dùng | An toàn? |
|---|---|---|---|
| BigQuery Storage | 10 GB/tháng | ~15-20 MB | ✅ Dư nhiều |
| BigQuery Query | 1 TB/tháng | ~1-5 GB/tháng | ✅ Dư nhiều |
| GitHub Actions | 2.000 phút/tháng | ~720 phút/tháng | ✅ Còn dư ~1.280 phút |
| Binance API | Không giới hạn (public data) | — | ✅ |

**Quy tắc bắt buộc để giữ mọi thứ miễn phí:**
- KHÔNG đổi cron GitHub Actions xuống dưới 1 giờ
- Mọi query BigQuery LUÔN có `WHERE timestamp >=` và `LIMIT`
- `--mode init` chỉ chạy 1 LẦN, không chạy lặp lại
