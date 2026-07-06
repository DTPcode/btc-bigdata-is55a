"""
src/storage/schemas.py
=====================================================================
ĐỊNH NGHĨA SCHEMA BigQuery — tách riêng khỏi logic upload/query.

Tại sao tách riêng file này?
- Khi cần thêm cột (VD: thêm chỉ báo mới), chỉ cần sửa ở ĐÂY, không
  phải mò trong logic upload phức tạp
- Schema là "hợp đồng" giữa Processing (tạo DataFrame) và Storage
  (lưu vào BigQuery) — để riêng giúp thấy rõ hợp đồng đó là gì
"""

from google.cloud import bigquery

# Schema cho table OHLCV + chỉ báo kỹ thuật
# QUAN TRỌNG: thứ tự và tên cột phải khớp với DataFrame do
# IndicatorEngine.calculate() tạo ra trong src/processing/indicators.py
SCHEMA_OHLCV = [
    bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED",
                          description="Thời điểm mở nến (UTC)"),
    bigquery.SchemaField("open", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("high", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("low", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("close", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("volume", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("trades", "INT64", mode="REQUIRED"),

    # Chỉ báo kỹ thuật — nullable vì vài dòng đầu có thể chưa đủ warmup
    bigquery.SchemaField("rsi_14", "FLOAT64"),
    bigquery.SchemaField("macd", "FLOAT64"),
    bigquery.SchemaField("macd_signal", "FLOAT64"),
    bigquery.SchemaField("macd_hist", "FLOAT64"),
    bigquery.SchemaField("bb_upper", "FLOAT64"),
    bigquery.SchemaField("bb_mid", "FLOAT64"),
    bigquery.SchemaField("bb_lower", "FLOAT64"),
    bigquery.SchemaField("bb_width", "FLOAT64"),
    bigquery.SchemaField("ema_20", "FLOAT64"),
    bigquery.SchemaField("ema_50", "FLOAT64"),
    bigquery.SchemaField("ema_200", "FLOAT64"),
    bigquery.SchemaField("atr_14", "FLOAT64"),
    bigquery.SchemaField("stoch_k", "FLOAT64"),
    bigquery.SchemaField("stoch_d", "FLOAT64"),
    bigquery.SchemaField("vol_ma_20", "FLOAT64"),

    # Metadata pipeline - phục vụ debug, biết dòng nào được ghi lúc nào
    bigquery.SchemaField("_loaded_at", "TIMESTAMP",
                          description="Thời điểm pipeline ghi dòng này vào BigQuery"),
]

# Cột dùng để CLUSTER table — BigQuery sẽ sắp xếp dữ liệu vật lý theo cột
# này, giúp các query filter theo timestamp (đã partition theo ngày) VÀ
# sort theo thời gian chạy nhanh hơn, scan ít dữ liệu hơn.
CLUSTERING_FIELDS = ["timestamp"]

# Schema cho bảng lịch sử % chênh lệch giá P2P (Binance P2P vs tỷ giá
# quốc tế) — mỗi dòng là 1 lần đo (mỗi lần chạy --mode update, ~1h/lần).
# Bảng này KHÔNG dùng cơ chế UPSERT/DDL-swap như OHLCV, vì đây là dữ
# liệu dạng LOG theo thời gian (mỗi lần đo là 1 sự kiện độc lập, không
# có khái niệm "sửa lại dòng cũ") — chỉ cần APPEND đơn giản.
SCHEMA_P2P_SPREAD = [
    bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED",
                          description="Thời điểm đo giá P2P (UTC)"),
    bigquery.SchemaField("asset", "STRING", mode="REQUIRED",
                          description="Tài sản quy đổi, VD: USDT"),
    bigquery.SchemaField("fiat", "STRING", mode="REQUIRED",
                          description="Tiền pháp định, VD: VND"),
    bigquery.SchemaField("trade_type", "STRING", mode="REQUIRED",
                          description="SELL hoặc BUY (chiều giao dịch trên P2P)"),
    bigquery.SchemaField("p2p_price", "FLOAT64", mode="REQUIRED",
                          description="Giá trung bình (của nhiều mẫu) top N quảng cáo trên Binance P2P"),
    bigquery.SchemaField("p2p_price_min", "FLOAT64",
                          description="Giá thấp nhất trong các mẫu đã lấy (biến động trong lần chạy)"),
    bigquery.SchemaField("p2p_price_max", "FLOAT64",
                          description="Giá cao nhất trong các mẫu đã lấy (biến động trong lần chạy)"),
    bigquery.SchemaField("samples", "INT64",
                          description="Số lần lấy mẫu thực tế đã dùng để tính trung bình"),
    bigquery.SchemaField("market_price", "FLOAT64", mode="REQUIRED",
                          description="Tỷ giá USD/VND quốc tế tại thời điểm đo"),
    bigquery.SchemaField("spread_pct", "FLOAT64", mode="REQUIRED",
                          description="% chênh lệch = (market - p2p) / market * 100"),
    bigquery.SchemaField("_loaded_at", "TIMESTAMP",
                          description="Thời điểm pipeline ghi dòng này vào BigQuery"),
]
