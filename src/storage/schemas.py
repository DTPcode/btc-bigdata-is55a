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
