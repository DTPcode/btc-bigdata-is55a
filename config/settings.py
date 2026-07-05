"""
config/settings.py
=====================================================================
CẤU HÌNH TẬP TRUNG cho toàn bộ hệ thống Big Data.

Tại sao cần file này?
- Tránh hardcode giá trị (symbol, project_id...) rải rác trong nhiều file
- Khi cần đổi coin (BTC → ETH) hoặc đổi timeframe, chỉ sửa 1 nơi (.env)
- Dễ kiểm tra thiếu biến môi trường TRƯỚC KHI pipeline chạy, tránh lỗi
  giữa chừng gây tốn quota BigQuery/Binance

Mọi module khác PHẢI import settings từ đây, KHÔNG tự gọi os.getenv() riêng.
"""

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

# Tìm và load file .env ở thư mục gốc project (dù chạy từ đâu cũng tìm ra)
_ROOT_DIR = Path(__file__).resolve().parent.parent
load_dotenv(_ROOT_DIR / ".env")


@dataclass(frozen=True)
class Settings:
    """Toàn bộ cấu hình hệ thống, immutable sau khi khởi tạo (frozen=True)."""

    # -- Google Cloud / BigQuery --
    gcp_project_id: str
    bq_dataset_id: str
    bq_location: str
    credentials_path: str

    # -- Binance --
    binance_api_key: str
    binance_api_secret: str

    # -- Pipeline --
    symbol: str
    interval: str
    history_start_date: str
    incremental_lookback: int

    # -- Logging --
    log_level: str
    log_dir: str

    # -- Đường dẫn hệ thống --
    root_dir: Path

    @property
    def table_ohlcv(self) -> str:
        """Tên table OHLCV, tự sinh theo symbol+interval (VD: btcusdt_ohlcv_1h)."""
        return f"{self.symbol.lower()}_ohlcv_{self.interval}"

    @property
    def full_table_id(self) -> str:
        """Table ID đầy đủ dùng cho query BigQuery: project.dataset.table"""
        return f"{self.gcp_project_id}.{self.bq_dataset_id}.{self.table_ohlcv}"

    def validate_for_bigquery(self) -> None:
        """
        Kiểm tra config đủ để dùng BigQuery hay chưa.
        Gọi hàm này TRƯỚC khi khởi tạo BigQueryStorage — KHÔNG gọi lúc
        import settings, vì mode "test" (Ingestion+Processing only) không
        cần GCP.
        """
        missing = []
        if not self.gcp_project_id:
            missing.append("GCP_PROJECT_ID (chưa điền trong .env)")
        if not Path(self.credentials_path).exists():
            missing.append(f"file credentials tại '{self.credentials_path}' (không tồn tại)")
        if missing:
            raise EnvironmentError(
                "❌ Thiếu cấu hình BigQuery:\n   - " + "\n   - ".join(missing)
            )


def load_settings() -> Settings:
    """
    Tải toàn bộ config từ .env. KHÔNG raise lỗi cho biến optional như
    GCP_PROJECT_ID ở đây, vì có mode chạy pipeline không cần BigQuery
    (--mode test). Validate riêng bằng settings.validate_for_bigquery().
    """
    return Settings(
        gcp_project_id      = os.getenv("GCP_PROJECT_ID", ""),
        bq_dataset_id       = os.getenv("BQ_DATASET_ID", "btc_bigdata"),
        bq_location         = os.getenv("BQ_LOCATION", "US"),
        credentials_path    = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "./credentials.json"),

        binance_api_key     = os.getenv("BINANCE_API_KEY", ""),
        binance_api_secret  = os.getenv("BINANCE_API_SECRET", ""),

        symbol               = os.getenv("SYMBOL", "BTCUSDT"),
        interval              = os.getenv("INTERVAL", "1h"),
        history_start_date   = os.getenv("HISTORY_START_DATE", "2020-01-01"),
        incremental_lookback = int(os.getenv("INCREMENTAL_LOOKBACK", "200")),

        log_level = os.getenv("LOG_LEVEL", "INFO"),
        log_dir   = os.getenv("LOG_DIR", "./logs"),

        root_dir = _ROOT_DIR,
    )


# Singleton - import biến này ở mọi nơi thay vì gọi load_settings() nhiều lần
settings = load_settings()
