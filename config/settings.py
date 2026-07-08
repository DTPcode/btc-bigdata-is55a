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
# override=True: giá trị trong .env LUÔN ưu tiên hơn biến môi trường hệ
# thống đã set sẵn (tránh trường hợp máy có biến cũ sót lại gây nhầm lẫn
# rất khó debug, như GCP_PROJECT_ID bị set nhầm từ lần test trước)
_ROOT_DIR = Path(__file__).resolve().parent.parent
load_dotenv(_ROOT_DIR / ".env", override=True)


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

    # -- P2P Spread (Binance P2P vs tỷ giá quốc tế) --
    p2p_asset: str
    p2p_fiat: str
    p2p_rows: int
    p2p_samples: int
    p2p_sample_delay_sec: float
    fx_api_url: str

    # -- Tax (thuế giao dịch crypto) --
    tax_vn_rate: float

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
        incremental_lookback = int(os.getenv("INCREMENTAL_LOOKBACK", "500")),

        log_level = os.getenv("LOG_LEVEL", "INFO"),
        log_dir   = os.getenv("LOG_DIR", "./logs"),

        # Mặc định: USDT/VND, lấy trung bình 10 quảng cáo bán uy tín nhất
        p2p_asset  = os.getenv("P2P_ASSET", "USDT"),
        p2p_fiat   = os.getenv("P2P_FIAT", "VND"),
        p2p_rows   = int(os.getenv("P2P_ROWS", "10")),
        # Lấy mẫu nhiều lần/lần chạy để "trung bình hoá" giống tinh thần
        # OHLCV (tổng hợp theo thời gian) thay vì tin vào 1 điểm tức thời.
        # 3 mẫu x cách nhau 3s ~ 9s/chiều, không tốn quá nhiều thời gian
        # trong 1 lần chạy pipeline (--mode update chạy mỗi giờ).
        p2p_samples          = int(os.getenv("P2P_SAMPLES", "3")),
        p2p_sample_delay_sec = float(os.getenv("P2P_SAMPLE_DELAY_SEC", "3")),
        # API tỷ giá miễn phí, không cần key (open.er-api.com)
        fx_api_url = os.getenv("FX_API_URL", "https://open.er-api.com/v6/latest/USD"),

        # Thuế TNCN chuyển nhượng tài sản số VN — 0.1% trên giá trị bán,
        # hiệu lực từ 01/07/2026 theo Nghị định mới (xem README/báo cáo
        # để trích dẫn nguồn chính xác nếu cần nộp kèm)
        tax_vn_rate = float(os.getenv("TAX_VN_RATE", "0.001")),

        root_dir = _ROOT_DIR,
    )


# Singleton - import biến này ở mọi nơi thay vì gọi load_settings() nhiều lần
settings = load_settings()
