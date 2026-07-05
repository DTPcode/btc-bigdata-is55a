"""
src/utils/logger.py
=====================================================================
LOGGING TẬP TRUNG cho toàn bộ hệ thống.

Tại sao cần file này?
- Thay vì dùng print() rải rác (không biết log lúc nào, module nào),
  logger ghi rõ: thời gian, tên module, mức độ (INFO/WARNING/ERROR)
- Tự động ghi ra cả console (để xem khi debug) VÀ file log (để soát lại
  sau khi GitHub Actions chạy xong, hoặc để đưa vào báo cáo minh chứng)
- Dễ debug khi pipeline lỗi giữa đêm mà không ai theo dõi console

Cách dùng trong module khác:
    from src.utils.logger import get_logger
    log = get_logger(__name__)
    log.info("Đã lấy 500 nến từ Binance")
    log.error("Lỗi kết nối BigQuery", exc_info=True)
"""

import logging
import sys
from datetime import datetime
from pathlib import Path

from config.settings import settings

_LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)-30s | %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

_configured = False  # Đảm bảo chỉ setup handler 1 lần, tránh log bị lặp


def _setup_root_logger() -> None:
    """Cấu hình handler cho console + file, chạy 1 lần duy nhất."""
    global _configured
    if _configured:
        return

    log_dir = Path(settings.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"pipeline_{datetime.now().strftime('%Y%m%d')}.log"

    root = logging.getLogger()
    root.setLevel(settings.log_level.upper())

    formatter = logging.Formatter(_LOG_FORMAT, datefmt=_DATE_FORMAT)

    # Handler 1: In ra console (để xem khi chạy local / GitHub Actions log)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root.addHandler(console_handler)

    # Handler 2: Ghi ra file (để lưu vết, soát lỗi sau, đưa vào báo cáo)
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    _configured = True


def get_logger(name: str) -> logging.Logger:
    """
    Lấy logger cho 1 module cụ thể.
    name nên truyền là __name__ để log hiện đúng tên module gây ra log đó.
    """
    _setup_root_logger()
    return logging.getLogger(name)
