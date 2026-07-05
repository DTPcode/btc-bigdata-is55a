"""
src/utils/retry.py
=====================================================================
RETRY DECORATOR — tối ưu độ ổn định khi gọi API bên ngoài.

Tại sao cần?
- Binance API / BigQuery đôi khi timeout hoặc rate-limit tạm thời
  (lỗi mạng, server bận) — KHÔNG phải lỗi code, chỉ cần thử lại là được
- Nếu không có retry, pipeline chạy trên GitHub Actions sẽ FAIL toàn bộ
  chỉ vì 1 lần mất kết nối tạm thời trong 1-2 giây
- Exponential backoff (chờ tăng dần: 2s → 4s → 8s) tránh spam liên tục
  vào API khi nó đang bị quá tải, đồng thời tăng khả năng thành công

Cách dùng:
    from src.utils.retry import with_retry

    @with_retry(max_attempts=3)
    def call_binance_api():
        ...
"""

from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from src.utils.logger import get_logger

log = get_logger(__name__)


def with_retry(max_attempts: int = 3, min_wait: float = 2, max_wait: float = 15):
    """
    Decorator retry chuẩn cho toàn hệ thống.

    max_attempts: số lần thử tối đa (bao gồm lần đầu)
    min_wait/max_wait: giây chờ giữa các lần retry, tăng dần theo cấp số nhân

    Chỉ retry với lỗi liên quan mạng/kết nối (ConnectionError, TimeoutError,
    OSError) — KHÔNG retry lỗi logic (ValueError, KeyError...) vì thử lại
    cũng sẽ lỗi giống nhau, chỉ tốn thời gian.
    """
    return retry(
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential(multiplier=1, min=min_wait, max=max_wait),
        retry=retry_if_exception_type((ConnectionError, TimeoutError, OSError)),
        before_sleep=before_sleep_log(log, log_level=30),  # 30 = WARNING
        reraise=True,  # Sau khi hết số lần retry, ném lỗi thật ra ngoài
    )
