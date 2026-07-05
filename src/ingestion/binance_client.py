"""
src/ingestion/binance_client.py
=====================================================================
TẦNG INGESTION — Thu thập dữ liệu thô (raw OHLCV) từ Binance.

Trách nhiệm DUY NHẤT của module này: lấy dữ liệu về, KHÔNG xử lý,
KHÔNG tính toán chỉ báo. Tách biệt trách nhiệm giúp dễ debug: nếu dữ
liệu sai, ta biết chắc lỗi nằm ở đây, không phải ở tầng Processing.

QUAN TRỌNG — vì sao dùng "requests" thay vì thư viện "python-binance":
Thư viện python-binance gọi domain chính api.binance.com, domain này
áp dụng chính sách chặn theo vùng địa lý (geo-restriction) cho một số
khu vực (VD: Mỹ) theo điều khoản dịch vụ của Binance. Máy chủ GitHub
Actions đặt tại Mỹ nên bị chặn ngay khi khởi tạo Client() (nó tự ping
kiểm tra eligibility).

Binance có riêng 1 domain "data-api.binance.vision" — CHỈ phục vụ dữ
liệu thị trường công khai (không cần đăng nhập, không giao dịch được),
không áp policy chặn vùng khắt khe như domain chính. Gọi thẳng REST
endpoint này bằng "requests" để pipeline chạy được cả trên máy local
lẫn trên GitHub Actions (server đặt ở nhiều vùng khác nhau).

Hai chế độ lấy dữ liệu:
1. fetch_historical() — BATCH: lấy toàn bộ lịch sử, chỉ chạy 1 LẦN
   khi khởi tạo hệ thống (--mode init). Tự động phân trang (mỗi lần
   API chỉ trả tối đa 1000 nến).
2. fetch_incremental() — chỉ lấy N nến gần nhất, chạy LẶP LẠI mỗi giờ
   (--mode update).
"""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import requests

from config.settings import settings
from src.utils.logger import get_logger
from src.utils.retry import with_retry

log = get_logger(__name__)

# Domain dữ liệu công khai — không bị geo-restriction như api.binance.com
_BASE_URL = "https://data-api.binance.vision/api/v3/klines"

# Binance giới hạn tối đa 1000 nến mỗi lần gọi — bắt buộc phải phân trang
# khi lấy lịch sử dài (VD: từ 2020 → hiện tại có ~57.000 nến 1h)
_MAX_LIMIT_PER_CALL = 1000

_RAW_COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "trades",
    "taker_buy_base", "taker_buy_quote", "ignore",
]


class BinanceIngestion:
    """
    Client tự viết gọi REST API công khai của Binance (qua "requests"),
    có retry + logging + chuẩn hóa output, để phần còn lại của hệ thống
    không cần biết chi tiết Binance trả dữ liệu ở format gì.
    """

    def __init__(self) -> None:
        self._symbol = settings.symbol
        self._interval = settings.interval  # Domain vision dùng string "1h" trực tiếp
        self._session = requests.Session()
        log.info(f"BinanceIngestion khởi tạo: symbol={self._symbol}, interval={self._interval} "
                 f"(endpoint công khai: {_BASE_URL})")

    # -- Public API ------------------------------------------------------

    def fetch_historical(self, start_date: str | None = None) -> pd.DataFrame:
        """
        BATCH: Lấy TOÀN BỘ lịch sử từ start_date đến hiện tại.
        Tự động phân trang vì mỗi lần gọi API chỉ trả tối đa 1000 nến.
        Chỉ nên gọi khi khởi tạo hệ thống lần đầu.
        """
        start = start_date or settings.history_start_date
        start_ms = int(pd.Timestamp(start, tz="UTC").timestamp() * 1000)
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        log.info(f"[BATCH] Đang lấy lịch sử {self._symbol} từ {start}...")

        all_klines: list = []
        current_start = start_ms
        page = 0
        while current_start < now_ms:
            page += 1
            batch = self._call_klines(start_time_ms=current_start, limit=_MAX_LIMIT_PER_CALL)
            if not batch:
                break
            all_klines.extend(batch)
            # Trang tiếp theo bắt đầu ngay sau close_time của nến cuối cùng
            current_start = int(batch[-1][6]) + 1
            if page % 10 == 0:
                log.info(f"[BATCH] Đã lấy {page} trang ({len(all_klines):,} nến)...")

        df = self._to_dataframe(all_klines)
        log.info(f"[BATCH] Lấy được {len(df):,} nến ({df['timestamp'].min()} → {df['timestamp'].max()})")
        return df

    def fetch_incremental(self, lookback: int | None = None) -> pd.DataFrame:
        """
        INCREMENTAL: Chỉ lấy N nến gần nhất (mặc định lấy theo config).
        Dùng cho cron job chạy mỗi giờ.
        """
        n = lookback or settings.incremental_lookback
        log.info(f"[INCREMENTAL] Đang lấy {n} nến gần nhất của {self._symbol}...")

        raw = self._call_klines(limit=n)
        df = self._to_dataframe(raw)
        log.info(f"[INCREMENTAL] Lấy được {len(df):,} nến, mới nhất: {df['timestamp'].max()}")
        return df

    # -- Private helpers ---------------------------------------------------

    @with_retry(max_attempts=3)
    def _call_klines(self, limit: int, start_time_ms: int | None = None) -> list:
        """Gọi trực tiếp REST endpoint /api/v3/klines, trả về raw list-of-lists."""
        params = {
            "symbol": self._symbol,
            "interval": self._interval,
            "limit": limit,
        }
        if start_time_ms is not None:
            params["startTime"] = start_time_ms

        response = self._session.get(_BASE_URL, params=params, timeout=15)
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _to_dataframe(raw_klines: list) -> pd.DataFrame:
        """
        Chuẩn hóa raw klines (list-of-lists) từ Binance sang DataFrame
        sạch, kiểu dữ liệu đúng, chỉ giữ cột cần thiết.
        """
        if not raw_klines:
            log.warning("Binance trả về danh sách rỗng — không có dữ liệu mới")
            return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume", "trades"])

        df = pd.DataFrame(raw_klines, columns=_RAW_COLUMNS)

        float_cols = ["open", "high", "low", "close", "volume"]
        df[float_cols] = df[float_cols].astype(float)
        df["trades"] = df["trades"].astype(int)
        df["timestamp"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)

        df = df[["timestamp", "open", "high", "low", "close", "volume", "trades"]]
        df = df.sort_values("timestamp").drop_duplicates(subset="timestamp").reset_index(drop=True)
        return df
