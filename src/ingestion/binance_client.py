"""
src/ingestion/binance_client.py
=====================================================================
TẦNG INGESTION — Thu thập dữ liệu thô (raw OHLCV) từ Binance.

Trách nhiệm DUY NHẤT của module này: lấy dữ liệu về, KHÔNG xử lý,
KHÔNG tính toán chỉ báo. Tách biệt trách nhiệm giúp dễ debug: nếu dữ
liệu sai, ta biết chắc lỗi nằm ở đây, không phải ở tầng Processing.

Hai chế độ lấy dữ liệu:
1. fetch_historical() — BATCH: lấy toàn bộ lịch sử, chỉ chạy 1 LẦN
   khi khởi tạo hệ thống (--mode init)
2. fetch_incremental() — chỉ lấy N nến gần nhất, chạy LẶP LẠI mỗi giờ
   (--mode update), tối ưu vì không tải lại toàn bộ lịch sử mỗi lần
"""

from __future__ import annotations

import pandas as pd
from binance.client import Client

from config.settings import settings
from src.utils.logger import get_logger
from src.utils.retry import with_retry

log = get_logger(__name__)

# Map string "1h", "1d"... sang hằng số của thư viện python-binance
_INTERVAL_MAP = {
    "1m": Client.KLINE_INTERVAL_1MINUTE,
    "5m": Client.KLINE_INTERVAL_5MINUTE,
    "15m": Client.KLINE_INTERVAL_15MINUTE,
    "1h": Client.KLINE_INTERVAL_1HOUR,
    "4h": Client.KLINE_INTERVAL_4HOUR,
    "1d": Client.KLINE_INTERVAL_1DAY,
}

_RAW_COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "trades",
    "taker_buy_base", "taker_buy_quote", "ignore",
]


class BinanceIngestion:
    """
    Bọc python-binance Client thành 1 class có retry + logging + chuẩn hóa
    output, để phần còn lại của hệ thống không cần biết chi tiết Binance
    trả dữ liệu ở format gì.
    """

    def __init__(self) -> None:
        # Không cần API key để lấy public market data (klines)
        self._client = Client(settings.binance_api_key, settings.binance_api_secret)
        self._symbol = settings.symbol
        self._interval = _INTERVAL_MAP.get(settings.interval)
        if self._interval is None:
            raise ValueError(
                f"Interval '{settings.interval}' không hợp lệ. "
                f"Chọn 1 trong: {list(_INTERVAL_MAP.keys())}"
            )
        log.info(f"BinanceIngestion khởi tạo: symbol={self._symbol}, interval={settings.interval}")

    # -- Public API ------------------------------------------------------

    @with_retry(max_attempts=3)
    def fetch_historical(self, start_date: str | None = None) -> pd.DataFrame:
        """
        BATCH: Lấy TOÀN BỘ lịch sử từ start_date đến hiện tại.
        Chỉ nên gọi khi khởi tạo hệ thống lần đầu — với BTC 1h từ 2020
        sẽ trả về ~50.000 dòng, mất khoảng 1-2 phút.
        """
        start = start_date or settings.history_start_date
        log.info(f"[BATCH] Đang lấy lịch sử {self._symbol} từ {start}...")

        raw = self._client.get_historical_klines(
            symbol=self._symbol,
            interval=self._interval,
            start_str=start,
        )
        df = self._to_dataframe(raw)
        log.info(f"[BATCH] Lấy được {len(df):,} nến ({df['timestamp'].min()} → {df['timestamp'].max()})")
        return df

    @with_retry(max_attempts=3)
    def fetch_incremental(self, lookback: int | None = None) -> pd.DataFrame:
        """
        INCREMENTAL: Chỉ lấy N nến gần nhất (mặc định lấy theo config).
        Dùng cho cron job chạy mỗi giờ — lookback=200 đủ để tầng Processing
        có warmup period tính RSI/MACD/EMA chính xác, KHÔNG cần tải lại
        toàn bộ lịch sử (tối ưu băng thông + thời gian chạy).
        """
        n = lookback or settings.incremental_lookback
        log.info(f"[INCREMENTAL] Đang lấy {n} nến gần nhất của {self._symbol}...")

        raw = self._client.get_klines(
            symbol=self._symbol,
            interval=self._interval,
            limit=n,
        )
        df = self._to_dataframe(raw)
        log.info(f"[INCREMENTAL] Lấy được {len(df):,} nến, mới nhất: {df['timestamp'].max()}")
        return df

    # -- Private helpers ---------------------------------------------------

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
