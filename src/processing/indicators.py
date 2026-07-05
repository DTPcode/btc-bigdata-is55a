"""
src/processing/indicators.py
=====================================================================
TẦNG PROCESSING — Biến dữ liệu thô thành dữ liệu có giá trị phân tích.

Gồm 2 trách nhiệm:
1. Data Quality Check — kiểm tra dữ liệu thô có hợp lệ không TRƯỚC KHI
   tính toán (tránh chỉ báo sai vì dữ liệu đầu vào bị lỗi/thiếu)
2. Feature Engineering — tính các chỉ báo kỹ thuật (RSI, MACD, BB, EMA...)

Tách 2 bước này rõ ràng vì: nếu chỉ báo ra kết quả vô lý (VD: RSI = 500),
ta biết ngay cần soát lại Data Quality trước, không phải soát công thức.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pandas_ta as ta

from src.utils.logger import get_logger

log = get_logger(__name__)

# Số nến tối thiểu cần có để tính chỉ báo dài nhất (EMA 200) cho ra kết quả
# đáng tin cậy. Nếu input ít hơn số này, chỉ báo sẽ có nhiều NaN ở đầu.
MIN_ROWS_REQUIRED = 200


class DataQualityChecker:
    """Kiểm tra và làm sạch dữ liệu OHLCV thô trước khi tính chỉ báo."""

    @staticmethod
    def check_and_clean(df: pd.DataFrame) -> pd.DataFrame:
        """
        Chạy tất cả các bước kiểm tra chất lượng, trả về DataFrame đã
        làm sạch. Log rõ WARNING nếu phát hiện vấn đề để dễ debug.
        """
        if df.empty:
            log.warning("DataQuality: DataFrame đầu vào rỗng")
            return df

        original_len = len(df)

        df = DataQualityChecker._drop_duplicate_timestamps(df)
        df = DataQualityChecker._drop_invalid_prices(df)
        df = DataQualityChecker._check_gaps(df)

        dropped = original_len - len(df)
        if dropped > 0:
            log.warning(f"DataQuality: đã loại bỏ {dropped} dòng không hợp lệ / trùng lặp")
        else:
            log.info(f"DataQuality: {original_len} dòng đều hợp lệ, không cần loại bỏ")

        return df

    @staticmethod
    def _drop_duplicate_timestamps(df: pd.DataFrame) -> pd.DataFrame:
        """Binance đôi khi trả về nến trùng timestamp ở API pagination."""
        return df.drop_duplicates(subset="timestamp", keep="last").reset_index(drop=True)

    @staticmethod
    def _drop_invalid_prices(df: pd.DataFrame) -> pd.DataFrame:
        """
        Loại bỏ dòng có giá <= 0 hoặc high < low (dữ liệu lỗi từ nguồn),
        đây là sanity check cơ bản nhưng bắt buộc trước khi tính chỉ báo.
        """
        mask_valid = (
            (df["open"] > 0) & (df["high"] > 0) &
            (df["low"] > 0) & (df["close"] > 0) &
            (df["high"] >= df["low"])
        )
        invalid_count = (~mask_valid).sum()
        if invalid_count > 0:
            log.warning(f"DataQuality: phát hiện {invalid_count} dòng giá không hợp lệ, đã loại bỏ")
        return df[mask_valid].reset_index(drop=True)

    @staticmethod
    def _check_gaps(df: pd.DataFrame) -> pd.DataFrame:
        """
        Kiểm tra có bị THIẾU nến giữa 2 mốc thời gian không (VD: Binance
        downtime). Chỉ CẢNH BÁO, không tự động điền — vì tự bơm dữ liệu
        giả vào time-series tài chính sẽ làm sai chỉ báo và mô hình dự đoán.
        """
        if len(df) < 2:
            return df
        expected_freq = pd.Timedelta(df["timestamp"].diff().mode()[0])
        gaps = df["timestamp"].diff() > expected_freq * 1.5
        gap_count = gaps.sum()
        if gap_count > 0:
            log.warning(
                f"DataQuality: phát hiện {gap_count} khoảng trống thời gian "
                f"(có thể do Binance downtime) — KHÔNG tự điền, giữ nguyên"
            )
        return df


def _get_col_by_prefix(df: pd.DataFrame, prefix: str) -> pd.Series:
    """
    Lấy cột trong DataFrame kết quả của pandas-ta theo TIỀN TỐ tên cột.
    Cần thiết vì các version khác nhau của pandas-ta đặt hậu tố số khác
    nhau (VD: BBU_20_2.0 vs BBU_20_2.0_2.0) — tránh code bị vỡ khi
    upgrade thư viện.
    """
    matches = [c for c in df.columns if c.startswith(prefix)]
    if not matches:
        raise KeyError(
            f"Không tìm thấy cột bắt đầu bằng '{prefix}' trong kết quả pandas-ta. "
            f"Các cột có sẵn: {list(df.columns)}"
        )
    return df[matches[0]]


class IndicatorEngine:
    """Tính toán bộ chỉ báo kỹ thuật chuẩn cho phân tích crypto."""

    @staticmethod
    def calculate(df: pd.DataFrame) -> pd.DataFrame:
        """
        Tính toàn bộ chỉ báo kỹ thuật. Yêu cầu tối thiểu MIN_ROWS_REQUIRED
        dòng để EMA(200) có ý nghĩa — nếu ít hơn, vẫn tính nhưng sẽ có
        NaN ở các chỉ báo dài, cảnh báo rõ trong log.
        """
        if len(df) < MIN_ROWS_REQUIRED:
            log.warning(
                f"Processing: chỉ có {len(df)} dòng, ít hơn mức tối thiểu "
                f"khuyến nghị ({MIN_ROWS_REQUIRED}) — EMA(200)/chỉ báo dài "
                f"có thể toàn NaN"
            )

        df = df.copy()
        close, high, low, volume = df["close"], df["high"], df["low"], df["volume"]

        # RSI — chỉ báo động lượng (momentum): >70 quá mua, <30 quá bán
        df["rsi_14"] = ta.rsi(close, length=14)

        # MACD — chỉ báo xu hướng, giao cắt giữa macd/signal là điểm vào/ra
        macd = ta.macd(close, fast=12, slow=26, signal=9)
        df["macd"] = macd["MACD_12_26_9"]
        df["macd_signal"] = macd["MACDs_12_26_9"]
        df["macd_hist"] = macd["MACDh_12_26_9"]

        # Bollinger Bands — đo biến động, giá chạm dải trên/dưới là tín hiệu
        # Lấy cột theo TIỀN TỐ (BBU_/BBM_/BBL_) thay vì tên cột đầy đủ, vì
        # các version khác nhau của pandas-ta đặt tên hậu tố khác nhau
        # (VD: "BBU_20_2.0" ở bản cũ vs "BBU_20_2.0_2.0" ở bản mới)
        bb = ta.bbands(close, length=20, std=2)
        df["bb_upper"] = _get_col_by_prefix(bb, "BBU_")
        df["bb_mid"] = _get_col_by_prefix(bb, "BBM_")
        df["bb_lower"] = _get_col_by_prefix(bb, "BBL_")
        df["bb_width"] = (df["bb_upper"] - df["bb_lower"]) / df["bb_mid"]

        # EMA — xu hướng theo nhiều khung: ngắn/trung/dài hạn
        df["ema_20"] = ta.ema(close, length=20)
        df["ema_50"] = ta.ema(close, length=50)
        df["ema_200"] = ta.ema(close, length=200)

        # ATR — đo biến động tuyệt đối, dùng đặt stop-loss
        df["atr_14"] = ta.atr(high, low, close, length=14)

        # Stochastic RSI — nhạy hơn RSI thường, bắt tín hiệu sớm hơn
        stoch = ta.stochrsi(close, length=14)
        if stoch is not None:
            df["stoch_k"] = _get_col_by_prefix(stoch, "STOCHRSIk_")
            df["stoch_d"] = _get_col_by_prefix(stoch, "STOCHRSId_")

        # Volume MA — xác nhận độ tin cậy của breakout (volume tăng kèm giá)
        df["vol_ma_20"] = ta.sma(volume, length=20)

        n_before = len(df)
        df = df.dropna(subset=["rsi_14", "macd", "bb_upper"]).reset_index(drop=True)
        n_after = len(df)
        log.info(f"Processing: tính xong {n_after} nến có đủ chỉ báo (loại {n_before - n_after} dòng warmup NaN)")

        return df
