"""
tests/test_indicators.py
=====================================================================
UNIT TEST cho tầng Processing — chạy độc lập, KHÔNG cần Binance API
hay BigQuery credentials. Dùng dữ liệu giả (synthetic) để kiểm tra
logic tính toán và data quality có đúng không.

Chạy: pytest tests/ -v
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.processing.indicators import DataQualityChecker, IndicatorEngine


def _make_fake_ohlcv(n: int = 300) -> pd.DataFrame:
    """Tạo dữ liệu giá giả lập dạng random walk — đủ để test chỉ báo chạy được."""
    rng = np.random.default_rng(seed=42)
    timestamps = pd.date_range("2024-01-01", periods=n, freq="1h", tz="UTC")
    price = 50_000 + np.cumsum(rng.normal(0, 100, n))

    df = pd.DataFrame({
        "timestamp": timestamps,
        "open": price,
        "high": price + rng.uniform(10, 100, n),
        "low": price - rng.uniform(10, 100, n),
        "close": price + rng.normal(0, 20, n),
        "volume": rng.uniform(100, 1000, n),
        "trades": rng.integers(50, 500, n),
    })
    return df


class TestDataQualityChecker:
    def test_removes_duplicate_timestamps(self):
        df = _make_fake_ohlcv(50)
        df_with_dupe = pd.concat([df, df.iloc[[0]]], ignore_index=True)
        cleaned = DataQualityChecker.check_and_clean(df_with_dupe)
        assert cleaned["timestamp"].is_unique

    def test_removes_invalid_prices(self):
        df = _make_fake_ohlcv(50)
        df.loc[5, "close"] = -100  # Giá âm - không hợp lệ
        df.loc[10, "high"] = 1     # high < low - không hợp lệ
        df.loc[10, "low"] = 100
        cleaned = DataQualityChecker.check_and_clean(df)
        assert len(cleaned) == 48  # Loại 2 dòng lỗi

    def test_empty_dataframe_does_not_crash(self):
        empty = pd.DataFrame()
        result = DataQualityChecker.check_and_clean(empty)
        assert result.empty


class TestIndicatorEngine:
    def test_calculates_all_expected_columns(self):
        df = _make_fake_ohlcv(300)
        result = IndicatorEngine.calculate(df)

        expected_cols = [
            "rsi_14", "macd", "macd_signal", "macd_hist",
            "bb_upper", "bb_mid", "bb_lower", "bb_width",
            "ema_20", "ema_50", "ema_200", "atr_14", "vol_ma_20",
        ]
        for col in expected_cols:
            assert col in result.columns, f"Thiếu cột {col}"

    def test_rsi_within_valid_range(self):
        """RSI luôn phải nằm trong [0, 100] — nếu sai là lỗi công thức nghiêm trọng."""
        df = _make_fake_ohlcv(300)
        result = IndicatorEngine.calculate(df)
        assert result["rsi_14"].between(0, 100).all()

    def test_bollinger_upper_always_above_lower(self):
        df = _make_fake_ohlcv(300)
        result = IndicatorEngine.calculate(df)
        assert (result["bb_upper"] >= result["bb_lower"]).all()

    def test_drops_warmup_nan_rows(self):
        """Sau khi tính chỉ báo, không còn dòng NaN ở các cột chính."""
        df = _make_fake_ohlcv(300)
        result = IndicatorEngine.calculate(df)
        assert result["rsi_14"].isna().sum() == 0
        assert result["ema_200"].notna().sum() > 0  # Có đủ data cho EMA200


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
