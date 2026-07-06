"""
Test kiểm tra logic multi-sample + 2 chiều SELL/BUY của P2PIngestion,
KHÔNG gọi API thật (mock toàn bộ _get_p2p_average_price và
_get_market_usd_vnd_rate) — dùng để verify logic trước khi push, vì môi
trường CI/sandbox có thể không truy cập được p2p.binance.com.
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

from src.ingestion.p2p_client import P2PIngestion


class DummySettings:
    p2p_asset = "USDT"
    p2p_fiat = "VND"
    p2p_rows = 10
    p2p_samples = 3
    p2p_sample_delay_sec = 0  # không chờ thật trong test
    fx_api_url = "https://open.er-api.com/v6/latest/USD"


@pytest.fixture(autouse=True)
def patched_settings():
    with patch("src.ingestion.p2p_client.settings", DummySettings()):
        yield


def test_fetch_spread_returns_both_directions():
    """fetch_spread() phải trả về 2 phần tử (SELL + BUY) khi mọi mẫu OK."""
    client = P2PIngestion()

    # Giả lập: mỗi chiều có 3 mẫu giá khác nhau chút để test avg/min/max
    sell_samples = iter([26200.0, 26250.0, 26300.0])
    buy_samples = iter([26100.0, 26150.0, 26200.0])

    def fake_price(trade_type):
        return next(sell_samples) if trade_type == "SELL" else next(buy_samples)

    with patch.object(client, "_get_market_usd_vnd_rate", return_value=26000.0), \
         patch.object(client, "_get_p2p_average_price", side_effect=fake_price):
        results = client.fetch_spread()

    assert len(results) == 2
    types = {r["trade_type"] for r in results}
    assert types == {"SELL", "BUY"}

    sell = next(r for r in results if r["trade_type"] == "SELL")
    assert sell["samples"] == 3
    assert sell["p2p_price"] == pytest.approx((26200 + 26250 + 26300) / 3, rel=1e-6)
    assert sell["p2p_price_min"] == 26200.0
    assert sell["p2p_price_max"] == 26300.0
    assert sell["market_price"] == 26000.0
    # spread = (market - p2p) / market * 100
    expected_spread = (26000.0 - sell["p2p_price"]) / 26000.0 * 100
    assert sell["spread_pct"] == pytest.approx(round(expected_spread, 4))


def test_fetch_spread_skips_direction_if_all_samples_fail():
    """Nếu 1 chiều lỗi hết 3 mẫu, chiều đó bị bỏ qua nhưng chiều kia vẫn OK."""
    client = P2PIngestion()

    def fake_price(trade_type):
        if trade_type == "SELL":
            raise ConnectionError("giả lập lỗi mạng")
        return 26100.0

    with patch.object(client, "_get_market_usd_vnd_rate", return_value=26000.0), \
         patch.object(client, "_get_p2p_average_price", side_effect=fake_price):
        results = client.fetch_spread()

    assert len(results) == 1
    assert results[0]["trade_type"] == "BUY"


def test_fetch_spread_returns_empty_list_if_market_price_missing():
    """Nếu không lấy được tỷ giá quốc tế, trả về [] (không raise, không crash)."""
    client = P2PIngestion()

    with patch.object(client, "_get_market_usd_vnd_rate", return_value=None):
        results = client.fetch_spread()

    assert results == []