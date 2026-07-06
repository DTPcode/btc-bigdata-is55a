"""
src/ingestion/p2p_client.py
=====================================================================
TẦNG INGESTION — Thu thập dữ liệu giá P2P (Binance P2P) + tỷ giá quốc tế,
để tính % CHÊNH LỆCH (spread) giữa giá USDT thực tế trên thị trường P2P
Việt Nam so với tỷ giá USD/VND "chuẩn" trên thị trường quốc tế.

Ý NGHĨA THỰC TẾ:
Người mua/bán BTC/USDT ở Việt Nam phải quy đổi qua kênh P2P (vì Binance
không cho rút thẳng VND). Giá P2P luôn LỆCH so với tỷ giá quốc tế thật
— đây là "chi phí ẩn" (hoặc đôi khi là "premium" có lợi) mà nhà đầu tư
cá nhân phải chịu, không sàn/nền tảng phân tích nào hiển thị sẵn.

--- VÌ SAO PHIÊN BẢN NÀY LẤY MẪU NHIỀU LẦN (multi-sample average) ---
Dữ liệu OHLCV (nến 1 giờ) của Binance là kết quả TỰ ĐỘNG TỔNG HỢP toàn
bộ giao dịch THẬT diễn ra trong suốt 1 giờ (open/high/low/close) — do
chính máy chủ khớp lệnh của Binance tính, KHÔNG phải 1 lần gọi API tại
1 thời điểm.

Ngược lại, Binance P2P KHÔNG có API lịch sử/nến — chỉ có API trả về
danh sách quảng cáo ĐANG ĐĂNG tại đúng thời điểm gọi (ảnh chụp tức
thời). Để giá P2P bớt phụ thuộc vào may rủi của 1 thời điểm và "gần"
với tinh thần tổng hợp theo thời gian như OHLCV, module này gọi API
NHIỀU LẦN liên tiếp (mỗi lần cách nhau vài giây), mỗi lần lấy trung
bình top N quảng cáo, rồi lấy trung bình của các lần đó — tương tự
việc lấy nhiều điểm dữ liệu trong 1 khoảng thời gian ngắn thay vì tin
vào 1 điểm duy nhất.

⚠️ Lưu ý: đây VẪN chỉ là các mẫu lấy trong vài chục giây tại thời điểm
pipeline chạy (mỗi giờ 1 lần theo cron), KHÔNG phải trung bình liên tục
suốt cả giờ như OHLCV thật — vì Binance P2P không cung cấp API lịch sử
để tính lại. Đây là giới hạn cố hữu của nguồn dữ liệu, cần nêu rõ trong
báo cáo.

NGUỒN DỮ LIỆU:
1. Binance P2P Public API (không chính thức nhưng công khai, không cần
   đăng nhập/API key):
       POST https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search
2. Tỷ giá USD/VND "chuẩn" — open.er-api.com (miễn phí, không cần key),
   coi 1 USDT ≈ 1 USD (đúng bản chất stablecoin neo giá USD).

⚠️ API Binance P2P là API KHÔNG CHÍNH THỨC (undocumented) — có thể đổi
cấu trúc bất cứ lúc nào. Module có retry + xử lý lỗi đầy đủ; nếu lỗi,
trả về None/danh sách rỗng thay vì làm crash pipeline OHLCV chính.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import TypedDict

import requests

from config.settings import settings
from src.utils.logger import get_logger
from src.utils.retry import with_retry

log = get_logger(__name__)

_P2P_URL = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"

# 2 chiều giao dịch cần lấy trong mỗi lần chạy — SELL (người bán USDT
# lấy VND) và BUY (người mua USDT bằng VND) — để có bức tranh đủ 2 phía.
_TRADE_TYPES = ["SELL", "BUY"]


class P2PSpreadResult(TypedDict):
    """Kết quả tính spread cho 1 CHIỀU giao dịch — dùng chung Ingestion/Storage."""
    timestamp: datetime
    asset: str
    fiat: str
    trade_type: str        # "SELL" hoặc "BUY"
    p2p_price: float        # Trung bình của trung bình nhiều mẫu (xem docstring class)
    p2p_price_min: float    # Giá thấp nhất trong các mẫu đã lấy
    p2p_price_max: float    # Giá cao nhất trong các mẫu đã lấy
    samples: int             # Số lần lấy mẫu thực tế đã dùng để tính trung bình
    market_price: float
    spread_pct: float


class P2PIngestion:
    """
    Client lấy giá P2P (Binance, lấy mẫu nhiều lần) + tỷ giá quốc tế
    (open.er-api.com), tính % spread cho CẢ 2 CHIỀU (SELL/BUY) trong
    cùng 1 lần gọi fetch_spread().
    """

    def __init__(self) -> None:
        self._asset = settings.p2p_asset
        self._fiat = settings.p2p_fiat
        self._rows = settings.p2p_rows
        self._n_samples = settings.p2p_samples
        self._sample_delay = settings.p2p_sample_delay_sec
        self._session = requests.Session()
        # QUAN TRỌNG: Binance P2P chặn request không có User-Agent hợp lệ
        # (trả về 403 Forbidden qua Cloudflare anti-bot) — giả lập header
        # như trình duyệt thật để request đi qua được.
        self._session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
            ),
            "Content-Type": "application/json",
            "Accept": "*/*",
        })
        log.info(
            f"P2PIngestion khởi tạo: asset={self._asset}, fiat={self._fiat}, "
            f"top {self._rows} quảng cáo/mẫu, {self._n_samples} mẫu/lần chạy "
            f"(cách nhau {self._sample_delay}s), cả 2 chiều SELL+BUY"
        )

    # -- Public API --------------------------------------------------------

    def fetch_spread(self) -> list[P2PSpreadResult]:
        """
        Lấy % spread cho CẢ 2 CHIỀU (SELL và BUY) trong 1 lần gọi.
        Trả về list (0-2 phần tử) — phần tử nào lỗi sẽ bị bỏ qua, KHÔNG
        làm hỏng chiều còn lại (2 chiều độc lập với nhau).
        """
        try:
            market_price = self._get_market_usd_vnd_rate()
        except Exception:
            log.exception("[p2p] Lỗi khi lấy tỷ giá quốc tế — bỏ qua cả 2 chiều lần này")
            return []

        if market_price is None:
            log.warning("[p2p] Không lấy được tỷ giá quốc tế — bỏ qua lần này")
            return []

        results: list[P2PSpreadResult] = []
        for trade_type in _TRADE_TYPES:
            result = self._fetch_one_direction(trade_type, market_price)
            if result is not None:
                results.append(result)
        return results

    # -- Private helpers ----------------------------------------------------

    def _fetch_one_direction(self, trade_type: str, market_price: float) -> P2PSpreadResult | None:
        """Lấy spread cho 1 chiều (SELL hoặc BUY), lấy mẫu nhiều lần rồi trung bình."""
        try:
            samples = self._collect_price_samples(trade_type)
        except Exception:
            log.exception(f"[p2p] Lỗi khi lấy mẫu giá P2P chiều {trade_type} — bỏ qua chiều này")
            return None

        if not samples:
            log.warning(f"[p2p] Không có mẫu giá P2P hợp lệ cho chiều {trade_type}")
            return None

        p2p_price = sum(samples) / len(samples)

        # Spread = (thị trường - P2P) / thị trường × 100
        # SELL: dương = người bán bị THIỆT (bán rẻ hơn giá quốc tế)
        # BUY:  dương = người mua được LỢI (mua rẻ hơn giá quốc tế)
        spread_pct = (market_price - p2p_price) / market_price * 100

        result: P2PSpreadResult = {
            "timestamp": datetime.now(timezone.utc),
            "asset": self._asset,
            "fiat": self._fiat,
            "trade_type": trade_type,
            "p2p_price": round(p2p_price, 2),
            "p2p_price_min": round(min(samples), 2),
            "p2p_price_max": round(max(samples), 2),
            "samples": len(samples),
            "market_price": round(market_price, 2),
            "spread_pct": round(spread_pct, 4),
        }
        log.info(
            f"[p2p] {self._asset}/{self._fiat} ({trade_type}, {len(samples)} mẫu): "
            f"P2P avg={result['p2p_price']:,.0f} (min={result['p2p_price_min']:,.0f}, "
            f"max={result['p2p_price_max']:,.0f}) | Market={result['market_price']:,.0f} | "
            f"Spread={result['spread_pct']:.3f}%"
        )
        return result

    def _collect_price_samples(self, trade_type: str) -> list[float]:
        """
        Gọi API P2P NHIỀU LẦN liên tiếp (cách nhau self._sample_delay giây),
        mỗi lần lấy giá trung bình top N quảng cáo — trả về danh sách các
        giá trị mẫu để tầng gọi tính trung bình/min/max.

        Nếu 1 mẫu lỗi/rỗng, BỎ QUA mẫu đó (không làm hỏng cả lần chạy) —
        chỉ khi TẤT CẢ mẫu đều lỗi thì mới trả về danh sách rỗng.
        """
        samples: list[float] = []
        for i in range(self._n_samples):
            try:
                price = self._get_p2p_average_price(trade_type=trade_type)
                if price is not None:
                    samples.append(price)
            except Exception:
                log.warning(f"[p2p] Mẫu {i + 1}/{self._n_samples} ({trade_type}) lỗi — bỏ qua mẫu này")

            # Không sleep sau mẫu cuối cùng — tránh chờ vô ích
            if i < self._n_samples - 1:
                time.sleep(self._sample_delay)
        return samples

    @with_retry(max_attempts=3)
    def _get_p2p_average_price(self, trade_type: str) -> float | None:
        """
        Gọi Binance P2P search API 1 LẦN, lấy giá trung bình của N quảng
        cáo TOP đầu (uy tín nhất theo mặc định sắp xếp của Binance).
        """
        payload = {
            "page": 1,
            "rows": self._rows,
            "payTypes": [],
            "asset": self._asset,
            "tradeType": trade_type,
            "fiat": self._fiat,
        }
        response = self._session.post(_P2P_URL, json=payload, timeout=15)
        response.raise_for_status()
        body = response.json()

        ads = body.get("data") or []
        if not ads:
            log.warning(f"[p2p] Binance P2P trả về danh sách rỗng cho {self._asset}/{self._fiat} ({trade_type})")
            return None

        prices = [float(ad["adv"]["price"]) for ad in ads if "adv" in ad and "price" in ad["adv"]]
        if not prices:
            return None
        return sum(prices) / len(prices)

    @with_retry(max_attempts=3)
    def _get_market_usd_vnd_rate(self) -> float | None:
        """
        Lấy tỷ giá USD/VND "chuẩn" quốc tế. Coi 1 USDT ≈ 1 USD (đúng bản
        chất stablecoin neo giá theo USD, sai số không đáng kể ở đây).
        Chỉ cần lấy 1 lần/lần chạy (tỷ giá liên ngân hàng không biến
        động nhanh như giá P2P nên không cần lấy mẫu nhiều lần).
        """
        response = self._session.get(settings.fx_api_url, timeout=15)
        response.raise_for_status()
        body = response.json()

        rates = body.get("rates") or {}
        rate = rates.get(self._fiat)
        if rate is None:
            log.warning(f"[p2p] Không tìm thấy tỷ giá {self._fiat} trong response FX API")
            return None
        return float(rate)
