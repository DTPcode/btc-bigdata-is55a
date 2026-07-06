"""
src/pipeline/orchestrator.py
=====================================================================
TẦNG ORCHESTRATION — Điều phối 3 tầng Ingestion → Processing → Storage.

Đây là NƠI DUY NHẤT các tầng "gặp nhau". Mỗi tầng (ingestion/processing/
storage) hoàn toàn không biết đến 2 tầng còn lại — chỉ Orchestrator
mới import và nối chúng lại. Thiết kế này giúp:
- Test riêng từng tầng độc lập (VD: test Processing mà không cần Binance)
- Đổi nguồn dữ liệu (Binance → Coinbase) chỉ cần sửa Orchestrator,
  không đụng tới Processing/Storage
- Debug dễ hơn: lỗi ở đâu, nhìn log prefix theo layer là biết
"""

from __future__ import annotations

import pandas as pd

from src.ingestion.binance_client import BinanceIngestion
from src.ingestion.p2p_client import P2PIngestion
from src.processing.indicators import DataQualityChecker, IndicatorEngine
from src.storage.bigquery_client import BigQueryStorage
from src.storage.p2p_storage import P2PStorage
from src.utils.logger import get_logger

log = get_logger(__name__)


class PipelineOrchestrator:
    """Điều phối toàn bộ luồng dữ liệu: Ingestion → Processing → Storage."""

    def __init__(self) -> None:
        self._ingestion = BinanceIngestion()
        self._p2p_ingestion = P2PIngestion()
        self._storage: BigQueryStorage | None = None  # Lazy init, xem _get_storage()
        self._p2p_storage: P2PStorage | None = None    # Lazy init, xem _get_p2p_storage()

    def _get_storage(self) -> BigQueryStorage:
        """
        Khởi tạo BigQueryStorage LAZY (chỉ khi thực sự cần dùng), vì mode
        "test" không cần chạm tới BigQuery — tránh bắt buộc phải có
        credentials chỉ để test Ingestion+Processing.
        """
        if self._storage is None:
            self._storage = BigQueryStorage()
        return self._storage

    def _get_p2p_storage(self) -> P2PStorage:
        """Lazy init cho P2PStorage — cùng lý do với _get_storage()."""
        if self._p2p_storage is None:
            self._p2p_storage = P2PStorage()
        return self._p2p_storage

    def _run_p2p_update(self) -> None:
        """
        Lấy % spread P2P hiện tại (CẢ 2 CHIỀU SELL + BUY) và ghi vào
        BigQuery. Bọc try/except RIÊNG, KHÔNG để lỗi ở đây làm gãy
        pipeline OHLCV chính — vì Binance P2P là API không chính thức,
        có thể lỗi bất cứ lúc nào mà không liên quan gì tới chất lượng
        dữ liệu OHLCV.
        """
        try:
            spreads = self._p2p_ingestion.fetch_spread()
            if not spreads:
                log.warning("[p2p] Không lấy được dữ liệu spread lần này (cả 2 chiều) — bỏ qua")
                return

            p2p_storage = self._get_p2p_storage()
            p2p_storage.ensure_table()
            for spread in spreads:
                p2p_storage.append(spread)
            log.info(f"[p2p] Đã ghi {len(spreads)} dòng spread ({', '.join(s['trade_type'] for s in spreads)})")
        except Exception:
            log.exception("[p2p] Lỗi khi cập nhật P2P spread — KHÔNG ảnh hưởng pipeline OHLCV chính")

    # -- Các mode chạy pipeline --------------------------------------------

    def run_init(self) -> pd.DataFrame:
        """
        MODE INIT — chạy 1 LẦN DUY NHẤT khi khởi tạo hệ thống.
        Lấy toàn bộ lịch sử → tính chỉ báo → upload BigQuery.
        Nên chạy trên máy LOCAL, không cần tự động hóa vì chỉ chạy 1 lần.
        """
        log.info("=" * 70)
        log.info("MODE: INIT — Khởi tạo hệ thống với dữ liệu lịch sử")
        log.info("=" * 70)

        raw_df = self._ingestion.fetch_historical()
        clean_df = DataQualityChecker.check_and_clean(raw_df)
        final_df = IndicatorEngine.calculate(clean_df)

        storage = self._get_storage()
        storage.ensure_dataset_and_table()
        uploaded = storage.upload(final_df)

        # [p2p] Đo luôn 1 lần spread P2P đầu tiên khi khởi tạo hệ thống
        self._run_p2p_update()

        log.info(f"✅ INIT hoàn tất: {uploaded:,} dòng đã lưu vào BigQuery")
        return final_df

    def run_update(self) -> pd.DataFrame:
        """
        MODE UPDATE — chạy LẶP LẠI mỗi giờ (qua GitHub Actions cron).
        Lấy N nến gần nhất → tính chỉ báo → upload incremental.
        """
        log.info("=" * 70)
        log.info("MODE: UPDATE — Cập nhật dữ liệu mới nhất")
        log.info("=" * 70)

        raw_df = self._ingestion.fetch_incremental()
        clean_df = DataQualityChecker.check_and_clean(raw_df)
        final_df = IndicatorEngine.calculate(clean_df)

        storage = self._get_storage()
        uploaded = storage.upload(final_df)

        # [p2p] Mỗi lần update (mỗi giờ) đo lại % spread P2P hiện tại
        self._run_p2p_update()

        if uploaded == 0:
            log.info("ℹ️  Không có nến mới — Binance chưa đóng nến kế tiếp hoặc đã cập nhật gần đây")
        else:
            latest = final_df.iloc[-1]
            log.info(
                f"✅ UPDATE hoàn tất: +{uploaded} dòng mới | "
                f"Giá mới nhất: ${latest['close']:,.2f} | RSI: {latest['rsi_14']:.1f}"
            )
        return final_df

    def run_test(self) -> pd.DataFrame:
        """
        MODE TEST — KHÔNG chạm BigQuery, chỉ kiểm tra Ingestion +
        Processing hoạt động đúng. Dùng khi mới code, chưa cần setup GCP.
        """
        log.info("=" * 70)
        log.info("MODE: TEST — Kiểm tra Ingestion + Processing (không dùng BigQuery)")
        log.info("=" * 70)

        raw_df = self._ingestion.fetch_incremental(lookback=300)
        clean_df = DataQualityChecker.check_and_clean(raw_df)
        final_df = IndicatorEngine.calculate(clean_df)

        latest = final_df.iloc[-1]
        log.info("📊 Nến gần nhất:")
        log.info(f"   Thời gian:  {latest['timestamp']}")
        log.info(f"   Giá đóng:   ${latest['close']:,.2f}")
        log.info(f"   RSI(14):    {latest['rsi_14']:.2f}")
        log.info(f"   MACD:       {latest['macd']:.4f}")
        log.info(f"   BB Upper:   ${latest['bb_upper']:,.2f}")
        log.info(f"   BB Lower:   ${latest['bb_lower']:,.2f}")
        log.info(f"   EMA 50:     ${latest['ema_50']:,.2f}")
        log.info("✅ TEST thành công — Ingestion và Processing hoạt động bình thường")
        return final_df
