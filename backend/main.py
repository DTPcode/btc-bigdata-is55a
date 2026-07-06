"""
backend/main.py
=====================================================================
BACKEND API — Cầu nối giữa BigQuery và Frontend.

Backend KHÔNG đổ dữ liệu vào BigQuery (việc đó do pipeline + GitHub
Actions đảm nhiệm, chạy độc lập mỗi giờ). Backend chỉ ĐỌC dữ liệu đã
có sẵn trong BigQuery và trả về dạng JSON cho Frontend gọi.

Chạy local:
    uvicorn backend.main:app --reload --port 8000

Sau khi chạy, xem tài liệu API tự sinh tại:
    http://localhost:8000/docs
"""

import sys
from pathlib import Path

# Cho phép import "config" và "src" dù chạy uvicorn từ đâu
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from datetime import datetime, timezone

import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from src.processing.tax_calculator import TaxCalculator
from src.storage.bigquery_client import BigQueryStorage
from src.storage.p2p_storage import P2PStorage
from src.utils.logger import get_logger

log = get_logger(__name__)

app = FastAPI(
    title="BTC Big Data API",
    description="API đọc dữ liệu BTC + chỉ báo kỹ thuật từ BigQuery — IS55A FinTech",
    version="1.0.0",
)

# Cho phép Frontend (chạy ở domain/port khác) gọi được API này
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # Production: nên giới hạn đúng domain Frontend
    allow_methods=["GET"],
    allow_headers=["*"],
)

# Khởi tạo BigQueryStorage MỘT LẦN DUY NHẤT khi server start — không tạo
# lại mỗi request, tránh tốn thời gian xác thực credentials lặp lại
_storage: BigQueryStorage | None = None


def get_storage() -> BigQueryStorage:
    """Lazy singleton — chỉ kết nối BigQuery khi có request đầu tiên gọi tới."""
    global _storage
    if _storage is None:
        _storage = BigQueryStorage()
    return _storage


_p2p_storage: P2PStorage | None = None


def get_p2p_storage() -> P2PStorage:
    """Lazy singleton cho P2PStorage — cùng lý do với get_storage()."""
    global _p2p_storage
    if _p2p_storage is None:
        _p2p_storage = P2PStorage()
    return _p2p_storage


# -- Endpoints --------------------------------------------------------------

@app.get("/")
def root():
    """Endpoint gốc — kiểm tra server sống, xem thông tin cơ bản."""
    return {
        "message": "BTC Big Data API — IS55A FinTech",
        "docs": "/docs",
        "endpoints": [
            "/api/ohlcv", "/api/latest", "/api/indicators/summary",
            "/api/p2p-spread", "/api/tax-estimate",
        ],
        "time": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/health")
def health():
    """Health check — dùng cho Render/Railway kiểm tra server còn sống không."""
    return {"status": "ok", "time": datetime.now(timezone.utc).isoformat()}


@app.get("/api/ohlcv")
def get_ohlcv(
    hours: int = Query(default=168, ge=1, le=8760, description="Số giờ dữ liệu (mặc định 168 = 7 ngày)")
):
    """
    Trả về dữ liệu nến OHLCV + toàn bộ chỉ báo kỹ thuật trong N giờ gần nhất.
    Frontend dùng endpoint này để vẽ biểu đồ nến (candlestick chart).
    """
    try:
        df = get_storage().query_recent(hours=hours, limit=5000)
        if df.empty:
            return {"symbol": "BTCUSDT", "timeframe": "1h", "count": 0, "data": []}

        # Chuyển timestamp sang ISO string để serialize JSON đúng chuẩn
        df["timestamp"] = df["timestamp"].astype(str)
        if "_loaded_at" in df.columns:
            df = df.drop(columns=["_loaded_at"])  # Cột nội bộ, Frontend không cần

        # [api] Xử lý NaN -> None: các dòng đầu tiên (nến cũ) thường thiếu
        # đủ dữ liệu lịch sử để tính EMA200/MACD/StochRSI... nên bị NaN.
        # JSON chuẩn không cho phép NaN -> phải đổi thành null trước khi trả.
        df = df.replace({np.nan: None})
        df = df.where(pd.notnull(df), None)  # phòng trường hợp NaT/NaN còn sót

        return {
            "symbol": "BTCUSDT",
            "timeframe": "1h",
            "hours": hours,
            "count": len(df),
            "data": df.to_dict(orient="records"),
        }
    except Exception as e:
        log.exception("Lỗi khi query /api/ohlcv")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/latest")
def get_latest():
    """
    Trả về nến gần nhất + toàn bộ chỉ báo — dùng cho ticker giá hiển
    thị trên đầu trang Frontend.
    """
    try:
        df = get_storage().query_recent(hours=24, limit=1000)
        if df.empty:
            raise HTTPException(status_code=404, detail="Chưa có dữ liệu trong BigQuery")

        latest = df.sort_values("timestamp").iloc[-1]
        result = latest.to_dict()
        result["timestamp"] = str(result["timestamp"])
        result.pop("_loaded_at", None)
        return result
    except HTTPException:
        raise
    except Exception as e:
        log.exception("Lỗi khi query /api/latest")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/indicators/summary")
def get_indicators_summary():
    """
    Trả về tín hiệu mua/bán tổng hợp dựa trên các chỉ báo kỹ thuật của
    nến gần nhất. Frontend dùng để hiển thị bảng tín hiệu trực quan.
    """
    try:
        df = get_storage().query_recent(hours=24, limit=1000)
        if df.empty:
            raise HTTPException(status_code=404, detail="Chưa có dữ liệu trong BigQuery")

        d = df.sort_values("timestamp").iloc[-1]
        price = float(d["close"])

        signals = {
            "RSI": {
                "value": round(float(d["rsi_14"]), 2),
                "signal": "SELL" if d["rsi_14"] > 70 else "BUY" if d["rsi_14"] < 30 else "NEUTRAL",
                "note": "Quá mua" if d["rsi_14"] > 70 else "Quá bán" if d["rsi_14"] < 30 else "Trung tính",
            },
            "MACD": {
                "value": round(float(d["macd"] - d["macd_signal"]), 4),
                "signal": "BUY" if d["macd"] > d["macd_signal"] else "SELL",
                "note": "MACD trên Signal" if d["macd"] > d["macd_signal"] else "MACD dưới Signal",
            },
            "Bollinger": {
                "value": round(price, 2),
                "signal": "SELL" if price > d["bb_upper"] else "BUY" if price < d["bb_lower"] else "NEUTRAL",
                "note": "Trên dải trên" if price > d["bb_upper"] else "Dưới dải dưới" if price < d["bb_lower"] else "Trong dải BB",
            },
            "EMA_Trend": {
                "value": round(float(d["ema_50"]), 2),
                "signal": "BUY" if price > d["ema_50"] else "SELL",
                "note": "Giá trên EMA50 (uptrend)" if price > d["ema_50"] else "Giá dưới EMA50 (downtrend)",
            },
        }

        buy_count = sum(1 for s in signals.values() if s["signal"] == "BUY")
        sell_count = sum(1 for s in signals.values() if s["signal"] == "SELL")

        return {
            "timestamp": str(d["timestamp"]),
            "price": round(price, 2),
            "signals": signals,
            "overall": {
                "buy": buy_count,
                "sell": sell_count,
                "neutral": len(signals) - buy_count - sell_count,
                "verdict": "BUY" if buy_count > sell_count else "SELL" if sell_count > buy_count else "NEUTRAL",
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        log.exception("Lỗi khi query /api/indicators/summary")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/p2p-spread")
def get_p2p_spread(
    hours: int = Query(default=168, ge=1, le=8760, description="Số giờ lịch sử spread (mặc định 168 = 7 ngày)")
):
    """
    Trả về % chênh lệch (spread) giữa giá bán USDT trên Binance P2P và
    tỷ giá USD/VND quốc tế — cả giá trị MỚI NHẤT và LỊCH SỬ N giờ gần
    đây. Frontend dùng để hiển thị "chi phí ẩn" khi rút tiền qua P2P.
    """
    try:
        df = get_p2p_storage().query_recent(hours=hours, limit=2000)
        if df.empty:
            return {
                "count": 0, "data": [], "latest": None,
                "note": "Chưa có dữ liệu spread — pipeline có thể chưa chạy đủ 1 chu kỳ",
            }

        df["timestamp"] = df["timestamp"].astype(str)
        if "_loaded_at" in df.columns:
            df = df.drop(columns=["_loaded_at"])

        # df đã ORDER BY timestamp DESC trong query_recent -> dòng đầu là mới nhất
        latest = df.iloc[0].to_dict()

        return {
            "count": len(df),
            "hours": hours,
            "latest": latest,
            "data": df.to_dict(orient="records"),
        }
    except Exception as e:
        log.exception("Lỗi khi query /api/p2p-spread")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/tax-estimate")
def get_tax_estimate(
    amount: float = Query(..., gt=0, description="VN: giá trị bán (VNĐ). US: lợi nhuận (USD)"),
    country: str = Query(default="VN", pattern="^(VN|US)$", description="VN hoặc US"),
    holding_days: int = Query(default=0, ge=0, description="Chỉ dùng cho US: số ngày đã nắm giữ"),
):
    """
    Ước tính thuế khi bán BTC/crypto, theo 2 kịch bản:
    - VN: 0.1% trên giá trị bán (amount = tổng tiền bán, VNĐ)
    - US: bracket lũy tiến trên lợi nhuận (amount = lợi nhuận, USD),
      cần thêm holding_days để phân biệt short/long-term.

    Đây là công cụ ƯỚC TÍNH tham khảo, KHÔNG thay thế tư vấn thuế.
    """
    try:
        if country == "VN":
            result = TaxCalculator.calc_tax_vn(sale_value_vnd=amount)
        else:
            result = TaxCalculator.calc_tax_us(capital_gain_usd=amount, holding_period_days=holding_days)

        return {
            "country": result.country,
            "gross_amount": result.gross_amount,
            "taxable_base": result.taxable_base,
            "tax_rate_pct": result.tax_rate_pct,
            "tax_amount": result.tax_amount,
            "net_amount": result.net_amount,
            "note": result.note,
            "disclaimer": "Chỉ mang tính ước tính tham khảo, không thay thế tư vấn thuế chuyên nghiệp.",
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        log.exception("Lỗi khi tính /api/tax-estimate")
        raise HTTPException(status_code=500, detail=str(e))
