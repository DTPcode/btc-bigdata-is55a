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
import numpy as np
import pandas as pd
import sys
from pathlib import Path

# Cho phép import "config" và "src" dù chạy uvicorn từ đâu
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from src.storage.bigquery_client import BigQueryStorage
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


# -- Endpoints --------------------------------------------------------------

@app.get("/")
def root():
    """Endpoint gốc — kiểm tra server sống, xem thông tin cơ bản."""
    return {
        "message": "BTC Big Data API — IS55A FinTech",
        "docs": "/docs",
        "endpoints": ["/api/ohlcv", "/api/latest", "/api/indicators/summary"],
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

                # [api] Xử lý NaN -> None để tránh lỗi JSON "Out of range float values"
        df = df.replace({np.nan: None})
        df = df.where(pd.notnull(df), None)
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
