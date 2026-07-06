"""
src/storage/p2p_storage.py
=====================================================================
TẦNG STORAGE — Lưu trữ dữ liệu spread P2P vào BigQuery.

Khác với BigQueryStorage (OHLCV), bảng này KHÔNG cần cơ chế UPSERT
qua DDL swap phức tạp, vì:
- Mỗi dòng là 1 "sự kiện đo" độc lập theo _loaded_at, không có khái
  niệm "nến bị sửa lại" như OHLCV (nến đang mở có thể update giá)
- Tần suất ghi thấp (1 dòng/giờ), dữ liệu nhỏ → APPEND đơn giản, dùng
  Batch Load (KHÔNG Streaming Insert) để giữ đúng nguyên tắc miễn phí
  của toàn hệ thống.

Vẫn dùng chung Partitioning theo NGÀY để tối ưu quota khi query lịch
sử dài, và cùng retry pattern (with_retry) như các module storage khác.
"""

from __future__ import annotations

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

from config.settings import settings
from src.ingestion.p2p_client import P2PSpreadResult
from src.storage.schemas import SCHEMA_P2P_SPREAD
from src.utils.logger import get_logger
from src.utils.retry import with_retry

log = get_logger(__name__)

_TABLE_NAME = "p2p_spread_history"


class P2PStorage:
    """Client BigQuery riêng cho bảng p2p_spread_history (append-only)."""

    def __init__(self) -> None:
        settings.validate_for_bigquery()

        credentials = service_account.Credentials.from_service_account_file(
            settings.credentials_path,
            scopes=["https://www.googleapis.com/auth/bigquery"],
        )
        self._client = bigquery.Client(
            project=settings.gcp_project_id,
            credentials=credentials,
        )
        self._dataset_id = f"{settings.gcp_project_id}.{settings.bq_dataset_id}"
        self._table_id = f"{self._dataset_id}.{_TABLE_NAME}"
        log.info(f"P2PStorage kết nối tới: {self._table_id}")

    def ensure_table(self) -> None:
        """Tạo table nếu chưa tồn tại — idempotent, an toàn gọi nhiều lần."""
        table = bigquery.Table(self._table_id, schema=SCHEMA_P2P_SPREAD)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="timestamp",
        )
        self._client.create_table(table, exists_ok=True)
        log.info(f"[p2p] Table '{self._table_id}' sẵn sàng (partitioned by DAY)")

    @with_retry(max_attempts=3)
    def append(self, result: P2PSpreadResult) -> None:
        """
        Ghi 1 dòng kết quả spread vào BigQuery bằng Load Job (Batch Load,
        miễn phí) — KHÔNG dùng insert_rows_json (Streaming Insert có phí).
        """
        from datetime import datetime, timezone

        row = dict(result)
        row["_loaded_at"] = datetime.now(timezone.utc)
        df = pd.DataFrame([row])
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

        job_config = bigquery.LoadJobConfig(
            schema=SCHEMA_P2P_SPREAD,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        job = self._client.load_table_from_dataframe(df, self._table_id, job_config=job_config)
        job.result()
        log.info(f"[p2p] Đã ghi 1 dòng spread vào '{self._table_id}'")

    def query_recent(self, hours: int = 168, limit: int = 1000) -> pd.DataFrame:
        """Lấy lịch sử spread N giờ gần nhất — LUÔN có WHERE + LIMIT."""
        query = f"""
            SELECT *
            FROM `{self._table_id}`
            WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {int(hours)} HOUR)
            ORDER BY timestamp DESC
            LIMIT {int(limit)}
        """
        df = self._client.query(query).to_dataframe()
        log.info(f"[p2p] Query: lấy {len(df)} dòng lịch sử spread ({hours}h gần nhất)")
        return df
