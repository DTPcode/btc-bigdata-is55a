"""
src/storage/bigquery_client.py
=====================================================================
TẦNG STORAGE — Lưu trữ và truy vấn dữ liệu trên BigQuery (Data Warehouse).

Các điểm TỐI ƯU HÓA quan trọng trong module này:
1. Client Reuse (Singleton) — khởi tạo 1 lần, không tạo connection mới
   mỗi lần gọi hàm → giảm overhead, tránh rò rỉ kết nối
2. Batch Load (KHÔNG Streaming Insert) — Streaming Insert của BigQuery
   tính phí (~$0.01/200MB), Batch Load hoàn toàn MIỄN PHÍ
3. Chunked Upload — khi upload lịch sử lớn (~50.000 dòng), chia nhỏ
   thành từng chunk để tránh timeout và dễ retry nếu 1 chunk lỗi
4. Incremental Load — chỉ upload dòng có timestamp MỚI HƠN dữ liệu đã
   có trong BigQuery, tránh trùng lặp (deduplication tại nguồn)
5. Partitioning + Clustering — table được partition theo NGÀY và
   cluster theo timestamp, giúp mọi query filter theo thời gian chỉ
   scan đúng phần dữ liệu cần, tiết kiệm quota 1TB/tháng miễn phí
6. Query luôn bắt buộc có LIMIT — tránh quét nhầm toàn bộ table
"""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

from config.settings import settings
from src.storage.schemas import SCHEMA_OHLCV, CLUSTERING_FIELDS
from src.utils.logger import get_logger
from src.utils.retry import with_retry

log = get_logger(__name__)

# Chia nhỏ mỗi lần upload thành chunk 10.000 dòng — cân bằng giữa số lượng
# API call (ít quá thì mỗi call quá nặng, dễ timeout) và overhead mỗi call
CHUNK_SIZE = 10_000


class BigQueryStorage:
    """
    Client BigQuery dùng chung (singleton per process) cho toàn hệ thống.
    Khởi tạo 1 instance duy nhất trong orchestrator, KHÔNG tạo mới mỗi
    lần gọi hàm.
    """

    def __init__(self) -> None:
        settings.validate_for_bigquery()  # Fail sớm nếu thiếu config

        credentials = service_account.Credentials.from_service_account_file(
            settings.credentials_path,
            scopes=["https://www.googleapis.com/auth/bigquery"],
        )
        self._client = bigquery.Client(
            project=settings.gcp_project_id,
            credentials=credentials,
        )
        self._table_id = settings.full_table_id
        log.info(f"BigQueryStorage kết nối tới: {self._table_id}")

    # -- Khởi tạo hạ tầng (chạy 1 lần, idempotent) ------------------------

    def ensure_dataset_and_table(self) -> None:
        """
        Tạo dataset + table nếu chưa tồn tại. An toàn để gọi nhiều lần
        (idempotent) — không lỗi nếu đã tồn tại rồi.
        """
        dataset_ref = bigquery.Dataset(f"{settings.gcp_project_id}.{settings.bq_dataset_id}")
        dataset_ref.location = settings.bq_location
        self._client.create_dataset(dataset_ref, exists_ok=True)
        log.info(f"Dataset '{settings.bq_dataset_id}' sẵn sàng (location={settings.bq_location})")

        table = bigquery.Table(self._table_id, schema=SCHEMA_OHLCV)
        # Partition theo NGÀY dựa trên cột timestamp — mọi query có
        # WHERE timestamp >= X sẽ chỉ scan các partition liên quan
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="timestamp",
        )
        # Cluster theo timestamp — tối ưu thêm cho query sort/filter theo thời gian
        table.clustering_fields = CLUSTERING_FIELDS

        self._client.create_table(table, exists_ok=True)
        log.info(f"Table '{self._table_id}' sẵn sàng (partitioned by DAY, clustered by timestamp)")

    # -- Ghi dữ liệu (Batch Load - miễn phí) ------------------------------

    def get_latest_timestamp(self) -> datetime | None:
        """
        Lấy timestamp mới nhất hiện có trong BigQuery.
        Dùng để lọc incremental — chỉ upload dòng MỚI HƠN mốc này.
        Trả về None nếu table rỗng (lần chạy đầu tiên).
        """
        query = f"SELECT MAX(timestamp) AS max_ts FROM `{self._table_id}`"
        result = self._client.query(query).result()
        for row in result:
            return row.max_ts
        return None

    def upload(self, df: pd.DataFrame) -> int:
        """
        Upload DataFrame lên BigQuery bằng Batch Load (KHÔNG Streaming Insert).

        Tự động:
        - Lọc chỉ giữ dòng có timestamp MỚI HƠN dữ liệu đã có (incremental)
        - Chia thành chunk nếu dữ liệu lớn (tránh timeout 1 request khổng lồ)
        - Gắn cột _loaded_at để biết dòng này được ghi lúc nào

        Trả về số dòng THỰC SỰ đã upload (0 nếu không có gì mới).
        """
        if df.empty:
            log.info("Upload: DataFrame rỗng, không có gì để tải lên")
            return 0

        df = df.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

        latest_ts = self.get_latest_timestamp()
        if latest_ts is not None:
            before = len(df)
            # BigQuery trả về datetime ĐÃ có timezone (tz-aware) — không được
            # gán thêm tz="UTC" lần nữa (pandas sẽ báo lỗi ValueError vì xung
            # đột). Dùng pd.Timestamp() trực tiếp, rồi tz_convert nếu cần.
            latest_ts_pd = pd.Timestamp(latest_ts)
            if latest_ts_pd.tzinfo is None:
                latest_ts_pd = latest_ts_pd.tz_localize("UTC")
            else:
                latest_ts_pd = latest_ts_pd.tz_convert("UTC")
            df = df[df["timestamp"] > latest_ts_pd]
            log.info(f"Upload: lọc incremental — giữ {len(df)}/{before} dòng mới hơn {latest_ts}")
        else:
            log.info(f"Upload: table đang rỗng — sẽ tải lên toàn bộ {len(df)} dòng (lần khởi tạo)")

        if df.empty:
            log.info("Upload: không có dòng mới sau khi lọc — bỏ qua")
            return 0

        df["_loaded_at"] = datetime.now(timezone.utc)

        total_uploaded = 0
        n_chunks = (len(df) + CHUNK_SIZE - 1) // CHUNK_SIZE
        for i in range(0, len(df), CHUNK_SIZE):
            chunk = df.iloc[i : i + CHUNK_SIZE]
            self._load_chunk(chunk)
            total_uploaded += len(chunk)
            log.info(f"Upload: chunk {i // CHUNK_SIZE + 1}/{n_chunks} xong ({len(chunk)} dòng)")

        log.info(f"✅ Upload hoàn tất: {total_uploaded} dòng mới vào '{self._table_id}'")
        return total_uploaded

    @with_retry(max_attempts=3)
    def _load_chunk(self, chunk: pd.DataFrame) -> None:
        """Upload 1 chunk — có retry riêng để chunk lỗi không làm hỏng cả batch."""
        job_config = bigquery.LoadJobConfig(
            schema=SCHEMA_OHLCV,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        job = self._client.load_table_from_dataframe(chunk, self._table_id, job_config=job_config)
        job.result()  # Chờ job hoàn thành, raise lỗi nếu job fail

    # -- Đọc dữ liệu (Query - tối ưu quota) --------------------------------

    def query_recent(self, hours: int = 168, limit: int = 5000) -> pd.DataFrame:
        """
        Lấy dữ liệu N giờ gần nhất. LUÔN có WHERE timestamp (tận dụng
        partition pruning) VÀ LIMIT — bắt buộc theo chuẩn tối ưu quota.
        """
        query = f"""
            SELECT *
            FROM `{self._table_id}`
            WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {int(hours)} HOUR)
            ORDER BY timestamp ASC
            LIMIT {int(limit)}
        """
        df = self._client.query(query).to_dataframe()
        log.info(f"Query: lấy {len(df)} dòng ({hours}h gần nhất, limit={limit})")
        return df

    def count_rows(self) -> int:
        """Đếm tổng số dòng hiện có — dùng để kiểm tra/báo cáo, không phải hot path."""
        query = f"SELECT COUNT(*) AS n FROM `{self._table_id}`"
        result = list(self._client.query(query).result())
        return result[0].n if result else 0
