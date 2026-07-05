"""
TẦNG STORAGE — Lưu trữ và truy vấn dữ liệu trên BigQuery (Data Warehouse).

Các điểm TỐI ƯU HÓA quan trọng trong module này:
1. Client Reuse (Singleton) — khởi tạo 1 lần, không tạo connection mới
   mỗi lần gọi hàm → giảm overhead, tránh rò rỉ kết nối
2. Batch Load (KHÔNG Streaming Insert) — Streaming Insert của BigQuery
   tính phí (~$0.01/200MB), Batch Load hoàn toàn MIỄN PHÍ
3. Chunked Upload — khi upload lịch sử lớn (~50.000 dòng), chia nhỏ
   thành từng chunk để tránh timeout và dễ retry nếu 1 chunk lỗi
4. UPSERT qua DDL swap (KHÔNG dùng MERGE/DML) — BigQuery Sandbox (project
   chưa bật Billing Account) CẤM mọi lệnh DML (MERGE/UPDATE/DELETE).
   Thay vào đó, module này dùng CREATE TABLE ... AS SELECT (DDL, được
   phép trong Sandbox) để gộp dữ liệu cũ + mới, khử trùng theo timestamp,
   rồi ALTER TABLE RENAME để thay thế bảng chính. Kết quả tương đương
   UPSERT nhưng không cần bật Billing / nhập thẻ.
5. Partitioning + Clustering — table được partition theo NGÀY và
   cluster theo timestamp, giúp mọi query filter theo thời gian chỉ
   scan đúng phần dữ liệu cần, tiết kiệm quota 1TB/tháng miễn phí
6. Query luôn bắt buộc có LIMIT — tránh quét nhầm toàn bộ table
"""

from __future__ import annotations

import uuid
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

# Cột dùng làm KHÓA để so khớp khi MERGE — 1 timestamp = 1 nến duy nhất
_MERGE_KEY = "timestamp"


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
        self._dataset_id = f"{settings.gcp_project_id}.{settings.bq_dataset_id}"
        self._table_id = settings.full_table_id
        log.info(f"BigQueryStorage kết nối tới: {self._table_id}")

    # -- Khởi tạo hạ tầng (chạy 1 lần, idempotent) ------------------------

    def ensure_dataset_and_table(self) -> None:
        """
        Tạo dataset + table nếu chưa tồn tại. An toàn để gọi nhiều lần
        (idempotent) — không lỗi nếu đã tồn tại rồi.
        """
        dataset_ref = bigquery.Dataset(self._dataset_id)
        dataset_ref.location = settings.bq_location
        self._client.create_dataset(dataset_ref, exists_ok=True)
        log.info(f"Dataset '{settings.bq_dataset_id}' sẵn sàng (location={settings.bq_location})")

        table = bigquery.Table(self._table_id, schema=SCHEMA_OHLCV)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="timestamp",
        )
        table.clustering_fields = CLUSTERING_FIELDS

        self._client.create_table(table, exists_ok=True)
        log.info(f"Table '{self._table_id}' sẵn sàng (partitioned by DAY, clustered by timestamp)")

    # -- Ghi dữ liệu (Batch Load + UPSERT qua DDL swap - miễn phí, không cần Billing) --

    def get_latest_timestamp(self) -> datetime | None:
        """
        Lấy timestamp mới nhất hiện có trong BigQuery.
        Dùng để LOG/kiểm tra tình trạng dữ liệu, KHÔNG dùng để lọc trước
        khi upload nữa (cơ chế upload mới dùng DDL swap, tự khử trùng
        theo timestamp, nên không cần lọc thủ công ở bước này).
        Trả về None nếu table rỗng (lần chạy đầu tiên).
        """
        query = f"SELECT MAX(timestamp) AS max_ts FROM `{self._table_id}`"
        result = self._client.query(query).result()
        for row in result:
            return row.max_ts
        return None

    def upload(self, df: pd.DataFrame) -> int:
        """
        Upload DataFrame lên BigQuery bằng cơ chế UPSERT — nhưng dùng DDL
        (CREATE TABLE ... AS SELECT + RENAME) thay vì DML (MERGE).

        Vì sao KHÔNG dùng MERGE trực tiếp:
        BigQuery Sandbox (project CHƯA bật Billing Account) CẤM mọi câu
        lệnh DML (MERGE/UPDATE/DELETE/INSERT-qua-query) — báo lỗi 403
        "Billing has not been enabled". Vì nhóm không muốn nhập thẻ chỉ
        để dùng free tier, giải pháp là dùng DDL (CREATE TABLE AS SELECT,
        ALTER TABLE RENAME) — DDL vẫn được phép trong Sandbox, không cần
        Billing, và SELECT/Load Job cũng không cần Billing (chỉ DML bị chặn).

        Cách hoạt động:
        1. Load dữ liệu mới vào bảng TẠM (staging) — bằng Load Job (an toàn,
           không cần Billing)
        2. Tạo 1 bảng MỚI bằng CREATE TABLE ... AS SELECT: gộp bảng chính
           (dữ liệu cũ) + bảng staging (dữ liệu mới), khử trùng theo
           timestamp — dòng nào trùng thì giữ bản ghi có _loaded_at MỚI
           NHẤT (tức bản ghi mới luôn thắng, tự sửa lại nến chưa đóng)
        3. Xóa bảng chính cũ, ĐỔI TÊN bảng mới thành tên bảng chính
        4. Dọn dẹp bảng tạm

        Trả về tổng số dòng trong bảng sau khi upsert xong.
        """
        if df.empty:
            log.info("Upload: DataFrame rỗng, không có gì để tải lên")
            return 0

        df = df.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df["_loaded_at"] = datetime.now(timezone.utc)

        suffix = uuid.uuid4().hex[:8]
        staging_table_id = f"{self._dataset_id}.__staging_{suffix}"
        swap_table_id = f"{self._dataset_id}.__swap_{suffix}"
        swap_table_short_name = f"{settings.table_ohlcv}"  # Tên ngắn để RENAME TO

        try:
            self._load_to_staging(df, staging_table_id)
            self._create_merged_table_via_ctas(staging_table_id, swap_table_id)
            self._swap_tables(swap_table_id, swap_table_short_name)

            total_rows = self.count_rows()
            log.info(f"✅ Upsert hoàn tất (qua DDL swap): table hiện có {total_rows:,} dòng")
            return total_rows
        finally:
            self._client.delete_table(staging_table_id, not_found_ok=True)
            self._client.delete_table(swap_table_id, not_found_ok=True)

    def _load_to_staging(self, df: pd.DataFrame, staging_table_id: str) -> None:
        """Load toàn bộ DataFrame vào bảng tạm, chia chunk nếu dữ liệu lớn."""
        n_chunks = (len(df) + CHUNK_SIZE - 1) // CHUNK_SIZE
        for i in range(0, len(df), CHUNK_SIZE):
            chunk = df.iloc[i : i + CHUNK_SIZE]
            write_mode = (
                bigquery.WriteDisposition.WRITE_TRUNCATE
                if i == 0
                else bigquery.WriteDisposition.WRITE_APPEND
            )
            self._load_chunk(chunk, staging_table_id, write_mode)
            log.info(f"Upload: nạp staging chunk {i // CHUNK_SIZE + 1}/{n_chunks} ({len(chunk)} dòng)")

    @with_retry(max_attempts=3)
    def _load_chunk(self, chunk: pd.DataFrame, table_id: str, write_mode: str) -> None:
        """Load 1 chunk vào table chỉ định — có retry riêng cho từng chunk."""
        job_config = bigquery.LoadJobConfig(schema=SCHEMA_OHLCV, write_disposition=write_mode)
        job = self._client.load_table_from_dataframe(chunk, table_id, job_config=job_config)
        job.result()

    @with_retry(max_attempts=2)
    def _create_merged_table_via_ctas(self, staging_table_id: str, swap_table_id: str) -> None:
        """
        Tạo bảng mới = gộp (bảng chính + staging), khử trùng theo timestamp,
        giữ bản ghi có _loaded_at mới nhất. Đây là DDL (CREATE TABLE AS
        SELECT), KHÔNG phải DML, nên không bị Sandbox chặn.
        """
        ctas_sql = f"""
            CREATE OR REPLACE TABLE `{swap_table_id}`
            PARTITION BY DATE({_MERGE_KEY})
            CLUSTER BY {_MERGE_KEY}
            AS
            SELECT * EXCEPT(_rn) FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY {_MERGE_KEY}
                        ORDER BY _loaded_at DESC
                    ) AS _rn
                FROM (
                    SELECT * FROM `{self._table_id}`
                    UNION ALL
                    SELECT * FROM `{staging_table_id}`
                )
            )
            WHERE _rn = 1
        """
        job = self._client.query(ctas_sql)
        job.result()
        log.info("Upload: đã tạo bảng gộp (dedup theo timestamp) qua CREATE TABLE AS SELECT")

    @with_retry(max_attempts=2)
    def _swap_tables(self, swap_table_id: str, short_name: str) -> None:
        """
        Xóa bảng chính cũ, đổi tên bảng mới (đã gộp + dedup) thành bảng
        chính. Cả DROP và RENAME đều là DDL — không cần Billing.
        """
        self._client.delete_table(self._table_id, not_found_ok=True)
        rename_sql = f"ALTER TABLE `{swap_table_id}` RENAME TO `{short_name}`"
        job = self._client.query(rename_sql)
        job.result()
        log.info(f"Upload: đã đổi tên bảng gộp thành '{self._table_id}'")

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